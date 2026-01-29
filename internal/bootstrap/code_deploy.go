package bootstrap

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// DeploymentResult contains paths to the deployed code and virtual environment.
type DeploymentResult struct {
	RepoPath   string // Path to cloned repository
	CodePath   string // Path to task code (RepoPath + spec.CodePath, or RepoPath if no CodePath)
	VenvPath   string // Path to virtual environment
	VenvPython string // Path to python executable in venv
}

// DeployCode clones a git repository, creates a Python virtual environment,
// and installs dependencies.
func DeployCode(ctx context.Context, config Config, spec DeploymentConfig, logger Logger) (*DeploymentResult, error) {
	deployDir := config.DeploymentDir
	if deployDir == "" {
		deployDir = "./deployment"
	}

	// Convert to absolute path
	absDeployDir, err := filepath.Abs(deployDir)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve deployment directory: %w", err)
	}

	// Phase 1: Git Clone (with sparse checkout if CodePath is set)
	repoPath, err := gitClone(ctx, absDeployDir, spec, config, logger)
	if err != nil {
		return nil, fmt.Errorf("git clone failed: %w", err)
	}

	// Calculate code path (subdirectory where task code lives)
	codePath := repoPath
	if spec.CodePath != "" {
		codePath = filepath.Join(repoPath, spec.CodePath)
		// Verify the code path exists
		if _, err := os.Stat(codePath); os.IsNotExist(err) {
			return nil, fmt.Errorf("code_path directory does not exist: %s", codePath)
		}
		logger.Info("Using code path: %s", codePath)
	}

	// Phase 2: Detect package manager (in code path)
	pkgManager := detectPackageManager(codePath, logger)

	// Phase 3: Create virtual environment (in code path)
	venvPath := filepath.Join(codePath, ".venv")
	venvPython, err := createVirtualEnv(ctx, codePath, venvPath, logger)
	if err != nil {
		return nil, fmt.Errorf("virtualenv creation failed: %w", err)
	}

	// Phase 4: Install dependencies (in code path)
	if err := installDependencies(ctx, codePath, venvPython, pkgManager, logger); err != nil {
		return nil, fmt.Errorf("dependency installation failed: %w", err)
	}

	logger.Info("Code deployment completed: %s", codePath)
	return &DeploymentResult{
		RepoPath:   repoPath,
		CodePath:   codePath,
		VenvPath:   venvPath,
		VenvPython: venvPython,
	}, nil
}

// gitClone clones the repository or reuses existing deployment.
// If spec.CodePath is set, uses sparse checkout to only download that subdirectory.
func gitClone(ctx context.Context, deployDir string, spec DeploymentConfig, config Config, logger Logger) (string, error) {
	// Check if deployment directory exists
	info, err := os.Stat(deployDir)
	if err == nil && info.IsDir() {
		// Directory exists - check for .git
		gitDir := filepath.Join(deployDir, ".git")
		if _, err := os.Stat(gitDir); err == nil {
			logger.Info("Using existing deployment at %s", deployDir)
			return deployDir, nil
		}
		// Directory exists but no .git - error
		return "", fmt.Errorf("deployment directory exists but is not a git repository: %s (manual cleanup required)", deployDir)
	}

	// Create parent directory if needed
	parentDir := filepath.Dir(deployDir)
	if err := os.MkdirAll(parentDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create parent directory: %w", err)
	}

	// Handle authentication
	gitURL := spec.GitURL
	var env []string

	// Use git token from config (resolved from vault by server)
	gitToken := config.GitToken

	if config.GitSSHKey != "" {
		// SSH key authentication
		sshCmd := fmt.Sprintf("ssh -i %s -o StrictHostKeyChecking=no", config.GitSSHKey)
		env = append(os.Environ(), "GIT_SSH_COMMAND="+sshCmd)
		logger.Info("Using SSH key authentication: %s", config.GitSSHKey)
	} else if gitToken != "" {
		// Token authentication - embed in URL
		gitURL = EmbedTokenInURL(spec.GitURL, gitToken)
		env = os.Environ()
		logger.Info("Using token authentication")
	} else {
		// Public repository
		env = os.Environ()
		logger.Info("Using public repository access")
	}

	// Use sparse checkout if CodePath is specified
	if spec.CodePath != "" {
		return gitCloneSparse(ctx, deployDir, parentDir, spec, gitURL, env, logger)
	}

	// Regular clone (no CodePath)
	args := []string{"clone", "--branch", spec.Branch, "--depth", "1", gitURL, deployDir}

	logger.Info("Cloning %s (branch: %s) to %s", spec.GitURL, spec.Branch, deployDir)

	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Env = env
	cmd.Dir = parentDir

	output, err := cmd.CombinedOutput()
	if err != nil {
		// Clean up on failure
		os.RemoveAll(deployDir)
		return "", fmt.Errorf("git clone failed: %w\nOutput: %s", err, string(output))
	}

	logger.Info("Git clone successful")
	return deployDir, nil
}

// gitCloneSparse performs a sparse checkout to only download the specified subdirectory.
func gitCloneSparse(ctx context.Context, deployDir, parentDir string, spec DeploymentConfig, gitURL string, env []string, logger Logger) (string, error) {

	repoURL := gitURL

	if parentDir == "" {
		parentDir = "."
	}
	if strings.HasPrefix(parentDir, "http://") || strings.HasPrefix(parentDir, "https://") {
		return "", fmt.Errorf("invalid parentDir (looks like URL): %s", parentDir)
	}
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create parentDir %s: %w", parentDir, err)
	}

	if !filepath.IsAbs(deployDir) {
		deployDir = filepath.Join(parentDir, deployDir)
	}

	logger.Info("Sparse checkout: cloning %s (branch: %s, path: %s) to %s",
		repoURL, spec.Branch, spec.CodePath, deployDir)

	// Step 1: Clone with filter (no blobs) and sparse mode
	args := []string{
		"clone",
		"--filter=blob:none",
		"--sparse",
		"--branch", spec.Branch,
		"--depth", "1",
		repoURL,
		deployDir,
	}

	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Env = env
	cmd.Dir = parentDir

	output, err := cmd.CombinedOutput()
	if err != nil {
		os.RemoveAll(deployDir)
		return "", fmt.Errorf("git sparse clone failed: %w\nOutput: %s", err, string(output))
	}

	// Step 2: Some git versions need init before set
	initCmd := exec.CommandContext(ctx, "git", "sparse-checkout", "init", "--cone")
	initCmd.Env = env
	initCmd.Dir = deployDir
	_, _ = initCmd.CombinedOutput() // ignore error; newer git doesn't always need it

	// Step 3: Set sparse path
	setCmd := exec.CommandContext(ctx, "git", "sparse-checkout", "set", spec.CodePath)
	setCmd.Env = env
	setCmd.Dir = deployDir

	setOut, err := setCmd.CombinedOutput()
	if err != nil {
		os.RemoveAll(deployDir)
		return "", fmt.Errorf("git sparse-checkout set failed: %w\nOutput: %s", err, string(setOut))
	}

	logger.Info("Sparse checkout successful (only downloaded: %s)", spec.CodePath)
	return deployDir, nil
}

// EmbedTokenInURL embeds a token into a git URL for authentication.
func EmbedTokenInURL(gitURL, token string) string {
	// Handle HTTPS URLs: https://github.com/user/repo.git -> https://token@github.com/user/repo.git
	if strings.HasPrefix(gitURL, "https://") {
		return "https://" + token + "@" + strings.TrimPrefix(gitURL, "https://")
	}
	// Handle HTTP URLs (not recommended but supported)
	if strings.HasPrefix(gitURL, "http://") {
		return "http://" + token + "@" + strings.TrimPrefix(gitURL, "http://")
	}
	// Return unchanged for SSH URLs (token auth doesn't apply)
	return gitURL
}

// packageManager represents the detected Python package manager.
type packageManager int

const (
	pkgPip packageManager = iota
	pkgPoetry
)

// detectPackageManager checks for pyproject.toml with poetry or requirements.txt.
func detectPackageManager(repoPath string, logger Logger) packageManager {
	pyprojectPath := filepath.Join(repoPath, "pyproject.toml")
	if data, err := os.ReadFile(pyprojectPath); err == nil {
		// Check if it's a Poetry project
		if strings.Contains(string(data), "[tool.poetry]") {
			logger.Info("Detected Poetry project (pyproject.toml with [tool.poetry])")
			return pkgPoetry
		}
		logger.Info("Detected pyproject.toml without Poetry, using pip")
	}

	requirementsPath := filepath.Join(repoPath, "requirements.txt")
	if _, err := os.Stat(requirementsPath); err == nil {
		logger.Info("Detected requirements.txt, using pip")
	} else {
		logger.Warn("No requirements.txt or pyproject.toml found")
	}

	return pkgPip
}

// createVirtualEnv creates a Python virtual environment if it doesn't exist.
func createVirtualEnv(ctx context.Context, repoPath, venvPath string, logger Logger) (string, error) {
	// Determine python executable path based on OS
	var venvPython string
	if runtime.GOOS == "windows" {
		venvPython = filepath.Join(venvPath, "Scripts", "python.exe")
	} else {
		venvPython = filepath.Join(venvPath, "bin", "python")
	}

	// Check if venv already exists
	if _, err := os.Stat(venvPython); err == nil {
		logger.Info("Reusing existing virtualenv at %s", venvPath)
		return venvPython, nil
	}

	// Find python executable
	pythonCmd := FindPython()
	if pythonCmd == "" {
		return "", fmt.Errorf("python not found in PATH")
	}

	logger.Info("Creating virtualenv at %s using %s", venvPath, pythonCmd)

	cmd := exec.CommandContext(ctx, pythonCmd, "-m", "venv", venvPath)
	cmd.Dir = repoPath

	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to create virtualenv: %w\nOutput: %s", err, string(output))
	}

	// Verify the python executable exists
	if _, err := os.Stat(venvPython); err != nil {
		return "", fmt.Errorf("virtualenv created but python not found at %s", venvPython)
	}

	logger.Info("Virtualenv created successfully")
	return venvPython, nil
}

// FindPython finds a Python 3 executable in PATH.
func FindPython() string {
	// Try python3 first (common on Unix), then python (common on Windows)
	candidates := []string{"python3", "python"}

	for _, candidate := range candidates {
		path, err := exec.LookPath(candidate)
		if err != nil {
			continue
		}

		// Verify it's Python 3
		cmd := exec.Command(path, "--version")
		output, err := cmd.Output()
		if err != nil {
			continue
		}

		version := string(output)
		if strings.Contains(version, "Python 3") {
			return path
		}
	}

	return ""
}

// installDependencies installs Python dependencies using pip or poetry.
func installDependencies(ctx context.Context, repoPath, venvPython string, pkgMgr packageManager, logger Logger) error {
	switch pkgMgr {
	case pkgPoetry:
		return installWithPoetry(ctx, repoPath, logger)
	default:
		return installWithPip(ctx, repoPath, venvPython, logger)
	}
}

// installWithPip installs dependencies using pip.
func installWithPip(ctx context.Context, repoPath, venvPython string, logger Logger) error {
	requirementsPath := filepath.Join(repoPath, "requirements.txt")

	// Check if requirements.txt exists
	if _, err := os.Stat(requirementsPath); os.IsNotExist(err) {
		logger.Warn("No requirements.txt found, skipping pip install")
		return nil
	}

	logger.Info("Installing dependencies with pip...")

	// Upgrade pip first
	upgradeCmd := exec.CommandContext(ctx, venvPython, "-m", "pip", "install", "--upgrade", "pip")
	upgradeCmd.Dir = repoPath
	if output, err := upgradeCmd.CombinedOutput(); err != nil {
		logger.Warn("Failed to upgrade pip: %v\nOutput: %s", err, string(output))
		// Continue anyway - old pip might still work
	}

	// Install requirements
	cmd := exec.CommandContext(ctx, venvPython, "-m", "pip", "install", "-r", requirementsPath)
	cmd.Dir = repoPath

	// Stream output for long-running installs
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start pip install: %w", err)
	}

	// Log output line by line
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		logger.Debug("[pip] %s", scanner.Text())
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("pip install failed: %w", err)
	}

	logger.Info("Dependencies installed successfully with pip")
	return nil
}

// installWithPoetry installs dependencies using poetry.
func installWithPoetry(ctx context.Context, repoPath string, logger Logger) error {
	// Check if poetry is available
	poetryPath, err := exec.LookPath("poetry")
	if err != nil {
		return fmt.Errorf("poetry not found in PATH (required for this project)")
	}

	logger.Info("Installing dependencies with poetry...")

	// Configure poetry to use in-project virtualenv
	configCmd := exec.CommandContext(ctx, poetryPath, "config", "virtualenvs.in-project", "true")
	configCmd.Dir = repoPath
	if output, err := configCmd.CombinedOutput(); err != nil {
		logger.Warn("Failed to configure poetry: %v\nOutput: %s", err, string(output))
	}

	// Install dependencies
	cmd := exec.CommandContext(ctx, poetryPath, "install", "--no-interaction")
	cmd.Dir = repoPath

	// Stream output
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start poetry install: %w", err)
	}

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		logger.Debug("[poetry] %s", scanner.Text())
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("poetry install failed: %w", err)
	}

	logger.Info("Dependencies installed successfully with poetry")
	return nil
}
