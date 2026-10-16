package bootstrap

import (
	"bufio"
	"context"
	"fmt"
	"net/url"
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
	venvPython, err := createVirtualEnv(ctx, codePath, venvPath, config.UseSystemSitePackages, logger)
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


// gitEnv builds the environment variables for git operations (SSH key or token auth).
func gitEnv(config Config, spec DeploymentConfig) []string {
	if config.GitSSHKey != "" {
		sshCmd := fmt.Sprintf("ssh -i %s -o StrictHostKeyChecking=no", config.GitSSHKey)
		return append(os.Environ(), "GIT_SSH_COMMAND="+sshCmd)
	}
	return os.Environ()
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
			logger.Info("Existing deployment found at %s, pulling latest changes...", deployDir)

			// Point origin at the current token so a rotated token (or the older
			// token-only URL form) does not break the pull.
			if config.GitSSHKey == "" && config.GitToken != "" {
				setURL := exec.CommandContext(ctx, "git", "remote", "set-url", "origin", EmbedTokenInURL(spec.GitURL, config.GitToken))
				setURL.Dir = deployDir
				if out, err := setURL.CombinedOutput(); err != nil {
					logger.Warn("Failed to update origin URL: %v\nOutput: %s", err, redactToken(string(out), config.GitToken))
				}
			}

			// Pull latest changes to ensure worker runs up-to-date code
			pullArgs := []string{"pull", "--ff-only"}
			pullCmd := exec.CommandContext(ctx, "git", pullArgs...)
			pullCmd.Dir = deployDir
			pullCmd.Env = gitEnv(config, spec)

			if pullOutput, pullErr := pullCmd.CombinedOutput(); pullErr != nil {
				logger.Warn("Git pull failed (continuing with existing code): %v\nOutput: %s", pullErr, redactToken(string(pullOutput), config.GitToken))
			} else {
				logger.Info("Git pull successful")
			}

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
		return gitCloneSparse(ctx, deployDir, parentDir, spec, gitURL, gitToken, env, logger)
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
		return "", fmt.Errorf("git clone failed: %w\nOutput: %s", err, redactToken(string(output), gitToken))
	}

	logger.Info("Git clone successful")
	return deployDir, nil
}

// gitCloneSparse performs a sparse checkout to only download the specified subdirectory.
func gitCloneSparse(ctx context.Context, deployDir, parentDir string, spec DeploymentConfig, gitURL, gitToken string, env []string, logger Logger) (string, error) {

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

	// Log the URL as configured: repoURL may carry the token.
	logger.Info("Sparse checkout: cloning %s (branch: %s, path: %s) to %s",
		spec.GitURL, spec.Branch, spec.CodePath, deployDir)

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
		return "", fmt.Errorf("git sparse clone failed: %w\nOutput: %s", err, redactToken(string(output), gitToken))
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
		return "", fmt.Errorf("git sparse-checkout set failed: %w\nOutput: %s", err, redactToken(string(setOut), gitToken))
	}

	logger.Info("Sparse checkout successful (only downloaded: %s)", spec.CodePath)
	return deployDir, nil
}

// gitTokenUser is the username sent with a bare token (GitHub ignores it for token auth).
// Other hosts that need a specific username take the token as user:password.
const gitTokenUser = "x-access-token"

// EmbedTokenInURL embeds a token into an HTTP(S) git URL for authentication, as
// https://x-access-token:<token>@host/... A token containing ':' is taken as user:password.
// Any credentials already in the URL are replaced; SSH and other URLs are returned unchanged.
//
// The token goes in the password slot: a token-only username (https://<token>@host) breaks
// with Ubuntu's curl 8.5.0-2ubuntu10.16, which loses it when GitHub redirects the request.
func EmbedTokenInURL(gitURL, token string) string {
	token = strings.TrimSpace(token)
	if token == "" || !(strings.HasPrefix(gitURL, "https://") || strings.HasPrefix(gitURL, "http://")) {
		return gitURL
	}
	u, err := url.Parse(gitURL)
	if err != nil {
		return gitURL
	}
	if user, pass, ok := strings.Cut(token, ":"); ok {
		u.User = url.UserPassword(user, pass)
	} else {
		u.User = url.UserPassword(gitTokenUser, token)
	}
	return u.String()
}

// redactToken hides a git token (raw and URL-escaped) in git output before it is logged.
func redactToken(s, token string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return s
	}
	s = strings.ReplaceAll(s, token, "***")
	if esc := url.QueryEscape(token); esc != token {
		s = strings.ReplaceAll(s, esc, "***")
	}
	return s
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
func createVirtualEnv(ctx context.Context, repoPath, venvPath string, useSystemSitePackages bool, logger Logger) (string, error) {
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

	// Try uv first (faster, doesn't require ensurepip)
	if uvCmd := findUV(); uvCmd != "" {
		var args []string
		if useSystemSitePackages {
			args = []string{"venv", "--seed", "--system-site-packages", venvPath}
			logger.Info("Creating virtualenv at %s using uv (with system-site-packages)", venvPath)
		} else {
			args = []string{"venv", "--seed", venvPath}
			logger.Info("Creating virtualenv at %s using uv (isolated)", venvPath)
		}
		cmd := exec.CommandContext(ctx, uvCmd, args...)
		cmd.Dir = repoPath
		if output, err := cmd.CombinedOutput(); err == nil {
			// Verify the python executable exists
			if _, err := os.Stat(venvPython); err == nil {
				logger.Info("Virtualenv created successfully with uv")
				return venvPython, nil
			}
			logger.Warn("uv venv succeeded but python not found at %s", venvPython)
		} else {
			logger.Warn("uv venv failed, falling back to python -m venv: %s", string(output))
		}
	}

	// Fallback to python -m venv
	pythonCmd := FindPython()
	if pythonCmd == "" {
		return "", fmt.Errorf("python not found in PATH")
	}

	var cmd *exec.Cmd
	if useSystemSitePackages {
		logger.Info("Creating virtualenv at %s using %s (with system-site-packages)", venvPath, pythonCmd)
		cmd = exec.CommandContext(ctx, pythonCmd, "-m", "venv", "--system-site-packages", venvPath)
	} else {
		logger.Info("Creating virtualenv at %s using %s (isolated)", venvPath, pythonCmd)
		cmd = exec.CommandContext(ctx, pythonCmd, "-m", "venv", venvPath)
	}
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

// findUV finds the uv executable in PATH.
func findUV() string {
	if path, err := exec.LookPath("uv"); err == nil {
		return path
	}
	return ""
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
// Note: We use pip instead of uv for installation because uv doesn't properly
// respect system-site-packages when checking for already-installed packages.
// This is important for Docker images with pre-installed packages (e.g., PyTorch).
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
