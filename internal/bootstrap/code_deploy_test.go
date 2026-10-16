package bootstrap

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEmbedTokenInURL(t *testing.T) {
	tests := []struct {
		name     string
		gitURL   string
		token    string
		expected string
	}{
		{
			name:     "https url",
			gitURL:   "https://github.com/user/repo.git",
			token:    "ghp_token123",
			expected: "https://x-access-token:ghp_token123@github.com/user/repo.git",
		},
		{
			name:     "http url",
			gitURL:   "http://github.com/user/repo.git",
			token:    "token123",
			expected: "http://x-access-token:token123@github.com/user/repo.git",
		},
		{
			name:     "ssh url unchanged",
			gitURL:   "git@github.com:user/repo.git",
			token:    "token123",
			expected: "git@github.com:user/repo.git",
		},
		{
			name:     "existing credentials replaced",
			gitURL:   "https://oldtoken@github.com/user/repo.git",
			token:    "newtoken",
			expected: "https://x-access-token:newtoken@github.com/user/repo.git",
		},
		{
			name:     "user:password token",
			gitURL:   "https://gitlab.com/user/repo.git",
			token:    "oauth2:glpat-abc",
			expected: "https://oauth2:glpat-abc@gitlab.com/user/repo.git",
		},
		{
			name:     "surrounding whitespace trimmed",
			gitURL:   "https://github.com/user/repo.git",
			token:    " ghp_token123\n",
			expected: "https://x-access-token:ghp_token123@github.com/user/repo.git",
		},
		{
			name:     "empty token unchanged",
			gitURL:   "https://github.com/user/repo.git",
			token:    "",
			expected: "https://github.com/user/repo.git",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EmbedTokenInURL(tt.gitURL, tt.token)
			if result != tt.expected {
				t.Errorf("EmbedTokenInURL(%q, %q) = %q, want %q", tt.gitURL, tt.token, result, tt.expected)
			}
		})
	}
}

func TestRedactToken(t *testing.T) {
	out := "fatal: unable to access 'https://x-access-token:ghp_secret@github.com/user/repo.git/'"
	got := redactToken(out, "ghp_secret")
	if strings.Contains(got, "ghp_secret") {
		t.Errorf("token not redacted: %q", got)
	}
	if redactToken(out, "") != out {
		t.Error("empty token must leave output unchanged")
	}
}

func TestDetectPackageManager(t *testing.T) {
	// Create temp directory for tests
	tempDir, err := os.MkdirTemp("", "deploy-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	logger := newTestLogger()

	t.Run("pip with requirements.txt", func(t *testing.T) {
		dir := filepath.Join(tempDir, "pip-project")
		os.MkdirAll(dir, 0755)
		os.WriteFile(filepath.Join(dir, "requirements.txt"), []byte("flask==2.0.0\n"), 0644)

		result := detectPackageManager(dir, logger)
		if result != pkgPip {
			t.Errorf("expected pkgPip, got %v", result)
		}
	})

	t.Run("poetry with pyproject.toml", func(t *testing.T) {
		dir := filepath.Join(tempDir, "poetry-project")
		os.MkdirAll(dir, 0755)
		content := `[tool.poetry]
name = "myproject"
version = "0.1.0"

[tool.poetry.dependencies]
python = "^3.9"
`
		os.WriteFile(filepath.Join(dir, "pyproject.toml"), []byte(content), 0644)

		result := detectPackageManager(dir, logger)
		if result != pkgPoetry {
			t.Errorf("expected pkgPoetry, got %v", result)
		}
	})

	t.Run("pyproject.toml without poetry", func(t *testing.T) {
		dir := filepath.Join(tempDir, "pep517-project")
		os.MkdirAll(dir, 0755)
		content := `[build-system]
requires = ["setuptools"]
build-backend = "setuptools.build_meta"
`
		os.WriteFile(filepath.Join(dir, "pyproject.toml"), []byte(content), 0644)

		result := detectPackageManager(dir, logger)
		if result != pkgPip {
			t.Errorf("expected pkgPip for non-poetry pyproject.toml, got %v", result)
		}
	})

	t.Run("no dependency files", func(t *testing.T) {
		dir := filepath.Join(tempDir, "empty-project")
		os.MkdirAll(dir, 0755)

		result := detectPackageManager(dir, logger)
		if result != pkgPip {
			t.Errorf("expected pkgPip as default, got %v", result)
		}
	})
}

func TestFindPython(t *testing.T) {
	// This test depends on Python being installed on the system
	// Skip if no Python is available
	pythonPath := FindPython()
	if pythonPath == "" {
		t.Skip("Python 3 not found in PATH, skipping test")
	}

	t.Logf("Found Python at: %s", pythonPath)
}

func TestDeploymentResult(t *testing.T) {
	// Test that DeploymentResult struct is properly initialized
	result := &DeploymentResult{
		RepoPath:   "/path/to/repo",
		VenvPath:   "/path/to/repo/.venv",
		VenvPython: "/path/to/repo/.venv/bin/python",
	}

	if result.RepoPath != "/path/to/repo" {
		t.Errorf("unexpected RepoPath: %s", result.RepoPath)
	}
	if result.VenvPath != "/path/to/repo/.venv" {
		t.Errorf("unexpected VenvPath: %s", result.VenvPath)
	}
	if result.VenvPython != "/path/to/repo/.venv/bin/python" {
		t.Errorf("unexpected VenvPython: %s", result.VenvPython)
	}
}
