package bootstrap

import (
	"testing"
)

func TestParseCommand(t *testing.T) {
	tests := []struct {
		name     string
		cmd      string
		expected []string
	}{
		{
			name:     "simple command",
			cmd:      "python app.py",
			expected: []string{"python", "app.py"},
		},
		{
			name:     "uvicorn command",
			cmd:      "uvicorn app.main:app --host 0.0.0.0 --port 8080",
			expected: []string{"uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"},
		},
		{
			name:     "double quoted argument",
			cmd:      `python -c "print('hello world')"`,
			expected: []string{"python", "-c", "print('hello world')"},
		},
		{
			name:     "single quoted argument",
			cmd:      `python -c 'print("hello world")'`,
			expected: []string{"python", "-c", `print("hello world")`},
		},
		{
			name:     "multiple spaces",
			cmd:      "python   app.py   --verbose",
			expected: []string{"python", "app.py", "--verbose"},
		},
		{
			name:     "empty string",
			cmd:      "",
			expected: []string{},
		},
		{
			name:     "single word",
			cmd:      "python",
			expected: []string{"python"},
		},
		{
			name:     "path with spaces in quotes",
			cmd:      `python "/path/with spaces/script.py"`,
			expected: []string{"python", "/path/with spaces/script.py"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseCommand(tt.cmd)

			if len(result) != len(tt.expected) {
				t.Errorf("ParseCommand(%q) returned %d args, want %d", tt.cmd, len(result), len(tt.expected))
				t.Errorf("  got:  %v", result)
				t.Errorf("  want: %v", tt.expected)
				return
			}

			for i, arg := range result {
				if arg != tt.expected[i] {
					t.Errorf("ParseCommand(%q)[%d] = %q, want %q", tt.cmd, i, arg, tt.expected[i])
				}
			}
		})
	}
}

func TestProcessSupervisorConstruction(t *testing.T) {
	deployment := &DeploymentResult{
		RepoPath:   "/path/to/repo",
		VenvPath:   "/path/to/repo/.venv",
		VenvPython: "/path/to/repo/.venv/bin/python",
	}

	spec := DeploymentConfig{
		GitURL:             "https://github.com/test/repo.git",
		Branch:             "main",
		StartupCmd:         "uvicorn app:app --port 8080",
		StartupTimeoutSecs: 120,
	}

	logger := newTestLogger()
	vaults := map[string]string{"KEY": "value"}
	supervisor := NewProcessSupervisor(deployment, spec, vaults, logger)

	if supervisor.deployment != deployment {
		t.Error("deployment not set correctly")
	}
	if supervisor.startupCmd != "uvicorn app:app --port 8080" {
		t.Errorf("startupCmd = %q, want %q", supervisor.startupCmd, "uvicorn app:app --port 8080")
	}
	if supervisor.timeoutSec != 120 {
		t.Errorf("timeoutSec = %d, want 120", supervisor.timeoutSec)
	}
	if supervisor.vaultVars["KEY"] != "value" {
		t.Errorf("vaultVars[KEY] = %q, want %q", supervisor.vaultVars["KEY"], "value")
	}
	if supervisor.healthy {
		t.Error("supervisor should not be healthy initially")
	}
	if supervisor.crashed {
		t.Error("supervisor should not be crashed initially")
	}
}

func TestProcessSupervisorDefaultTimeout(t *testing.T) {
	deployment := &DeploymentResult{}
	spec := DeploymentConfig{
		StartupTimeoutSecs: 0, // Should default to 60
	}

	logger := newTestLogger()
	supervisor := NewProcessSupervisor(deployment, spec, nil, logger)

	if supervisor.timeoutSec != 60 {
		t.Errorf("expected default timeout 60, got %d", supervisor.timeoutSec)
	}
}

func TestProcessSupervisorHealthMethods(t *testing.T) {
	supervisor := &ProcessSupervisor{
		done: make(chan struct{}),
	}

	// Initially not healthy
	if supervisor.IsHealthy() {
		t.Error("should not be healthy initially")
	}

	// Set healthy
	supervisor.healthy = true
	if !supervisor.IsHealthy() {
		t.Error("should be healthy after setting")
	}

	// Crash should make it unhealthy
	supervisor.crashed = true
	if supervisor.IsHealthy() {
		t.Error("should not be healthy when crashed")
	}

	// Check HasCrashed
	if !supervisor.HasCrashed() {
		t.Error("HasCrashed should return true")
	}
}
