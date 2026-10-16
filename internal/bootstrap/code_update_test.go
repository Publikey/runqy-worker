package bootstrap

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func git(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func commitFile(t *testing.T, dir, name, content, msg string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, dir, "add", name)
	git(t, dir, "commit", "-q", "-m", msg)
}

func TestGitRefreshFollowsBranch(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	root := t.TempDir()
	origin := filepath.Join(root, "origin")
	os.MkdirAll(origin, 0o755)
	git(t, origin, "init", "-q", "-b", "main")
	commitFile(t, origin, "main.py", "v1", "v1")

	spec := DeploymentConfig{GitURL: "file://" + filepath.ToSlash(origin), Branch: "main"}
	deployDir := filepath.Join(root, "deploy")
	if _, err := gitClone(context.Background(), deployDir, spec, Config{}, &testLogger{}, false); err != nil {
		t.Fatal(err)
	}
	// Untracked files (the virtualenv) must survive an update.
	os.WriteFile(filepath.Join(deployDir, "local.txt"), []byte("keep"), 0o644)

	// New commit on main: the update picks it up.
	commitFile(t, origin, "main.py", "v2", "v2")
	if err := gitRefresh(context.Background(), deployDir, spec, Config{}, &testLogger{}); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(deployDir, "main.py")); string(got) != "v2" {
		t.Fatalf("main.py = %q after update, want v2", got)
	}

	// The queue config now points at another branch: the update switches to it.
	git(t, origin, "checkout", "-q", "-b", "next")
	commitFile(t, origin, "main.py", "next", "next")
	spec.Branch = "next"
	if err := gitRefresh(context.Background(), deployDir, spec, Config{}, &testLogger{}); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(deployDir, "main.py")); string(got) != "next" {
		t.Fatalf("main.py = %q after branch change, want next", got)
	}
	if _, err := os.Stat(filepath.Join(deployDir, "local.txt")); err != nil {
		t.Error("untracked file removed by the update")
	}

	// A branch that does not exist is an error (not a silent warning).
	spec.Branch = "missing"
	if err := gitRefresh(context.Background(), deployDir, spec, Config{}, &testLogger{}); err == nil {
		t.Error("update to a missing branch should fail")
	}
}
