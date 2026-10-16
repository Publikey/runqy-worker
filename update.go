package worker

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"
)

// defaultReleaseBaseURL is where worker_update downloads releases from;
// RUNQY_UPDATE_BASE_URL overrides it (mirror). Files follow the goreleaser layout:
// <base>/v<version>/runqy-worker_<version>_<os>_<arch>.tar.gz and checksums.txt.
const defaultReleaseBaseURL = "https://github.com/Publikey/runqy-worker/releases/download"

// maxReleaseArchive bounds the download (the archive is ~10 MB).
const maxReleaseArchive = 200 << 20

var releaseVersionRe = regexp.MustCompile(`^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$`)

// workerUpdate downloads and verifies the requested release, waits for in-flight tasks,
// then shuts the worker down and re-executes the new binary in place (same container,
// so model caches and the deployment survive). Run() performs the re-exec.
func (c *controller) workerUpdate(msg ControlMessage) (string, error) {
	if runtime.GOOS == "windows" {
		return "", fmt.Errorf("worker_update is not supported on Windows")
	}
	version := strings.TrimPrefix(strings.TrimSpace(msg.Version), "v")
	if !releaseVersionRe.MatchString(version) {
		return "", fmt.Errorf("invalid version %q (expected e.g. 0.3.2)", msg.Version)
	}
	if version == strings.TrimPrefix(c.w.config.Version, "v") {
		return "already running " + version, nil
	}
	timeout, err := parseTimeout(msg.Timeout, defaultUpdateDrainTimeout)
	if err != nil {
		return "", err
	}

	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("cannot locate the running binary: %w", err)
	}
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved
	}

	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Minute)
	defer cancel()
	staged := exe + ".new"
	if err := downloadRelease(ctx, releaseBaseURL(), version, staged); err != nil {
		os.Remove(staged)
		return "", err
	}
	if err := checkBinaryVersion(ctx, staged, version); err != nil {
		os.Remove(staged)
		return "", err
	}

	// Let in-flight tasks finish, then swap the binary and restart into it.
	if _, err := c.holdIdle(c.parents(), timeout); err != nil {
		os.Remove(staged)
		return "", err
	}
	if err := os.Rename(staged, exe); err != nil {
		os.Remove(staged)
		c.w.processor.release(c.parents()...)
		return "", fmt.Errorf("cannot replace %s: %w", exe, err)
	}
	c.logger.Info("Worker binary replaced by %s, restarting...", version)
	c.w.requestReexec(exe)
	return "restarting into " + version, nil
}

func releaseBaseURL() string {
	if v := strings.TrimSpace(os.Getenv("RUNQY_UPDATE_BASE_URL")); v != "" {
		return strings.TrimSuffix(v, "/")
	}
	return defaultReleaseBaseURL
}

// downloadRelease fetches the release archive for this OS/arch, checks it against the
// release's checksums.txt and extracts the runqy-worker binary to dest (mode 0755).
func downloadRelease(ctx context.Context, base, version, dest string) error {
	archive := fmt.Sprintf("runqy-worker_%s_%s_%s.tar.gz", version, runtime.GOOS, runtime.GOARCH)
	dir := base + "/v" + version
	client := &http.Client{Timeout: 5 * time.Minute}

	sums, err := httpGet(ctx, client, dir+"/checksums.txt", 1<<20)
	if err != nil {
		return fmt.Errorf("cannot download checksums: %w", err)
	}
	want := ""
	sc := bufio.NewScanner(strings.NewReader(string(sums)))
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 2 && fields[1] == archive {
			want = strings.ToLower(fields[0])
		}
	}
	if want == "" {
		return fmt.Errorf("release %s has no %s", version, archive)
	}

	data, err := httpGet(ctx, client, dir+"/"+archive, maxReleaseArchive)
	if err != nil {
		return fmt.Errorf("cannot download %s: %w", archive, err)
	}
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != want {
		return fmt.Errorf("checksum mismatch for %s: got %s, want %s", archive, got, want)
	}
	return extractBinary(data, dest)
}

func httpGet(ctx context.Context, client *http.Client, url string, limit int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: HTTP %d", url, resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("GET %s: response larger than %d bytes", url, limit)
	}
	return data, nil
}

// extractBinary writes the "runqy-worker" entry of a .tar.gz archive to dest.
func extractBinary(archive []byte, dest string) error {
	gz, err := gzip.NewReader(strings.NewReader(string(archive)))
	if err != nil {
		return fmt.Errorf("invalid archive: %w", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return fmt.Errorf("archive has no runqy-worker binary")
		}
		if err != nil {
			return fmt.Errorf("invalid archive: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg || filepath.Base(hdr.Name) != "runqy-worker" {
			continue
		}
		f, err := os.OpenFile(dest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
		if err != nil {
			return err
		}
		if _, err := io.Copy(f, io.LimitReader(tr, maxReleaseArchive)); err != nil {
			f.Close()
			return err
		}
		return f.Close()
	}
}

// checkBinaryVersion runs "<bin> -version" and checks it reports the expected version,
// so a broken or wrong binary is never swapped in.
func checkBinaryVersion(ctx context.Context, bin, version string) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, "-version").CombinedOutput()
	if err != nil {
		return fmt.Errorf("downloaded binary does not run: %w (%s)", err, strings.TrimSpace(string(out)))
	}
	if !strings.Contains(string(out), version) {
		return fmt.Errorf("downloaded binary reports %q, want %s", strings.TrimSpace(string(out)), version)
	}
	return nil
}
