// Uploader: moves finished bronze data from the VM to Swift.
//
// For every day folder older than today (UTC), e.g. /data/vehicle_positions/date=2026-10-21/:
//  1. gzip every file that is not compressed yet (lossless, checked before the original is removed)
//  2. upload it to Swift with the same path as its name, e.g.
//     vehicle_positions/date=2026-10-21/vehicle_positions_20261021T120000Z.json.gz
//     (files already in Swift with the same MD5 are skipped, so re-running is safe)
//  3. delete the local file, but only after Swift confirmed the upload (MD5 check)
//
// Today's folders are never touched, because the collectors are still writing to them.
//
// Settings (environment variables):
//
//	DATA_DIR         /data          folder with <feed>/date=YYYY-MM-DD/ subfolders
//	LOG_DIR          /logs          uploader.log is written here
//	SWIFT_CONTAINER  buspulse-raw   target container (created if missing)
//	DELETE_LOCAL     true           false = upload but keep local files
//	DRY_RUN          false          true  = only log what would be done
//	WORKERS          4              parallel uploads
//	RETRIES          3              attempts per file
//	OS_*                            OpenStack login, same as openstack.env
package main

import (
	"compress/gzip"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ncw/swift/v2"
)

type config struct {
	dataDir     string
	logDir      string
	container   string
	deleteLocal bool
	dryRun      bool
	workers     int
	retries     int
}

type stats struct {
	mu                                     sync.Mutex
	compressed, uploaded, skipped, deleted int
	failed                                 int
}

func (s *stats) add(field *int) { s.mu.Lock(); *field++; s.mu.Unlock() }

var dayFolder = regexp.MustCompile(`^date=(\d{4}-\d{2}-\d{2})$`)

func main() {
	cfg := loadConfig()
	logger := newLogger(cfg.logDir)

	logger.Printf("INFO  | uploader | started (container=%s delete_local=%t dry_run=%t)",
		cfg.container, cfg.deleteLocal, cfg.dryRun)

	conn := &swift.Connection{Timeout: 5 * time.Minute}
	st, err := run(context.Background(), cfg, conn, logger, time.Now().UTC())
	if err != nil {
		logger.Printf("ERROR | uploader | %v", err)
		os.Exit(1)
	}

	logger.Printf("INFO  | uploader | finished: %d compressed, %d uploaded, %d already in swift, %d deleted locally, %d failed",
		st.compressed, st.uploaded, st.skipped, st.deleted, st.failed)
	if st.failed > 0 {
		os.Exit(1) // lets cron / Kubernetes see that something went wrong
	}
}

// run does the whole job and returns the counts. Split out from main so it can be tested.
func run(ctx context.Context, cfg config, conn *swift.Connection, logger *log.Logger, now time.Time) (*stats, error) {
	if err := conn.ApplyEnvironment(); err != nil {
		return nil, fmt.Errorf("reading OS_* settings: %w", err)
	}
	if err := conn.Authenticate(ctx); err != nil {
		return nil, fmt.Errorf("logging in to OpenStack: %w", err)
	}
	if !cfg.dryRun {
		if err := conn.ContainerCreate(ctx, cfg.container, nil); err != nil {
			return nil, fmt.Errorf("creating container %s: %w", cfg.container, err)
		}
	}

	today := now.Format("2006-01-02")
	days, err := findOldDays(cfg.dataDir, today)
	if err != nil {
		return nil, err
	}

	st := &stats{}
	for _, day := range days {
		processDay(ctx, cfg, conn, logger, day, st)
	}
	return st, nil
}

// findOldDays returns all <feed>/date=YYYY-MM-DD folders whose date is before today.
func findOldDays(dataDir, today string) ([]string, error) {
	feeds, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", dataDir, err)
	}
	var days []string
	for _, feed := range feeds {
		if !feed.IsDir() {
			continue
		}
		entries, err := os.ReadDir(filepath.Join(dataDir, feed.Name()))
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			m := dayFolder.FindStringSubmatch(e.Name())
			if e.IsDir() && m != nil && m[1] < today {
				days = append(days, filepath.Join(dataDir, feed.Name(), e.Name()))
			}
		}
	}
	sort.Strings(days)
	return days, nil
}

// processDay compresses, uploads and (optionally) deletes all files in one day folder.
func processDay(ctx context.Context, cfg config, conn *swift.Connection, logger *log.Logger, dayDir string, st *stats) {
	prefix, _ := filepath.Rel(cfg.dataDir, dayDir) // e.g. vehicle_positions/date=2026-10-21
	prefix = filepath.ToSlash(prefix) + "/"

	// What is already in Swift for this day: object name -> MD5
	existing := map[string]string{}
	objects, err := conn.ObjectsAll(ctx, cfg.container, &swift.ObjectsOpts{Prefix: prefix})
	if err != nil {
		logger.Printf("ERROR | uploader | listing %s in swift: %v", prefix, err)
		st.add(&st.failed)
		return
	}
	for _, o := range objects {
		existing[o.Name] = o.Hash
	}

	files, err := listFiles(dayDir)
	if err != nil {
		logger.Printf("ERROR | uploader | reading %s: %v", dayDir, err)
		st.add(&st.failed)
		return
	}

	if cfg.dryRun {
		logger.Printf("INFO  | uploader | dry run: %s has %d local files, %d already in swift",
			prefix, len(files), len(existing))
		return
	}

	jobs := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < cfg.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range jobs {
				if err := processFile(ctx, cfg, conn, path, existing, st); err != nil {
					logger.Printf("ERROR | uploader | %s: %v", path, err)
					st.add(&st.failed)
				}
			}
		}()
	}
	for _, f := range files {
		jobs <- f
	}
	close(jobs)
	wg.Wait()

	// Remove the day folder once it is empty
	if cfg.deleteLocal && !cfg.dryRun {
		if left, _ := os.ReadDir(dayDir); len(left) == 0 {
			os.Remove(dayDir)
		}
	}
}

func listFiles(dir string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".tmp") { // half-written file from an interrupted run; redone below
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		return nil, err
	}

	// If a run was interrupted right after compressing, both X and X.gz can exist.
	// Keep only X: compressing it again recreates the same X.gz.
	seen := map[string]bool{}
	for _, f := range files {
		seen[f] = true
	}
	var result []string
	for _, f := range files {
		if strings.HasSuffix(f, ".gz") && seen[strings.TrimSuffix(f, ".gz")] {
			continue
		}
		result = append(result, f)
	}
	return result, nil
}

// processFile: compress -> upload (unless already there) -> delete local copy.
func processFile(ctx context.Context, cfg config, conn *swift.Connection, path string, existing map[string]string, st *stats) error {
	if !strings.HasSuffix(path, ".gz") && !strings.HasSuffix(path, ".zip") {
		gzPath, err := compress(path)
		if err != nil {
			return fmt.Errorf("compressing: %w", err)
		}
		st.add(&st.compressed)
		path = gzPath
	}

	rel, _ := filepath.Rel(cfg.dataDir, path)
	name := filepath.ToSlash(rel) // object name in Swift = path under /data

	sum, err := md5File(path)
	if err != nil {
		return err
	}

	if existing[name] == sum {
		st.add(&st.skipped)
	} else {
		if err := uploadWithRetry(ctx, conn, cfg, path, name, sum); err != nil {
			return err
		}
		st.add(&st.uploaded)
	}

	if cfg.deleteLocal {
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("deleting local copy: %w", err)
		}
		st.add(&st.deleted)
	}
	return nil
}

// uploadWithRetry uploads one file. Swift checks the MD5 we send against what it received
// and refuses the file if they differ, so a successful upload is a byte-for-byte copy.
func uploadWithRetry(ctx context.Context, conn *swift.Connection, cfg config, path, name, sum string) error {
	var lastErr error
	for attempt := 1; attempt <= cfg.retries; attempt++ {
		lastErr = uploadOnce(ctx, conn, cfg.container, path, name, sum)
		if lastErr == nil {
			return nil
		}
		time.Sleep(time.Duration(attempt*attempt) * 2 * time.Second) // 2 s, 8 s, 18 s ...
	}
	return fmt.Errorf("upload failed after %d attempts: %w", cfg.retries, lastErr)
}

func uploadOnce(ctx context.Context, conn *swift.Connection, container, path, name, sum string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = conn.ObjectPut(ctx, container, name, f, true, sum, contentType(path), nil)
	return err
}

func contentType(path string) string {
	switch {
	case strings.HasSuffix(path, ".gz"):
		return "application/gzip"
	case strings.HasSuffix(path, ".zip"):
		return "application/zip"
	default:
		return "application/octet-stream"
	}
}

// compress gzips path to path.gz, checks that unzipping gives back exactly the original,
// and only then removes the original. Returns the new path.
func compress(path string) (string, error) {
	gzPath := path + ".gz"
	tmpPath := gzPath + ".tmp"

	in, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return "", err
	}

	out, err := os.Create(tmpPath)
	if err != nil {
		return "", err
	}
	origHash := sha256.New()
	zw := gzip.NewWriter(out)
	zw.Name = filepath.Base(path)
	zw.ModTime = info.ModTime()
	_, err = io.Copy(zw, io.TeeReader(in, origHash))
	if err == nil {
		err = zw.Close()
	}
	if err == nil {
		err = out.Sync()
	}
	if closeErr := out.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		os.Remove(tmpPath)
		return "", err
	}

	// Verify: decompress and compare with the original
	checkHash, err := gunzipHash(tmpPath)
	if err != nil || hex.EncodeToString(checkHash) != hex.EncodeToString(origHash.Sum(nil)) {
		os.Remove(tmpPath)
		return "", fmt.Errorf("verification failed, original kept (%v)", err)
	}

	if err := os.Rename(tmpPath, gzPath); err != nil {
		return "", err
	}
	os.Chtimes(gzPath, info.ModTime(), info.ModTime())
	return gzPath, os.Remove(path)
}

func gunzipHash(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	if _, err := io.Copy(h, zr); err != nil {
		return nil, err
	}
	return h.Sum(nil), zr.Close()
}

func md5File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// ---------------------------------------------------------------- settings and logging

func loadConfig() config {
	return config{
		dataDir:     env("DATA_DIR", "/data"),
		logDir:      env("LOG_DIR", "/logs"),
		container:   env("SWIFT_CONTAINER", "buspulse-raw"),
		deleteLocal: env("DELETE_LOCAL", "true") == "true",
		dryRun:      env("DRY_RUN", "false") == "true",
		workers:     envInt("WORKERS", 4),
		retries:     envInt("RETRIES", 3),
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(key)); err == nil && n > 0 {
		return n
	}
	return def
}

// newLogger writes to stdout and LOG_DIR/uploader.log, same format as the collectors (UTC).
func newLogger(logDir string) *log.Logger {
	var w io.Writer = os.Stdout
	if err := os.MkdirAll(logDir, 0o755); err == nil {
		if f, err := os.OpenFile(filepath.Join(logDir, "uploader.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
			w = io.MultiWriter(os.Stdout, f)
		}
	}
	return log.New(utcWriter{w}, "", 0)
}

type utcWriter struct{ w io.Writer }

func (u utcWriter) Write(p []byte) (int, error) {
	stamp := time.Now().UTC().Format("2006-01-02T15:04:05Z") + " | "
	if _, err := u.w.Write(append([]byte(stamp), p...)); err != nil {
		return 0, err
	}
	return len(p), nil
}
