package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ncw/swift/v2"
	"github.com/ncw/swift/v2/swifttest"
)

// Runs the uploader against an in-memory Swift server and checks that:
// old days are compressed, uploaded byte-for-byte and deleted; today is untouched;
// a second run finds nothing left to do.
func TestUploader(t *testing.T) {
	srv, err := swifttest.NewSwiftServer("localhost")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	t.Setenv("OS_AUTH_URL", srv.AuthURL)
	t.Setenv("OS_USERNAME", swifttest.TEST_ACCOUNT)
	t.Setenv("OS_PASSWORD", swifttest.TEST_ACCOUNT)

	dataDir := t.TempDir()
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)

	xml := bytes.Repeat([]byte("<EstimatedCall><Order>1</Order></EstimatedCall>"), 1000)
	write(t, dataDir, "estimated_timetable/date=2026-10-21/estimated_timetable_20261021T120000Z.xml", xml)
	write(t, dataDir, "vehicle_positions/date=2026-10-20/vehicle_positions_20261020T120000Z.json", []byte(`{"data":{}}`))
	write(t, dataDir, "static_timetable/date=2026-10-21/static_timetable_20261021T000000Z.zip", []byte("PK fake zip"))
	write(t, dataDir, "vehicle_positions/date=2026-10-22/vehicle_positions_20261022T110000Z.json", []byte(`{"today":1}`))

	cfg := config{dataDir: dataDir, container: "buspulse-raw", deleteLocal: true, workers: 2, retries: 2}
	logger := log.New(io.Discard, "", 0)

	st, err := run(context.Background(), cfg, &swift.Connection{}, logger, now)
	if err != nil {
		t.Fatal(err)
	}
	if st.compressed != 2 || st.uploaded != 3 || st.deleted != 3 || st.failed != 0 {
		t.Fatalf("unexpected counts: %+v", st)
	}

	// The ET file in Swift unzips to exactly the original bytes
	conn := &swift.Connection{AuthUrl: srv.AuthURL, UserName: swifttest.TEST_ACCOUNT, ApiKey: swifttest.TEST_ACCOUNT}
	ctx := context.Background()
	if err := conn.Authenticate(ctx); err != nil {
		t.Fatal(err)
	}
	gz, err := conn.ObjectGetBytes(ctx, "buspulse-raw",
		"estimated_timetable/date=2026-10-21/estimated_timetable_20261021T120000Z.xml.gz")
	if err != nil {
		t.Fatal(err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(zr)
	if !bytes.Equal(got, xml) {
		t.Fatal("uploaded file does not unzip to the original")
	}

	// The zip was uploaded as-is, not gzipped again
	if _, _, err := conn.Object(ctx, "buspulse-raw",
		"static_timetable/date=2026-10-21/static_timetable_20261021T000000Z.zip"); err != nil {
		t.Fatal("zip not uploaded as-is:", err)
	}

	// Old day folders are gone, today's file is untouched
	if _, err := os.Stat(filepath.Join(dataDir, "estimated_timetable/date=2026-10-21")); !os.IsNotExist(err) {
		t.Fatal("old day folder was not removed")
	}
	if _, err := os.Stat(filepath.Join(dataDir, "vehicle_positions/date=2026-10-22/vehicle_positions_20261022T110000Z.json")); err != nil {
		t.Fatal("today's file was touched:", err)
	}

	// Second run: nothing left to do
	st, err = run(context.Background(), cfg, &swift.Connection{}, logger, now)
	if err != nil {
		t.Fatal(err)
	}
	if st.uploaded != 0 || st.failed != 0 {
		t.Fatalf("second run should do nothing: %+v", st)
	}
}

// With DELETE_LOCAL=false, a re-run skips files that are already in Swift.
func TestSkipAlreadyUploaded(t *testing.T) {
	srv, err := swifttest.NewSwiftServer("localhost")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	t.Setenv("OS_AUTH_URL", srv.AuthURL)
	t.Setenv("OS_USERNAME", swifttest.TEST_ACCOUNT)
	t.Setenv("OS_PASSWORD", swifttest.TEST_ACCOUNT)

	dataDir := t.TempDir()
	write(t, dataDir, "service_alerts/date=2026-10-21/service_alerts_20261021T080000Z.xml", []byte("<Siri/>"))
	cfg := config{dataDir: dataDir, container: "buspulse-raw", deleteLocal: false, workers: 1, retries: 1}
	logger := log.New(io.Discard, "", 0)
	now := time.Date(2026, 10, 22, 0, 30, 0, 0, time.UTC)

	if st, err := run(context.Background(), cfg, &swift.Connection{}, logger, now); err != nil || st.uploaded != 1 {
		t.Fatalf("first run: %+v %v", st, err)
	}
	st, err := run(context.Background(), cfg, &swift.Connection{}, logger, now)
	if err != nil || st.skipped != 1 || st.uploaded != 0 {
		t.Fatalf("second run should skip: %+v %v", st, err)
	}
}

func write(t *testing.T, dir, rel string, content []byte) {
	t.Helper()
	p := filepath.Join(dir, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, content, 0o644); err != nil {
		t.Fatal(err)
	}
}
