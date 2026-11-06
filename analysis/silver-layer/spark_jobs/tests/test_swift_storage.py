import hashlib
from types import SimpleNamespace

from spark_jobs import common


class FakeSwift:
    def __init__(self):
        self.objects = {"raw": {"feed/date=2026-10-22/file.json.gz": b"raw"}}

    def get_container(self, bucket, prefix="", full_listing=False, delimiter=None):
        objects = self.objects.setdefault(bucket, {})
        selected = sorted(name for name in objects if name.startswith(prefix))
        if delimiter:
            dirs = sorted({name.split("/", 1)[0] + "/" for name in objects if "/" in name})
            return {}, [{"subdirectory": name} for name in dirs]
        return {}, [
            {"name": name, "hash": hashlib.md5(objects[name]).hexdigest(), "bytes": len(objects[name])}
            for name in selected
        ]

    def get_object(self, bucket, name, resp_chunk_size=None):
        content = self.objects[bucket][name]
        return {}, iter((content[i:i + 2] for i in range(0, len(content), 2)))

    def put_object(self, bucket, name, contents, content_length=None, content_type=None):
        if hasattr(contents, "read"):
            contents = contents.read()
        self.objects.setdefault(bucket, {})[name] = contents

    def delete_object(self, bucket, name):
        del self.objects[bucket][name]

    def head_container(self, bucket):
        if bucket not in self.objects:
            raise AssertionError("unexpected missing container")


def test_swift_path_stages_objects_and_upload_replaces_prefix(tmp_path, monkeypatch):
    swift = FakeSwift()
    monkeypatch.delenv("DATA_ROOT", raising=False)
    monkeypatch.setattr(common, "SWIFT_CACHE", tmp_path)
    monkeypatch.setattr(common._swift_local, "connection", swift, raising=False)
    common._downloaded.clear()

    input_path = common.path("raw", "feed", "date=2026-10-22")
    assert (tmp_path / "raw/feed/date=2026-10-22/file.json.gz").read_bytes() == b"raw"
    assert input_path.endswith("raw/feed/date=2026-10-22")

    output = tmp_path / "bronze/table/date=2026-10-22"
    output.mkdir(parents=True)
    (output / "part-0.parquet").write_bytes(b"parquet")
    (output / "_SUCCESS").write_bytes(b"")
    swift.objects["bronze"] = {
        "table/date=2026-10-22/stale.parquet": b"stale",
        "table/date=2026-10-22/part-0.parquet": b"old",
    }

    common._upload_prefix("bronze", "table/date=2026-10-22", str(output))

    assert swift.objects["bronze"] == {
        "table/date=2026-10-22/part-0.parquet": b"parquet",
        "table/date=2026-10-22/_SUCCESS": b"",
    }


def test_swift_downloads_many_files_in_parallel(tmp_path, monkeypatch):
    swift = FakeSwift()
    swift.objects["raw"] = {f"feed/date=2026-10-22/f{i:03d}.json.gz": bytes([i]) * 10 for i in range(50)}
    monkeypatch.delenv("DATA_ROOT", raising=False)
    monkeypatch.setattr(common, "SWIFT_CACHE", tmp_path)
    monkeypatch.setattr(common, "swift_connection", lambda: swift)   # every worker thread gets the fake
    monkeypatch.setattr(common, "DOWNLOAD_THREADS", 4)
    common._downloaded.clear()

    common.path("raw", "feed", "date=2026-10-22")
    files = sorted((tmp_path / "raw/feed/date=2026-10-22").iterdir())
    assert len(files) == 50 and files[7].read_bytes() == bytes([7]) * 10
    assert not list(tmp_path.rglob("*.swift-tmp"))


def test_raw_feeds_reads_swift_subdir_entries(monkeypatch):
    class Listing:
        def get_container(self, bucket, delimiter=None):
            return {}, [{"subdir": "estimated_timetable/"}, {"subdir": "vehicle_positions/"}]
    monkeypatch.setattr(common._swift_local, "connection", Listing(), raising=False)
    assert common.raw_feeds() == ["estimated_timetable", "vehicle_positions"]


def test_bronze_source_name_is_relative_to_swift_container():
    import gzip
    from spark_jobs import bronze

    assert bronze._object_name(
        "/tmp/buspulse-swift/buspulse-raw/feed/date=2026-10-22/file.json.gz"
    ) == "feed/date=2026-10-22/file.json.gz"
    records = list(bronze._rows(
        [SimpleNamespace(
            path="/tmp/buspulse-swift/buspulse-raw/feed/date=2026-10-22/broken.json.gz",
            content=b"not gzip",
        )],
        lambda payload, base: iter(()),
        ["snapshot_time", "source_file"],
    ))
    assert records[0][-2] == "feed/date=2026-10-22/broken.json.gz"
    assert "Not a gzipped file" in records[0][-1]
