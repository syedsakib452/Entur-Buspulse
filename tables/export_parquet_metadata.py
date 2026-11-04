import argparse
import json
from pathlib import Path

import pyarrow.parquet as pq


DEFAULT_INPUT_DIR = Path(__file__).parent / "parquet_2026-10-24" / "clean"


def describe_parquet(path: Path, base_dir: Path) -> dict:
    parquet_file = pq.ParquetFile(path)
    metadata = parquet_file.metadata
    size_bytes = path.stat().st_size

    return {
        "file": path.relative_to(base_dir).as_posix(),
        "size_bytes": size_bytes,
        "size_mib": round(size_bytes / (1024 * 1024), 2),
        "rows": metadata.num_rows,
        "columns_count": metadata.num_columns,
        "row_groups": metadata.num_row_groups,
        "columns": [
            {"name": field.name, "type": str(field.type)}
            for field in parquet_file.schema_arrow
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export Parquet file metadata without reading table contents."
    )
    parser.add_argument(
        "input_dir",
        nargs="?",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help=f"folder to scan (default: {DEFAULT_INPUT_DIR})",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="JSON output path (default: <input_dir>/parquet_metadata.json)",
    )
    args = parser.parse_args()

    input_dir = args.input_dir.resolve()
    if not input_dir.is_dir():
        parser.error(f"input directory does not exist: {input_dir}")

    files = sorted(input_dir.glob("*.parquet"))
    if not files:
        parser.error(f"no Parquet files found in: {input_dir}")

    output_path = args.output or input_dir / "parquet_metadata.json"
    manifest = {
        "source_directory": input_dir.name,
        "files": [describe_parquet(path, input_dir) for path in files],
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote metadata for {len(files)} Parquet files to {output_path}")


if __name__ == "__main__":
    main()
