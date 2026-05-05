"""
MCP Tools Package
Exports all three MCP tools + utility helpers.
"""
from .data_cleaner   import clean_data
from .data_analyzer  import analyze_data
from .data_visualizer import visualize_data

# Legacy utility helpers (used by app.py)
import os
import csv
import hashlib
from datetime import datetime


def validate_csv(filepath):
    """
    Validate a CSV file for basic integrity.
    Returns (is_valid, message, stats).
    """
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            headers = next(reader, None)
            if not headers:
                return False, "CSV file is empty or has no headers.", {}

            row_count = 0
            col_count = len(headers)
            for _ in reader:
                row_count += 1

            if row_count == 0:
                return False, "CSV file has headers but no data rows.", {}

            return True, "Valid CSV file.", {
                "rows": row_count,
                "columns": col_count,
                "headers": headers
            }
    except Exception as e:
        return False, f"Error reading CSV: {str(e)}", {}


def get_file_hash(filepath):
    """Generate MD5 hash of a file."""
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()[:12]


def format_file_size(size_bytes):
    """Format file size in human-readable format."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"


def get_upload_metadata(filepath, filename):
    """Get metadata for an uploaded file."""
    stat = os.stat(filepath)
    return {
        "filename": filename,
        "size": format_file_size(stat.st_size),
        "size_bytes": stat.st_size,
        "uploaded_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        "hash": get_file_hash(filepath)
    }


def list_uploaded_files(upload_dir):
    """List all uploaded CSV files with metadata."""
    files = []
    if not os.path.exists(upload_dir):
        return files

    for fname in os.listdir(upload_dir):
        if fname.lower().endswith(".csv"):
            fpath = os.path.join(upload_dir, fname)
            files.append(get_upload_metadata(fpath, fname))

    files.sort(key=lambda x: x["uploaded_at"], reverse=True)
    return files


__all__ = [
    "clean_data",
    "analyze_data",
    "visualize_data",
    "validate_csv",
    "get_file_hash",
    "format_file_size",
    "get_upload_metadata",
    "list_uploaded_files",
]
