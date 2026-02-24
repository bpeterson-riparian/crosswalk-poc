"""Database setup and CSV data loading for the crosswalk POC.

Creates an in-memory SQLite database, reads each data source from its
corresponding CSV file in the data/ directory, and builds indexes on
columns used in join conditions.
"""

import csv
import sqlite3
from pathlib import Path

from metadata import TABLE_METADATA

DATA_DIR: Path = Path(__file__).parent / "data"


def create_connection() -> sqlite3.Connection:
    """Create and return a new in-memory SQLite connection with Row factory."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def load_sample_data(conn: sqlite3.Connection) -> None:
    """Load all data source CSVs into the database and create join indexes.

    For each table in TABLE_METADATA, reads the matching CSV from data/,
    creates the table using the CSV headers (with the configured primary key),
    inserts all rows (treating empty strings as NULL), and creates indexes
    on columns referenced by available join conditions.
    """
    cursor = conn.cursor()

    for table_name, meta in TABLE_METADATA.items():
        csv_path = DATA_DIR / f"{table_name}.csv"
        if not csv_path.exists():
            raise FileNotFoundError(f"Data file not found: {csv_path}")

        with open(csv_path, newline="") as f:
            reader = csv.reader(f)
            headers: list[str] = next(reader)

        pk: str = meta["pk"]
        col_defs = ", ".join(
            f"{col} TEXT PRIMARY KEY" if col == pk else f"{col} TEXT"
            for col in headers
        )
        cursor.execute(f"CREATE TABLE {table_name} ({col_defs})")

        placeholders = ", ".join("?" for _ in headers)
        with open(csv_path, newline="") as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                cleaned: list[str | None] = [v if v != "" else None for v in row]
                cursor.execute(
                    f"INSERT INTO {table_name} VALUES ({placeholders})", cleaned
                )

    cursor.execute("CREATE INDEX idx_ncpdp_npi ON ncpdp(npi)")
    cursor.execute("CREATE INDEX idx_ncpdp_dea_id ON ncpdp(dea_id)")
    cursor.execute("CREATE INDEX idx_nppes_address ON nppes(address, zip)")
    cursor.execute("CREATE INDEX idx_nppes_zip ON nppes(zip)")
    cursor.execute("CREATE INDEX idx_hin_address ON hin(address, zip)")
    cursor.execute("CREATE INDEX idx_hin_zip ON hin(zip)")

    conn.commit()
