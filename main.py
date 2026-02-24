"""Interactive CLI for the crosswalk POC.

Loads data sources into an in-memory SQLite database, presents available
join options to the user, builds and executes dynamic queries based on
their selections, and displays the results.
"""

import sqlite3
from typing import Any, Sequence

from db import create_connection, load_sample_data
from metadata import get_available_joins, TABLE_METADATA, JOIN_CLASSIFICATIONS
from query_builder import build_query


def format_table(headers: list[str], rows: Sequence[Sequence[Any]], max_col_width: int = 30) -> str:
    """Format query results as an aligned ASCII table string."""
    if not rows:
        return "  No results."

    str_rows: list[list[str]] = [
        [
            str(v)[:max_col_width] if v is not None else "NULL"
            for v in row
        ]
        for row in rows
    ]

    widths: list[int] = [len(h) for h in headers]
    for row in str_rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(val))

    header_line = " | ".join(h.ljust(w) for h, w in zip(headers, widths))
    separator = "-+-".join("-" * w for w in widths)

    lines: list[str] = ["  " + header_line, "  " + separator]
    for row in str_rows:
        line = " | ".join(val.ljust(w) for val, w in zip(row, widths))
        lines.append("  " + line)

    return "\n".join(lines)


def display_data_sources(conn: sqlite3.Connection) -> None:
    """Print a summary of each loaded data source with its row count and primary key."""
    print("\nData Sources:")
    for table_name, meta in TABLE_METADATA.items():
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count: int = cursor.fetchone()[0]
        print(f"  {table_name.upper():<8} {count:>3} rows  (PK: {meta['pk']})")


def display_available_joins() -> None:
    """Print the numbered list of available joins with their classifications."""
    print("\nAvailable Joins:")
    for j in get_available_joins():
        classification: str = JOIN_CLASSIFICATIONS[j["classification"]]
        print(f"  [{j['id']}] {j['description']}  ({classification})")


def run_query(conn: sqlite3.Connection, selected_ids: list[int]) -> None:
    """Build a query from the selected joins, execute it, and print the results."""
    sql, classifications, error = build_query(selected_ids)

    if error:
        print(f"\n  Error: {error}")
        return

    print("\n--- Join Classifications Used ---")
    for c in classifications:
        print(f"  {c['join']}: {c['classification']}")

    print(f"\n--- Generated SQL ---\n{sql}\n")

    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        rows: list[tuple[Any, ...]] = cursor.fetchall()
        headers: list[str] = [desc[0] for desc in cursor.description]
        print(f"--- Results ({len(rows)} rows) ---")
        print(format_table(headers, rows))
    except sqlite3.Error as e:
        print(f"  SQL Error: {e}")


def main() -> None:
    """Entry point: load data, show options, and loop on user join selections."""
    conn = create_connection()
    load_sample_data(conn)

    print("=" * 50)
    print("  Crosswalk Demo - Dynamic Join POC")
    print("=" * 50)

    display_data_sources(conn)
    display_available_joins()

    while True:
        print()
        selection = input("Select joins (comma-separated, e.g. 1,2,3) or 'q' to quit: ").strip()

        if selection.lower() == "q":
            break

        try:
            selected_ids: list[int] = [int(x.strip()) for x in selection.split(",")]
        except ValueError:
            print("  Invalid input. Enter join numbers separated by commas.")
            continue

        run_query(conn, selected_ids)

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
