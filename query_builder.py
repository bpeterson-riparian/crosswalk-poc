"""Dynamic SQL query builder for the crosswalk POC.

Accepts a set of user-selected join IDs, validates them, determines join
order via BFS over the table graph, and produces a complete SELECT statement
with table-prefixed column aliases.
"""

from collections import deque

from metadata import (
    get_join_by_id,
    JOIN_CLASSIFICATIONS,
    TABLE_METADATA,
    JoinDef,
    ClassificationInfo,
)


type QueryResult = tuple[str, list[ClassificationInfo], None] | tuple[None, None, str]


def build_query(selected_join_ids: list[int]) -> QueryResult:
    """Build a dynamic JOIN query from user-selected join IDs.

    Returns (sql, classifications_used, None) on success,
    or (None, None, error) on failure.
    """
    if not selected_join_ids:
        return None, None, "No joins selected."

    selected_joins: list[JoinDef] = []
    for jid in selected_join_ids:
        j = get_join_by_id(jid)
        if j is None:
            return None, None, f"Unknown join ID: {jid}"
        selected_joins.append(j)

    # Joins 3 and 4 both link NPPES <-> HIN — mutually exclusive
    ids: set[int] = {j["id"] for j in selected_joins}
    if 3 in ids and 4 in ids:
        return None, None, (
            "Cannot select both full and partial address joins (3 and 4) "
            "-- they link the same tables."
        )

    # Build adjacency graph for BFS traversal
    adjacency: dict[str, list[tuple[JoinDef, str]]] = {}
    for j in selected_joins:
        lt, rt = j["left_table"], j["right_table"]
        adjacency.setdefault(lt, []).append((j, rt))
        adjacency.setdefault(rt, []).append((j, lt))

    root: str = selected_joins[0]["left_table"]
    visited: set[str] = {root}
    queue: deque[str] = deque([root])
    join_order: list[tuple[JoinDef, str]] = []

    while queue:
        current = queue.popleft()
        for j, neighbor in adjacency.get(current, []):
            if neighbor not in visited:
                visited.add(neighbor)
                join_order.append((j, neighbor))
                queue.append(neighbor)

    all_tables: set[str] = {t for j in selected_joins for t in (j["left_table"], j["right_table"])}
    if visited != all_tables:
        return None, None, (
            "Selected joins do not form a connected set of tables. "
            "Add a join to bridge the gap."
        )

    # Build SELECT clause with table-prefixed aliases to avoid ambiguity
    ordered_tables: list[str] = [root] + [t for _, t in join_order]
    select_cols: list[str] = []
    for table in ordered_tables:
        for col in TABLE_METADATA[table]["columns"]:
            select_cols.append(f"{table}.{col} AS {table}_{col}")

    # Build JOIN clauses
    join_clauses: list[str] = []
    for j, to_table in join_order:
        conditions = " AND ".join(
            f"{j['left_table']}.{lc} = {j['right_table']}.{rc}"
            for lc, rc in j["on"]
        )
        label: str = JOIN_CLASSIFICATIONS[j["classification"]]
        join_clauses.append(f"  JOIN {to_table} ON {conditions}  /* {label} */")

    select_str = ",\n       ".join(select_cols)
    joins_str = "\n".join(join_clauses)
    sql = f"SELECT {select_str}\nFROM {root}\n{joins_str}"

    classifications_used: list[ClassificationInfo] = [
        {
            "join": j["description"],
            "classification": JOIN_CLASSIFICATIONS[j["classification"]],
        }
        for j in selected_joins
    ]

    return sql, classifications_used, None
