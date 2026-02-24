# Crosswalk POC

Proof of concept for dynamically linking healthcare data sources at query time using user-selected joins.

## Problem

Multiple data sources (NPPES, NCPDP, DEA, HIN) each have unique identifiers. Some share foreign key relationships, others can only be linked by address matching. Users need to choose which linkages to activate and see the combined results — with visibility into *how* each link was made.

## Approach

Data sources are loaded from CSV into an in-memory SQLite database. A metadata layer tracks available joins between tables and classifies each by type:

| # | Join | Classification |
|---|------|----------------|
| 1 | NCPDP → NPPES on NPI | Foreign Key (ID match) |
| 2 | NCPDP → DEA on DEA ID | Foreign Key (ID match) |
| 3 | NPPES → HIN on Address + ZIP | Full Address Match |
| 4 | NPPES → HIN on ZIP only | Partial Address Match |

The user selects which joins to include. A query builder constructs the SQL dynamically using BFS to determine join order, then executes it and displays the results.

## Data Sources

| Table | Primary Key | Example Column |
|-------|-------------|----------------|
| NPPES | npi | provider_name |
| NCPDP | ncpdp_id | pharmacy_name |
| DEA | dea_id | business_activity |
| HIN | hin_id | class_of_trade |

Sample data lives in `data/` as CSV files.

## Usage

```
python main.py
```

The CLI shows available joins and prompts for a comma-separated selection:

```
Select joins (comma-separated, e.g. 1,2,3) or 'q' to quit: 1,2,4
```

Output includes the join classifications used, the generated SQL, and the query results.

## Project Structure

```
├── main.py            # Interactive CLI
├── db.py              # In-memory SQLite setup and CSV loading
├── metadata.py        # Table registry, join definitions, classifications
├── query_builder.py   # Dynamic SQL generation via BFS
└── data/
    ├── nppes.csv
    ├── ncpdp.csv
    ├── dea.csv
    └── hin.csv
```

## Key Trade-offs

- **Full vs partial address**: Full address (address + zip) yields fewer, higher-confidence matches. Partial (zip only) casts a wider net at the cost of precision.
- **Dynamic joins**: Queries are built and executed at runtime — no materialized views. Simple and always fresh, but performance scales with table size and join count.
