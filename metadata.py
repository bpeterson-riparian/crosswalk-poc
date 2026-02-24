"""Registry of data source tables, their known join relationships, and join classifications.

Serves as the single source of truth for the metadata layer that drives
dynamic query construction in the crosswalk POC.
"""

from typing import TypedDict


class TableMeta(TypedDict):
    """Schema metadata for a single data source table."""

    pk: str
    columns: list[str]


class JoinDef(TypedDict):
    """Definition of a single available join between two data source tables."""

    id: int
    left_table: str
    right_table: str
    on: list[tuple[str, str]]
    classification: str
    description: str


class ClassificationInfo(TypedDict):
    """Human-readable summary of a join classification used in a query."""

    join: str
    classification: str


TABLE_METADATA: dict[str, TableMeta] = {
    "nppes": {
        "pk": "npi",
        "columns": ["npi", "address", "zip", "provider_name"],
    },
    "ncpdp": {
        "pk": "ncpdp_id",
        "columns": ["ncpdp_id", "npi", "dea_id", "pharmacy_name"],
    },
    "dea": {
        "pk": "dea_id",
        "columns": ["dea_id", "business_activity"],
    },
    "hin": {
        "pk": "hin_id",
        "columns": ["hin_id", "address", "zip", "class_of_trade"],
    },
}

JOIN_CLASSIFICATIONS: dict[str, str] = {
    "foreign_key": "Foreign Key (ID match)",
    "full_address": "Full Address Match",
    "partial_address": "Partial Address Match",
}

AVAILABLE_JOINS: list[JoinDef] = [
    {
        "id": 1,
        "left_table": "ncpdp",
        "right_table": "nppes",
        "on": [("npi", "npi")],
        "classification": "foreign_key",
        "description": "NCPDP -> NPPES on NPI",
    },
    {
        "id": 2,
        "left_table": "ncpdp",
        "right_table": "dea",
        "on": [("dea_id", "dea_id")],
        "classification": "foreign_key",
        "description": "NCPDP -> DEA on DEA ID",
    },
    {
        "id": 3,
        "left_table": "nppes",
        "right_table": "hin",
        "on": [("address", "address"), ("zip", "zip")],
        "classification": "full_address",
        "description": "NPPES -> HIN on Address + ZIP",
    },
    {
        "id": 4,
        "left_table": "nppes",
        "right_table": "hin",
        "on": [("zip", "zip")],
        "classification": "partial_address",
        "description": "NPPES -> HIN on ZIP only",
    },
]


def get_available_joins() -> list[JoinDef]:
    """Return all registered join definitions."""
    return AVAILABLE_JOINS


def get_join_by_id(join_id: int) -> JoinDef | None:
    """Look up a join definition by its numeric ID, or None if not found."""
    for j in AVAILABLE_JOINS:
        if j["id"] == join_id:
            return j
    return None
