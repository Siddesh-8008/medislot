"""The same index set must appear in the schema, the RDS check Lambda, the manual migration and the docs."""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXPECTED = {
    "ix_hospitals_city": ("hospitals", ("city", "is_active")),
    "ix_doctors_spec_hospital": ("doctors", ("specialization_id", "hospital_id")),
    "ix_doctors_hospital_id": ("doctors", ("hospital_id",)),
    "ix_doctors_name": ("doctors", ("last_name", "first_name")),
    "ix_doctors_first_name": ("doctors", ("first_name",)),
    "ix_doctor_consultation_active": ("doctor_consultation_types", ("doctor_id", "is_active", "fee")),
}


def index_defs(text):
    """Every `KEY|INDEX name (cols)` inside a CREATE TABLE, and every `CREATE INDEX name ON table (cols)`."""
    found, table = {}, None
    for line in text.splitlines():
        m = re.search(r"CREATE TABLE\s+(?:IF NOT EXISTS\s+)?(\w+)", line, re.I)
        if m:
            table = m.group(1)
            continue
        m = re.match(r"\s*(?:UNIQUE\s+)?(?:KEY|INDEX)\s+(\w+)\s*\(([^)]*)\)", line, re.I)
        if m and table:
            found[m.group(1)] = (table, tuple(c.strip() for c in m.group(2).split(",")))
        m = re.search(r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(\w+)\s+ON\s+(\w+)\s*\(([^)]*)\)", line, re.I)
        if m:
            found[m.group(1)] = (m.group(2), tuple(c.strip() for c in m.group(3).split(",")))
    return found


def test_schema_sql_has_every_search_index():
    found = index_defs((ROOT / "scehma" / "schema.sql").read_text())
    for name, expected in EXPECTED.items():
        assert found.get(name) == expected, name
    assert "ix_doctors_specialization_id" not in found, "replaced by the composite (specialization_id, hospital_id)"


def test_rds_check_lambda_creates_and_reports_the_same_indexes():
    text = (ROOT / "lambdas" / "rds_check" / "lambda_function.py").read_text()
    found = index_defs(text)
    for name, expected in EXPECTED.items():
        assert found.get(name) == expected, name
    required = re.search(r"REQUIRED_INDEXES\s*=\s*\{(.*?)\n\}", text, re.S).group(1)
    for name in EXPECTED:
        assert f'"{name}"' in required, name


def test_schema_sql_is_the_only_sql_file():
    """All tables and indexes live in scehma/schema.sql (the separate Milestone 4 index migration was folded in)."""
    assert sorted(p.name for p in (ROOT / "scehma").glob("*.sql")) == ["schema.sql"]
    text = (ROOT / "scehma" / "schema.sql").read_text()
    for name in ("ix_hospitals_city", "ix_doctors_spec_hospital", "ix_doctors_first_name", "ix_doctor_consultation_active"):
        assert name in text, name


def test_every_new_lambda_is_wired_into_ci_and_cloudformation():
    deploy = (ROOT / ".github" / "workflows" / "deploy.yaml").read_text()
    build = (ROOT / "scripts" / "build-lambda.sh").read_text()
    stack = (ROOT / "cloudformation" / "auth-stack.yaml").read_text()
    for fn in ("patient_domain", "doctor_discovery"):
        assert fn in deploy and fn in build and f"{fn}.zip" in stack, fn
    assert "patient_portal.py" in deploy
