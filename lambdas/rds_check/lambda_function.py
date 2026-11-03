
"""
RDS connection check and database table creation.

Creates all MediSlot tables and inserts reference data during deployment.

- Creates missing tables.
- Inserts hospitals.
- Inserts specializations.
- Inserts four standard consultation types.
- Verifies all required tables.
- Never deletes existing application records.
"""

import socket
import time

from common import config, dbconn, emit, instrument


# All tables expected by MediSlot.
REQUIRED_TABLES = {
    "users",
    "patients",
    "hospitals",
    "specializations",
    "doctors",
    "consultation_types",
    "doctor_consultation_types",
    "availability_patterns",
    "blackout_dates",
}


# ---------------------------------------------------------
# 0. INDEXES THE PATIENT SEARCH RELIES ON (Milestone 4)
# ---------------------------------------------------------
# Used only to REPORT whether the deployed database has them (the CI pipeline fails with a clear message if
# not). Nothing here creates or alters an index: existing databases get them from scehma/milestone4_indexes.sql.
REQUIRED_INDEXES = {
    "doctors": {
        "ix_doctors_spec_hospital": ("specialization_id", "hospital_id"),
        "ix_doctors_hospital_id": ("hospital_id",),
        "ix_doctors_name": ("last_name", "first_name"),
        "ix_doctors_first_name": ("first_name",),
    },
    "hospitals": {
        "ix_hospitals_city": ("city", "is_active"),
    },
    "doctor_consultation_types": {
        "ix_doctor_consultation_active": ("doctor_id", "is_active", "fee"),
    },
}


# ---------------------------------------------------------
# 1. TABLE CREATION
# ---------------------------------------------------------

# Foreign-key dependency order is important.
CREATE_TABLES = [

    # USERS
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        email VARCHAR(254) NOT NULL,
        password_hash VARCHAR(255) NOT NULL,
        role ENUM('PATIENT', 'DOCTOR') NOT NULL,
        phone VARCHAR(25) NULL,
        is_active TINYINT(1) NOT NULL DEFAULT 1,
        created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (user_id),
        UNIQUE KEY uq_users_email (email),
        UNIQUE KEY uq_users_phone (phone),
        KEY ix_users_role (role),
        KEY ix_users_is_active (is_active)
    ) ENGINE=InnoDB
    """,

    # HOSPITALS
    """
    CREATE TABLE IF NOT EXISTS hospitals (
        hospital_id INT UNSIGNED NOT NULL,
        hospital_name VARCHAR(180) NOT NULL,
        address VARCHAR(500) NOT NULL,
        city VARCHAR(100) NOT NULL DEFAULT 'Hyderabad',
        state VARCHAR(100) NOT NULL DEFAULT 'Telangana',
        phone VARCHAR(25) NULL,
        is_active TINYINT(1) NOT NULL DEFAULT 1,
        created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (hospital_id),
        UNIQUE KEY uq_hospitals_name (hospital_name),
        KEY ix_hospitals_active (is_active),
        KEY ix_hospitals_city (city, is_active)
    ) ENGINE=InnoDB
    """,

    # SPECIALIZATIONS
    """
    CREATE TABLE IF NOT EXISTS specializations (
        specialization_id INT UNSIGNED NOT NULL,
        specialization_name VARCHAR(120) NOT NULL,
        is_active TINYINT(1) NOT NULL DEFAULT 1,
        created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (specialization_id),
        UNIQUE KEY uq_specializations_name (specialization_name),
        KEY ix_specializations_active (is_active)
    ) ENGINE=InnoDB
    """,

    # PATIENTS
    """
    CREATE TABLE IF NOT EXISTS patients (
        patient_id BIGINT UNSIGNED NOT NULL,
        first_name VARCHAR(75) NOT NULL,
        last_name VARCHAR(75) NOT NULL,
        date_of_birth DATE NULL,
        gender VARCHAR(30) NULL,
        address VARCHAR(500) NULL,
        created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (patient_id),
        CONSTRAINT fk_patients_user FOREIGN KEY (patient_id)
            REFERENCES users(user_id) ON DELETE CASCADE
    ) ENGINE=InnoDB
    """,

    # DOCTORS
    """
    CREATE TABLE IF NOT EXISTS doctors (
        doctor_id BIGINT UNSIGNED NOT NULL,
        first_name VARCHAR(75) NOT NULL,
        last_name VARCHAR(75) NOT NULL,
        hospital_id INT UNSIGNED NOT NULL,
        specialization_id INT UNSIGNED NOT NULL,
        license_number VARCHAR(32) NOT NULL,
        years_experience SMALLINT UNSIGNED NULL,
        clinic_address VARCHAR(500) NULL,
        qualifications TEXT NULL,
        languages_spoken JSON NULL,
        timezone VARCHAR(64) NOT NULL DEFAULT 'Asia/Kolkata',
        photo_key VARCHAR(1024) NULL,
        created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (doctor_id),
        UNIQUE KEY uq_doctors_license_number (license_number),
        KEY ix_doctors_spec_hospital (specialization_id, hospital_id),
        KEY ix_doctors_hospital_id (hospital_id),
        KEY ix_doctors_name (last_name, first_name),
        KEY ix_doctors_first_name (first_name),
        CONSTRAINT fk_doctors_user FOREIGN KEY (doctor_id)
            REFERENCES users(user_id) ON DELETE CASCADE,
        CONSTRAINT fk_doctors_hospital FOREIGN KEY (hospital_id)
            REFERENCES hospitals(hospital_id),
        CONSTRAINT fk_doctors_specialization FOREIGN KEY (specialization_id)
            REFERENCES specializations(specialization_id)
    ) ENGINE=InnoDB
    """,

    # CONSULTATION TYPES
    # Master table containing the four standard consultation types.
    """
    CREATE TABLE IF NOT EXISTS consultation_types (
        consultation_type_id INT UNSIGNED NOT NULL,
        name VARCHAR(100) NOT NULL,
        duration_minutes SMALLINT UNSIGNED NOT NULL,
        min_capacity SMALLINT UNSIGNED NOT NULL DEFAULT 1,
        max_capacity SMALLINT UNSIGNED NOT NULL DEFAULT 1,
        is_active TINYINT(1) NOT NULL DEFAULT 1,
        created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (consultation_type_id),
        UNIQUE KEY uq_consultation_types_name (name),
        KEY ix_consultation_types_active (is_active),
        CONSTRAINT chk_consultation_types_capacity
            CHECK (min_capacity >= 1 AND max_capacity >= min_capacity)
    ) ENGINE=InnoDB
    """,

    # DOCTOR CONSULTATION TYPES
    # Stores the consultation types actually selected by each doctor.
    """
    CREATE TABLE IF NOT EXISTS doctor_consultation_types (
        doctor_consultation_type_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        doctor_id BIGINT UNSIGNED NOT NULL,
        consultation_type_id INT UNSIGNED NOT NULL,
        fee DECIMAL(10,2) NOT NULL,
        capacity SMALLINT UNSIGNED NOT NULL DEFAULT 1,
        is_active TINYINT(1) NOT NULL DEFAULT 1,
        created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (doctor_consultation_type_id),
        UNIQUE KEY uq_doctor_consultation
            (doctor_id, consultation_type_id),
        KEY ix_doctor_consultation_active (doctor_id, is_active, fee),
        KEY ix_doctor_consultation_type (consultation_type_id),
        CONSTRAINT fk_doctor_consultation_doctor
            FOREIGN KEY (doctor_id)
            REFERENCES doctors(doctor_id),
        CONSTRAINT fk_doctor_consultation_type
            FOREIGN KEY (consultation_type_id)
            REFERENCES consultation_types(consultation_type_id),
        CONSTRAINT chk_doctor_consultation_fee CHECK (fee >= 0)
    ) ENGINE=InnoDB
    """,

    # AVAILABILITY PATTERNS
    # References the doctor's selected consultation type.
    """
    CREATE TABLE IF NOT EXISTS availability_patterns (
        pattern_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        doctor_id BIGINT UNSIGNED NOT NULL,
        consultation_type_id INT UNSIGNED NOT NULL,
        day_of_week TINYINT UNSIGNED NOT NULL
            COMMENT 'ISO weekday: 1 Monday through 7 Sunday',
        start_time TIME NOT NULL,
        end_time TIME NOT NULL,
        is_active TINYINT(1) NOT NULL DEFAULT 1,
        created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (pattern_id),
        KEY ix_availability_doctor_day_active
            (doctor_id, day_of_week, is_active),
        KEY ix_availability_consultation (consultation_type_id),
        CONSTRAINT fk_availability_doctor
            FOREIGN KEY (doctor_id)
            REFERENCES doctors(doctor_id),
        CONSTRAINT fk_availability_doctor_consultation
            FOREIGN KEY (doctor_id, consultation_type_id)
            REFERENCES doctor_consultation_types
                (doctor_id, consultation_type_id),
        CONSTRAINT chk_availability_day
            CHECK (day_of_week BETWEEN 1 AND 7),
        CONSTRAINT chk_availability_time
            CHECK (end_time > start_time)
    ) ENGINE=InnoDB
    """,

    # BLACKOUT DATES
    """
    CREATE TABLE IF NOT EXISTS blackout_dates (
        blackout_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        doctor_id BIGINT UNSIGNED NOT NULL,
        blackout_date DATE NOT NULL,
        reason VARCHAR(255) NULL,
        created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        PRIMARY KEY (blackout_id),
        UNIQUE KEY uq_blackout_doctor_date
            (doctor_id, blackout_date),
        KEY ix_blackout_doctor_date
            (doctor_id, blackout_date),
        CONSTRAINT fk_blackout_doctor
            FOREIGN KEY (doctor_id)
            REFERENCES doctors(doctor_id)
    ) ENGINE=InnoDB
    """,
]


# ---------------------------------------------------------
# 2. HOSPITAL REFERENCE DATA
# ---------------------------------------------------------

HOSPITAL_SEED = [
    (1, "Apollo Hospitals Jubilee Hills",
     "Road No. 72, Film Nagar, Jubilee Hills"),

    (2, "Yashoda Hospitals Somajiguda",
     "Raj Bhavan Road, Somajiguda"),

    (3, "KIMS Hospitals Secunderabad",
     "Minister Road, Secunderabad"),

    (4, "CARE Hospitals Banjara Hills",
     "Road No. 1, Banjara Hills"),

    (5, "AIG Hospitals Gachibowli",
     "Mindspace Road, Gachibowli"),

    (6, "Continental Hospitals",
     "Financial District, Nanakramguda"),

    (7, "Rainbow Children's Hospital Banjara Hills",
     "Road No. 10, Banjara Hills"),

    (8, "NIMS Hyderabad",
     "Punjagutta"),

    (9, "Osmania General Hospital",
     "Afzal Gunj"),

    (10, "Gandhi Hospital",
     "Musheerabad, Secunderabad"),
]


# ---------------------------------------------------------
# 3. SPECIALIZATION REFERENCE DATA
# ---------------------------------------------------------

SPECIALIZATION_SEED = [
    "Cardiology",
    "Dermatology",
    "Endocrinology",
    "Family Medicine",
    "Gastroenterology",
    "General Medicine",
    "General Surgery",
    "Geriatrics",
    "Gynecology",
    "Hematology",
    "Nephrology",
    "Neurology",
    "Neurosurgery",
    "Oncology",
    "Ophthalmology",
    "Orthopedics",
    "Otolaryngology (ENT)",
    "Pediatrics",
    "Psychiatry",
    "Pulmonology",
    "Radiology",
    "Rheumatology",
    "Urology",
    "Anesthesiology",
    "Emergency Medicine",
    "Infectious Disease",
    "Physical Medicine and Rehabilitation",
    "Plastic Surgery",
    "Vascular Surgery",
    "Obstetrics",
]


# ---------------------------------------------------------
# 4. CONSULTATION TYPE REFERENCE DATA
# ---------------------------------------------------------

CONSULTATION_TYPE_SEED = [
    (1, "Quick Consult", 15, 1, 1, 1),
    (2, "Standard Consult", 30, 1, 1, 1),
    (3, "Detailed Consult", 60, 1, 1, 1),
    (4, "Health Camp Session", 60, 2, 10, 1),
]


# ---------------------------------------------------------
# 5. TCP CONNECTIVITY DIAGNOSTICS
# ---------------------------------------------------------

def _tcp_probe(host, port, timeout=3):
    """Diagnose DNS and TCP connectivity problems."""

    result = {
        "host": host,
        "port": port
    }

    try:
        result["resolved_ips"] = sorted({
            item[4][0]
            for item in socket.getaddrinfo(
                host,
                port,
                socket.AF_INET,
                socket.SOCK_STREAM
            )
        })

    except Exception as exc:
        result["dns_error"] = repr(exc)
        return result

    start = time.time()

    try:
        with socket.create_connection(
            (host, port),
            timeout=timeout
        ):
            result["tcp_connect"] = "ok"

    except Exception as exc:
        result["tcp_connect"] = f"failed: {exc!r}"

    result["tcp_ms"] = int(
        (time.time() - start) * 1000
    )

    return result


# ---------------------------------------------------------
# 6. CREATE TABLES AND INSERT REFERENCE DATA
# ---------------------------------------------------------

def _create_tables(conn):
    """Create missing tables and insert reference data."""

    with conn.cursor() as cur:

        # Create tables in foreign-key dependency order.
        for statement in CREATE_TABLES:
            cur.execute(statement)

        # Compatibility check for older doctors tables.
        # CREATE TABLE IF NOT EXISTS does not add new columns.
        cur.execute(
            "SHOW COLUMNS FROM doctors LIKE 'photo_key'"
        )

        if not cur.fetchone():
            cur.execute(
                """
                ALTER TABLE doctors
                ADD COLUMN photo_key VARCHAR(1024) NULL
                """
            )

        # Insert hospitals.
        cur.executemany(
            """
            INSERT INTO hospitals
                (
                    hospital_id,
                    hospital_name,
                    address,
                    city,
                    state,
                    is_active
                )
            VALUES (%s, %s, %s, 'Hyderabad', 'Telangana', 1)

            ON DUPLICATE KEY UPDATE
                hospital_name = VALUES(hospital_name),
                address = VALUES(address),
                city = VALUES(city),
                state = VALUES(state),
                is_active = VALUES(is_active)
            """,
            HOSPITAL_SEED
        )

        # Insert specializations.
        cur.executemany(
            """
            INSERT INTO specializations
                (
                    specialization_id,
                    specialization_name,
                    is_active
                )
            VALUES (%s, %s, 1)

            ON DUPLICATE KEY UPDATE
                specialization_name = VALUES(specialization_name),
                is_active = VALUES(is_active)
            """,
            [
                (i, name)
                for i, name in enumerate(
                    SPECIALIZATION_SEED,
                    start=1
                )
            ]
        )

        # Insert the four standard consultation types.
        cur.executemany(
            """
            INSERT INTO consultation_types
                (
                    consultation_type_id,
                    name,
                    duration_minutes,
                    min_capacity,
                    max_capacity,
                    is_active
                )
            VALUES (%s, %s, %s, %s, %s, %s)

            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                duration_minutes = VALUES(duration_minutes),
                min_capacity = VALUES(min_capacity),
                max_capacity = VALUES(max_capacity),
                is_active = VALUES(is_active)
            """,
            CONSULTATION_TYPE_SEED
        )

    conn.commit()


# ---------------------------------------------------------
# 7. VERIFY DATABASE TABLES
# ---------------------------------------------------------

def _list_tables(conn):
    """Return all tables available in the connected database."""

    with conn.cursor() as cur:

        cur.execute("SELECT 1 AS ok")
        cur.fetchone()

        cur.execute("SHOW TABLES")

        return sorted(
            next(iter(row.values()))
            for row in cur.fetchall()
        )


def _index_report(conn):
    """Read-only: {table: {index_name: (columns...)}} for the tables the search uses, plus what is missing."""

    found = {}
    missing = []

    with conn.cursor() as cur:
        for table_name, wanted in REQUIRED_INDEXES.items():
            cur.execute(f"SHOW INDEX FROM {table_name}")
            by_name = {}
            for row in sorted(cur.fetchall(), key=lambda r: (r["Key_name"], r["Seq_in_index"])):
                by_name.setdefault(row["Key_name"], []).append(row["Column_name"])
            found[table_name] = {name: list(cols) for name, cols in by_name.items()}
            for name, columns in wanted.items():
                if tuple(by_name.get(name, ())) != columns:
                    missing.append(f"{table_name}.{name}({', '.join(columns)})")

    return found, sorted(missing)


# ---------------------------------------------------------
# 8. LAMBDA HANDLER
# ---------------------------------------------------------

@instrument("rds_check")
def handler(event, context):

    cid = (event or {}).get("correlationId")

    c = None
    conn = None

    try:

        # Load RDS configuration.
        c = config()

        # Establish RDS connection.
        conn = dbconn(c)

        # Create tables and insert reference data.
        _create_tables(conn)

        # Verify all created tables.
        tables = _list_tables(conn)

        missing = sorted(
            REQUIRED_TABLES - {
                table.lower()
                for table in tables
            }
        )

        if missing:
            raise RuntimeError(
                f"Missing tables after setup: {missing}"
            )

        indexes, missing_indexes = _index_report(conn)

        emit(
            "info",
            "rds_connection_and_schema_setup_succeeded",
            cid,
            tables=tables,
            missingIndexes=missing_indexes
        )

        return {
            "statusCode": 200,
            "body": {
                "connected": True,
                "schema_created": True,
                "tables": tables,
                "missing_tables": [],
                "indexes": indexes,
                "missing_indexes": missing_indexes
            }
        }

    except Exception as exc:

        if conn:
            try:
                conn.rollback()
            except Exception:
                pass

        probe = {}

        if c and c.get("dbhost") and c.get("dbport"):

            probe = _tcp_probe(
                c["dbhost"],
                c["dbport"]
            )

        emit(
            "error",
            "rds_connection_or_schema_setup_failed",
            cid,
            error=repr(exc),
            probe=probe
        )

        return {
            "statusCode": 500,
            "body": {
                "connected": False,
                "schema_created": False,
                "error": repr(exc),
                "probe": probe
            }
        }

    finally:

        if conn:
            conn.close()
