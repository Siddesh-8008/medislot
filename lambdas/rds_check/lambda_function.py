
import socket
import time

from common import config, dbconn, emit


# All eight tables from schema.sql.
# Tables are created in foreign-key dependency order.
CREATE_TABLES = [
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
        KEY ix_hospitals_active (is_active)
    ) ENGINE=InnoDB
    """,
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
        KEY ix_doctors_specialization_id (specialization_id),
        KEY ix_doctors_hospital_id (hospital_id),
        KEY ix_doctors_name (last_name, first_name),
        CONSTRAINT fk_doctors_user FOREIGN KEY (doctor_id)
            REFERENCES users(user_id) ON DELETE CASCADE,
        CONSTRAINT fk_doctors_hospital FOREIGN KEY (hospital_id)
            REFERENCES hospitals(hospital_id),
        CONSTRAINT fk_doctors_specialization FOREIGN KEY (specialization_id)
            REFERENCES specializations(specialization_id)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS consultation_types (
        consultation_type_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        doctor_id BIGINT UNSIGNED NOT NULL,
        name VARCHAR(100) NOT NULL,
        duration_minutes SMALLINT UNSIGNED NOT NULL,
        fee DECIMAL(10,2) NOT NULL,
        capacity SMALLINT UNSIGNED NOT NULL DEFAULT 1,
        is_active TINYINT(1) NOT NULL DEFAULT 1,
        created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (consultation_type_id),
        UNIQUE KEY uq_consultation_doctor_name (doctor_id, name),
        KEY ix_consultation_doctor_active (doctor_id, is_active),
        CONSTRAINT fk_consultation_doctor FOREIGN KEY (doctor_id)
            REFERENCES doctors(doctor_id)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS availability_patterns (
        pattern_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        doctor_id BIGINT UNSIGNED NOT NULL,
        consultation_type_id BIGINT UNSIGNED NOT NULL,
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
        CONSTRAINT fk_availability_doctor FOREIGN KEY (doctor_id)
            REFERENCES doctors(doctor_id),
        CONSTRAINT fk_availability_consultation
            FOREIGN KEY (consultation_type_id)
            REFERENCES consultation_types(consultation_type_id),
        CONSTRAINT chk_availability_day
            CHECK (day_of_week BETWEEN 1 AND 7),
        CONSTRAINT chk_availability_time CHECK (end_time > start_time)
    ) ENGINE=InnoDB
    """,
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
        KEY ix_blackout_doctor_date (doctor_id, blackout_date),
        CONSTRAINT fk_blackout_doctor FOREIGN KEY (doctor_id)
            REFERENCES doctors(doctor_id)
    ) ENGINE=InnoDB
    """,
]


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
    (8, "NIMS Hyderabad", "Punjagutta"),
    (9, "Osmania General Hospital", "Afzal Gunj"),
    (10, "Gandhi Hospital", "Musheerabad, Secunderabad"),
]


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


EXPECTED_TABLES = [
    "users",
    "patients",
    "hospitals",
    "specializations",
    "doctors",
    "consultation_types",
    "availability_patterns",
    "blackout_dates",
]


def _tcp_probe(host, port, timeout=3):
    """Diagnose DNS and TCP connectivity problems."""
    result = {"host": host, "port": port}

    try:
        result["resolved_ips"] = sorted({
            item[4][0]
            for item in socket.getaddrinfo(
                host, port, socket.AF_INET, socket.SOCK_STREAM
            )
        })
    except Exception as exc:
        result["dns_error"] = repr(exc)
        return result

    start = time.time()

    try:
        with socket.create_connection((host, port), timeout=timeout):
            result["tcp_connect"] = "ok"
    except Exception as exc:
        result["tcp_connect"] = f"failed: {exc!r}"

    result["tcp_ms"] = int((time.time() - start) * 1000)
    return result


def _column_exists(cur, table_name, column_name):
    """Check whether a column exists in the currently selected database."""
    cur.execute(
        """
        SELECT COUNT(*) AS column_count
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = %s
          AND column_name = %s
        """,
        (table_name, column_name),
    )
    return cur.fetchone()["column_count"] > 0


def _ensure_legacy_columns(cur):
    """
    Bring forward tables created by the previous Lambda version.

    CREATE TABLE IF NOT EXISTS does not alter existing tables.
    Only add columns missing from the known earlier schema.
    """
    migrations = [
        (
            "doctors",
            "photo_key",
            "ALTER TABLE doctors ADD COLUMN photo_key VARCHAR(1024) NULL",
        ),
        (
            "hospitals",
            "created_at",
            """
            ALTER TABLE hospitals
            ADD COLUMN created_at DATETIME(6)
            NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            """,
        ),
        (
            "hospitals",
            "updated_at",
            """
            ALTER TABLE hospitals
            ADD COLUMN updated_at DATETIME(6)
            NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6)
            """,
        ),
        (
            "specializations",
            "created_at",
            """
            ALTER TABLE specializations
            ADD COLUMN created_at DATETIME(6)
            NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            """,
        ),
        (
            "specializations",
            "updated_at",
            """
            ALTER TABLE specializations
            ADD COLUMN updated_at DATETIME(6)
            NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6)
            """,
        ),
    ]

    for table_name, column_name, alter_sql in migrations:
        if not _column_exists(cur, table_name, column_name):
            cur.execute(alter_sql)


def _create_tables(conn):
    """Create all schema tables and seed reference data without duplicates."""
    with conn.cursor() as cur:
        for statement in CREATE_TABLES:
            cur.execute(statement)

        # Migrate columns omitted by the older version of this Lambda.
        _ensure_legacy_columns(cur)

        # Seed hospitals. Preserve the existing phone field on duplicate IDs.
        cur.executemany(
            """
            INSERT INTO hospitals
                (hospital_id, hospital_name, address, city, state, is_active)
            VALUES (%s, %s, %s, 'Hyderabad', 'Telangana', 1)
            ON DUPLICATE KEY UPDATE
                hospital_name = VALUES(hospital_name),
                address = VALUES(address),
                city = VALUES(city),
                state = VALUES(state),
                is_active = VALUES(is_active)
            """,
            HOSPITAL_SEED,
        )

        # Seed specializations.
        cur.executemany(
            """
            INSERT INTO specializations
                (specialization_id, specialization_name, is_active)
            VALUES (%s, %s, 1)
            ON DUPLICATE KEY UPDATE
                specialization_name = VALUES(specialization_name),
                is_active = VALUES(is_active)
            """,
            [
                (index, name)
                for index, name in enumerate(
                    SPECIALIZATION_SEED, start=1
                )
            ],
        )

        # Verify that all expected tables exist in the selected database.
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
              AND table_type = 'BASE TABLE'
            """
        )
        existing_tables = {
            row["table_name"] for row in cur.fetchall()
        }

        missing_tables = sorted(
            set(EXPECTED_TABLES) - existing_tables
        )

        if missing_tables:
            raise RuntimeError(
                f"Schema tables missing after creation: {missing_tables}"
            )

    conn.commit()


def handler(event, context):
    cid = (event or {}).get("correlationId")

    c = None
    conn = None

    try:
        # Read DB settings and credentials from the existing SSM configuration.
        c = config()

        # Connect to RDS.
        conn = dbconn(c)

        # Create missing tables, migrate known legacy columns and seed lookups.
        _create_tables(conn)

        # Verify a query works and list the actual tables.
        with conn.cursor() as cur:
            cur.execute("SELECT 1 AS connected")
            row = cur.fetchone()

            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """
            )
            existing_tables = [
                item["table_name"] for item in cur.fetchall()
            ]

        connected = bool(row and row["connected"] == 1)
        missing_tables = sorted(
            set(EXPECTED_TABLES) - set(existing_tables)
        )

        if not connected:
            raise RuntimeError("RDS connection verification failed")

        if missing_tables:
            raise RuntimeError(
                f"Missing schema tables: {missing_tables}"
            )

        emit(
            "info",
            "rds_connection_and_schema_setup_succeeded",
            cid,
            tables=EXPECTED_TABLES,
        )

        return {
            "statusCode": 200,
            "body": {
                "connected": True,
                "schema_created": True,
                "database": "medislot",
                "expected_table_count": len(EXPECTED_TABLES),
                "tables": EXPECTED_TABLES,
                "existing_tables": existing_tables,
                "missing_tables": [],
            },
        }

    except Exception as exc:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass

        probe = {}

        if c and c.get("dbhost") and c.get("dbport"):
            probe = _tcp_probe(c["dbhost"], c["dbport"])

        emit(
            "error",
            "rds_connection_or_schema_setup_failed",
            cid,
            error=repr(exc),
            probe=probe,
        )

        return {
            "statusCode": 500,
            "body": {
                "connected": False,
                "schema_created": False,
                "error": repr(exc),
                "probe": probe,
            },
        }

    finally:
        if conn:
            conn.close()
