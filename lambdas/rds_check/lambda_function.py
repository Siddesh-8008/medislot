
import socket
import time

from common import config, dbconn, emit


# Fresh-install schema.
# The users table must be created before patients and doctors because
# those tables reference users(user_id) through foreign keys.
CREATE_TABLES = [
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        email VARCHAR(254) NOT NULL,
        password_hash VARCHAR(255) NOT NULL,
        role ENUM('PATIENT', 'DOCTOR') NOT NULL,
        created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (user_id),
        UNIQUE KEY uq_users_email (email),
        KEY ix_users_role (role)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS patients (
        patient_id BIGINT UNSIGNED NOT NULL,
        first_name VARCHAR(75) NOT NULL,
        last_name VARCHAR(75) NOT NULL,
        phone VARCHAR(25) NULL,
        date_of_birth DATE NULL,
        gender VARCHAR(30) NULL,
        address VARCHAR(500) NULL,
        created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (patient_id),
        CONSTRAINT fk_patients_user
            FOREIGN KEY (patient_id)
            REFERENCES users(user_id)
            ON DELETE CASCADE
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS doctors (
        doctor_id BIGINT UNSIGNED NOT NULL,
        first_name VARCHAR(75) NOT NULL,
        last_name VARCHAR(75) NOT NULL,
        phone VARCHAR(25) NULL,
        specialization VARCHAR(120) NOT NULL,
        license_number VARCHAR(32) NOT NULL,
        years_experience SMALLINT UNSIGNED NULL,
        clinic_address VARCHAR(500) NULL,
        qualifications TEXT NULL,
        languages_spoken JSON NULL,
        photo_key VARCHAR(1024) NULL,
        timezone VARCHAR(64) NOT NULL DEFAULT 'Asia/Kolkata',
        created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ON UPDATE CURRENT_TIMESTAMP(6),
        PRIMARY KEY (doctor_id),
        UNIQUE KEY uq_doctors_license_number (license_number),
        KEY ix_doctors_specialization (specialization),
        KEY ix_doctors_name (last_name, first_name),
        CONSTRAINT fk_doctors_user
            FOREIGN KEY (doctor_id)
            REFERENCES users(user_id)
            ON DELETE CASCADE
    ) ENGINE=InnoDB
    """
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


def _create_tables(conn):
    """Create missing MediSlot tables without deleting existing data."""
    with conn.cursor() as cur:
        for statement in CREATE_TABLES:
            cur.execute(statement)

    # Persist any pending transaction changes.
    conn.commit()


def handler(event, context):
    cid = (event or {}).get("correlationId")

    c = None
    conn = None

    try:
        # Read DB settings and credentials from the existing SSM configuration.
        c = config()

        # Connect to the RDS database.
        conn = dbconn(c)

        # Create tables automatically during the deployment smoke test.
        _create_tables(conn)

        # Verify that SQL queries work after table creation.
        with conn.cursor() as cur:
            cur.execute("SELECT 1 AS connected")
            row = cur.fetchone()

        connected = bool(row and row["connected"] == 1)

        if not connected:
            raise RuntimeError("RDS connection verification failed")

        emit(
            "info",
            "rds_connection_and_schema_setup_succeeded",
            cid,
        )

        return {
            "statusCode": 200,
            "body": {
                "connected": True,
                "schema_created": True,
                "tables": ["users", "patients", "doctors"],
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
