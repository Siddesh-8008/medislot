
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
        hospital_id INT UNSIGNED NOT NULL PRIMARY KEY,
        hospital_name VARCHAR(180) NOT NULL,
        address VARCHAR(500) NOT NULL,
        city VARCHAR(100) NOT NULL DEFAULT 'Hyderabad',
        state VARCHAR(100) NOT NULL DEFAULT 'Telangana',
        phone VARCHAR(25) NULL,
        is_active TINYINT(1) NOT NULL DEFAULT 1,
        UNIQUE KEY uq_hospitals_name (hospital_name),
        KEY ix_hospitals_active (is_active)
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS specializations (
        specialization_id INT UNSIGNED NOT NULL PRIMARY KEY,
        specialization_name VARCHAR(120) NOT NULL,
        is_active TINYINT(1) NOT NULL DEFAULT 1,
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
    """
]

HOSPITAL_SEED = [
    (1, "Apollo Hospitals Jubilee Hills", "Road No. 72, Film Nagar, Jubilee Hills"),
    (2, "Yashoda Hospitals Somajiguda", "Raj Bhavan Road, Somajiguda"),
    (3, "KIMS Hospitals Secunderabad", "Minister Road, Secunderabad"),
    (4, "CARE Hospitals Banjara Hills", "Road No. 1, Banjara Hills"),
    (5, "AIG Hospitals Gachibowli", "Mindspace Road, Gachibowli"),
    (6, "Continental Hospitals", "Financial District, Nanakramguda"),
    (7, "Rainbow Children's Hospital Banjara Hills", "Road No. 10, Banjara Hills"),
    (8, "NIMS Hyderabad", "Punjagutta"),
    (9, "Osmania General Hospital", "Afzal Gunj"),
    (10, "Gandhi Hospital", "Musheerabad, Secunderabad"),
]

SPECIALIZATION_SEED = [
    "Cardiology", "Dermatology", "Endocrinology", "Family Medicine",
    "Gastroenterology", "General Medicine", "General Surgery", "Geriatrics",
    "Gynecology", "Hematology", "Nephrology", "Neurology", "Neurosurgery",
    "Oncology", "Ophthalmology", "Orthopedics", "Otolaryngology (ENT)",
    "Pediatrics", "Psychiatry", "Pulmonology", "Radiology", "Rheumatology",
    "Urology", "Anesthesiology", "Emergency Medicine", "Infectious Disease",
    "Physical Medicine and Rehabilitation", "Plastic Surgery", "Vascular Surgery",
    "Obstetrics",
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
    """Create missing tables and idempotently seed hospital/specialization lookup data."""
    with conn.cursor() as cur:
        for statement in CREATE_TABLES:
            cur.execute(statement)

        cur.executemany(
            """INSERT INTO hospitals
               (hospital_id, hospital_name, address, city, state, is_active)
               VALUES (%s, %s, %s, 'Hyderabad', 'Telangana', 1)
               ON DUPLICATE KEY UPDATE
                 hospital_name=VALUES(hospital_name), address=VALUES(address),
                 city=VALUES(city), state=VALUES(state), is_active=VALUES(is_active)""",
            HOSPITAL_SEED,
        )
        cur.executemany(
            """INSERT INTO specializations
               (specialization_id, specialization_name, is_active)
               VALUES (%s, %s, 1)
               ON DUPLICATE KEY UPDATE
                 specialization_name=VALUES(specialization_name), is_active=VALUES(is_active)""",
            [(index, name) for index, name in enumerate(SPECIALIZATION_SEED, start=1)],
        )

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
                "tables": ["users", "patients", "doctors", "hospitals", "specializations"],
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
