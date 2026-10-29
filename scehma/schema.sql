-- MediSlot account/profile schema and one-time migration for existing databases.
-- Run this in MySQL Workbench connected to the `medislot` database BEFORE deploying
-- the updated authentication Lambda. Take a database snapshot/backup first.
-- Passwords are stored as bcrypt hashes in password_hash, never as plaintext.
-- This script migrates legacy profile fields from users into patients/doctors,
-- verifies that every account has its corresponding profile, then removes the old
-- profile columns from users. If validation fails, the old columns are retained.

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
) ENGINE=InnoDB;

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
    CONSTRAINT fk_patients_user FOREIGN KEY (patient_id)
        REFERENCES users(user_id) ON DELETE CASCADE
) ENGINE=InnoDB;

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
    CONSTRAINT fk_doctors_user FOREIGN KEY (doctor_id)
        REFERENCES users(user_id) ON DELETE CASCADE
) ENGINE=InnoDB;

DELIMITER $$
DROP PROCEDURE IF EXISTS medislot_migrate_user_profiles$$
CREATE PROCEDURE medislot_migrate_user_profiles()
BEGIN
    DECLARE legacy_columns INT DEFAULT 0;
    DECLARE has_index INT DEFAULT 0;
    DECLARE i INT DEFAULT 1;
    DECLARE column_to_drop VARCHAR(64);
    DECLARE sql_text TEXT;

    -- Only copy data if this is the old users schema. Fresh installs have no
    -- legacy profile columns and simply create the three tables above.
    SELECT COUNT(*) INTO legacy_columns
      FROM information_schema.columns
     WHERE table_schema = DATABASE()
       AND table_name = 'users'
       AND column_name IN (
         'full_name', 'first_name', 'last_name', 'phone', 'date_of_birth',
         'gender', 'address', 'specialization', 'license_number',
         'years_experience', 'clinic_address'
       );

    IF legacy_columns = 11 THEN
        SET @medislot_sql = 'INSERT IGNORE INTO patients
          (patient_id, first_name, last_name, phone, date_of_birth, gender, address, created_at, updated_at)
          SELECT user_id,
            COALESCE(NULLIF(TRIM(first_name), ''''), NULLIF(SUBSTRING_INDEX(TRIM(full_name), '' '', 1), ''''), ''Unknown''),
            COALESCE(NULLIF(TRIM(last_name), ''''), CASE WHEN LOCATE('' '', TRIM(full_name)) > 0 THEN TRIM(SUBSTRING(TRIM(full_name), LOCATE('' '', TRIM(full_name)) + 1)) ELSE '''' END),
            phone, date_of_birth, gender, address, created_at, updated_at
          FROM users WHERE role = ''PATIENT''';
        PREPARE medislot_stmt FROM @medislot_sql;
        EXECUTE medislot_stmt;
        DEALLOCATE PREPARE medislot_stmt;

        SET @medislot_sql = 'INSERT IGNORE INTO doctors
          (doctor_id, first_name, last_name, phone, specialization, license_number, years_experience, clinic_address, created_at, updated_at)
          SELECT user_id,
            COALESCE(NULLIF(TRIM(first_name), ''''), NULLIF(SUBSTRING_INDEX(TRIM(full_name), '' '', 1), ''''), ''Unknown''),
            COALESCE(NULLIF(TRIM(last_name), ''''), CASE WHEN LOCATE('' '', TRIM(full_name)) > 0 THEN TRIM(SUBSTRING(TRIM(full_name), LOCATE('' '', TRIM(full_name)) + 1)) ELSE '''' END),
            phone, specialization, license_number, years_experience, clinic_address, created_at, updated_at
          FROM users WHERE role = ''DOCTOR''
            AND specialization IS NOT NULL AND TRIM(specialization) <> ''''
            AND license_number IS NOT NULL AND TRIM(license_number) <> ''''';
        PREPARE medislot_stmt FROM @medislot_sql;
        EXECUTE medislot_stmt;
        DEALLOCATE PREPARE medislot_stmt;
    END IF;

    -- Never remove the old data unless every account has a matching profile.
    IF EXISTS (
        SELECT 1 FROM users u LEFT JOIN patients p ON p.patient_id = u.user_id
         WHERE u.role = 'PATIENT' AND p.patient_id IS NULL LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
          SET MESSAGE_TEXT = 'Migration stopped: one or more PATIENT accounts lack a patients profile. Old users columns were retained.';
    END IF;

    IF EXISTS (
        SELECT 1 FROM users u LEFT JOIN doctors d ON d.doctor_id = u.user_id
         WHERE u.role = 'DOCTOR' AND d.doctor_id IS NULL LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
          SET MESSAGE_TEXT = 'Migration stopped: one or more DOCTOR accounts lack a doctors profile. Check specialization/license_number; old users columns were retained.';
    END IF;

    -- Remove old indexes before dropping the columns they depend on.
    SELECT COUNT(*) INTO has_index FROM information_schema.statistics
     WHERE table_schema = DATABASE() AND table_name = 'users'
       AND index_name = 'uq_users_license_number';
    IF has_index > 0 THEN
        SET @medislot_sql = 'ALTER TABLE users DROP INDEX uq_users_license_number';
        PREPARE medislot_stmt FROM @medislot_sql;
        EXECUTE medislot_stmt;
        DEALLOCATE PREPARE medislot_stmt;
    END IF;

    SELECT COUNT(*) INTO has_index FROM information_schema.statistics
     WHERE table_schema = DATABASE() AND table_name = 'users'
       AND index_name = 'ix_users_role_active';
    IF has_index > 0 THEN
        SET @medislot_sql = 'ALTER TABLE users DROP INDEX ix_users_role_active';
        PREPARE medislot_stmt FROM @medislot_sql;
        EXECUTE medislot_stmt;
        DEALLOCATE PREPARE medislot_stmt;
    END IF;

    -- Drop legacy profile columns only after profile-copy and completeness checks.
    WHILE i <= 11 DO
        CASE i
            WHEN 1 THEN SET column_to_drop = 'full_name';
            WHEN 2 THEN SET column_to_drop = 'first_name';
            WHEN 3 THEN SET column_to_drop = 'last_name';
            WHEN 4 THEN SET column_to_drop = 'phone';
            WHEN 5 THEN SET column_to_drop = 'date_of_birth';
            WHEN 6 THEN SET column_to_drop = 'gender';
            WHEN 7 THEN SET column_to_drop = 'address';
            WHEN 8 THEN SET column_to_drop = 'specialization';
            WHEN 9 THEN SET column_to_drop = 'license_number';
            WHEN 10 THEN SET column_to_drop = 'years_experience';
            WHEN 11 THEN SET column_to_drop = 'clinic_address';
        END CASE;

        SELECT COUNT(*) INTO has_index
          FROM information_schema.columns
         WHERE table_schema = DATABASE()
           AND table_name = 'users'
           AND column_name = column_to_drop;

        IF has_index > 0 THEN
            SET @medislot_sql = CONCAT('ALTER TABLE users DROP COLUMN `', column_to_drop, '`');
            PREPARE medislot_stmt FROM @medislot_sql;
            EXECUTE medislot_stmt;
            DEALLOCATE PREPARE medislot_stmt;
        END IF;
        SET i = i + 1;
    END WHILE;

    -- Remove the legacy account status column/index; the target users table
    -- intentionally contains only identity/authentication fields and timestamps.
    SELECT COUNT(*) INTO has_index FROM information_schema.columns
     WHERE table_schema = DATABASE() AND table_name = 'users' AND column_name = 'is_active';
    IF has_index > 0 THEN
        SET @medislot_sql = 'ALTER TABLE users DROP COLUMN is_active';
        PREPARE medislot_stmt FROM @medislot_sql;
        EXECUTE medislot_stmt;
        DEALLOCATE PREPARE medislot_stmt;
    END IF;
END$$
DELIMITER ;

CALL medislot_migrate_user_profiles();
DROP PROCEDURE IF EXISTS medislot_migrate_user_profiles;

-- Verification: users should contain only user_id, email, password_hash, role,
-- created_at and updated_at. Each account has a matching profile row.
DESCRIBE users;
SELECT role, COUNT(*) AS account_count FROM users GROUP BY role;
SELECT COUNT(*) AS patient_profile_count FROM patients;
SELECT COUNT(*) AS doctor_profile_count FROM doctors;
