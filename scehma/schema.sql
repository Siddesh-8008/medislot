-- MediSlot database schema - the ONE SQL file for every table, index and seed row.
-- Milestone 3: identity, doctor profile and reference data. Milestone 4: search indexes (marked "M4 search index").
-- Milestone 5: patient_dependents (family members), appointments and refund_ledger (bottom of the table section).
-- Run this file once against an EMPTY medislot database. To rebuild, drop the old tables first in child-to-parent
-- order: refund_ledger, appointments, patient_dependents, blackout_dates, availability_patterns,
-- doctor_consultation_types, consultation_types, doctors, specializations, hospitals, patients, users.
-- Every statement is CREATE TABLE IF NOT EXISTS / idempotent INSERT, so re-running it on a complete database is harmless.
-- Never store plaintext passwords.

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
) ENGINE=InnoDB;

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
) ENGINE=InnoDB;

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
    -- M4 search index: city filter, and the distinct-city list for the search form (covering).
    KEY ix_hospitals_city (city, is_active)
) ENGINE=InnoDB;

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
) ENGINE=InnoDB;

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
    -- M4 search index: specialization filter, and specialization + hospital together.
    -- Its leading column also serves fk_doctors_specialization, so a separate (specialization_id) index is not needed.
    KEY ix_doctors_spec_hospital (specialization_id, hospital_id),
    -- Hospital filter, and city filter (city -> hospital ids in hospitals -> doctors here).
    KEY ix_doctors_hospital_id (hospital_id),
    -- Last-name prefix search, and the default ORDER BY last_name, first_name, doctor_id (no filesort).
    KEY ix_doctors_name (last_name, first_name),
    -- M4 search index: first-name prefix search.
    KEY ix_doctors_first_name (first_name),
    CONSTRAINT fk_doctors_user FOREIGN KEY (doctor_id)
        REFERENCES users(user_id) ON DELETE CASCADE,
    CONSTRAINT fk_doctors_hospital FOREIGN KEY (hospital_id)
        REFERENCES hospitals(hospital_id),
    CONSTRAINT fk_doctors_specialization FOREIGN KEY (specialization_id)
        REFERENCES specializations(specialization_id)
) ENGINE=InnoDB;


-- Master list of the standard consultation types. Not owned by any doctor: the four rows are seeded
-- below, and a doctor *chooses* from this table (see doctor_consultation_types).
-- Duration is fixed by the type; capacity may range between min_capacity and max_capacity.
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
    CONSTRAINT chk_consultation_types_capacity CHECK (min_capacity >= 1 AND max_capacity >= min_capacity)
) ENGINE=InnoDB;

-- The consultation types a doctor actually offers: one row per (doctor, master type) with the
-- doctor's own fee and capacity. Name and duration come from consultation_types.
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
    UNIQUE KEY uq_doctor_consultation (doctor_id, consultation_type_id),
    -- M4 search index: covering index for "starting fee" and "number of types" per doctor (MIN(fee), COUNT(*))
    -- and for the "only doctors taking bookings" filter, without reading table rows.
    KEY ix_doctor_consultation_active (doctor_id, is_active, fee),
    KEY ix_doctor_consultation_type (consultation_type_id),
    CONSTRAINT fk_doctor_consultation_doctor FOREIGN KEY (doctor_id)
        REFERENCES doctors(doctor_id),
    CONSTRAINT fk_doctor_consultation_type FOREIGN KEY (consultation_type_id)
        REFERENCES consultation_types(consultation_type_id),
    CONSTRAINT chk_doctor_consultation_fee CHECK (fee >= 0)
) ENGINE=InnoDB;

-- A weekly pattern is always for a consultation type the doctor has chosen: the composite foreign key
-- (doctor_id, consultation_type_id) points at doctor_consultation_types.
CREATE TABLE IF NOT EXISTS availability_patterns (
    pattern_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    doctor_id BIGINT UNSIGNED NOT NULL,
    consultation_type_id INT UNSIGNED NOT NULL,
    day_of_week TINYINT UNSIGNED NOT NULL COMMENT 'ISO weekday: 1 Monday through 7 Sunday',
    start_time TIME NOT NULL,
    end_time TIME NOT NULL,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (pattern_id),
    KEY ix_availability_doctor_day_active (doctor_id, day_of_week, is_active),
    KEY ix_availability_consultation (consultation_type_id),
    CONSTRAINT fk_availability_doctor FOREIGN KEY (doctor_id)
        REFERENCES doctors(doctor_id),
    CONSTRAINT fk_availability_doctor_consultation FOREIGN KEY (doctor_id, consultation_type_id)
        REFERENCES doctor_consultation_types(doctor_id, consultation_type_id),
    CONSTRAINT chk_availability_day CHECK (day_of_week BETWEEN 1 AND 7),
    CONSTRAINT chk_availability_time CHECK (end_time > start_time)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS blackout_dates (
    blackout_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    doctor_id BIGINT UNSIGNED NOT NULL,
    blackout_date DATE NOT NULL,
    reason VARCHAR(255) NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (blackout_id),
    UNIQUE KEY uq_blackout_doctor_date (doctor_id, blackout_date),
    KEY ix_blackout_doctor_date (doctor_id, blackout_date),
    CONSTRAINT fk_blackout_doctor FOREIGN KEY (doctor_id)
        REFERENCES doctors(doctor_id)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------------------------------------------
-- Milestone 5: family members, bookings and refunds. RDS is the system of record for appointments; the DynamoDB
-- Slots table only holds the live seat counters (booked_count / held_count / version) used for optimistic concurrency.
-- Foreign keys are RESTRICT on purpose: closing an account never deletes appointment or refund history.
-- ---------------------------------------------------------------------------------------------------------

-- People a patient books for from their own account (mother, child, ...). Removing one is a soft delete
-- (is_active = 0) so old appointments keep pointing at the person they were for.
CREATE TABLE IF NOT EXISTS patient_dependents (
    dependent_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    patient_id BIGINT UNSIGNED NOT NULL COMMENT 'The account holder who manages and books for this person',
    first_name VARCHAR(75) NOT NULL,
    last_name VARCHAR(75) NOT NULL,
    relation VARCHAR(20) NOT NULL,
    date_of_birth DATE NOT NULL,
    gender VARCHAR(30) NULL,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (dependent_id),
    -- "My family members" list and the per-account limit. Also serves fk_dependents_patient.
    KEY ix_dependents_patient_active (patient_id, is_active),
    CONSTRAINT fk_dependents_patient FOREIGN KEY (patient_id)
        REFERENCES patients(patient_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT chk_dependents_relation CHECK (relation IN ('Mother', 'Father', 'Spouse', 'Child', 'Sibling', 'Grandparent', 'Grandchild', 'Other'))
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS appointments (
    appointment_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    patient_id BIGINT UNSIGNED NOT NULL,
    doctor_id BIGINT UNSIGNED NOT NULL,
    consultation_type_id INT UNSIGNED NOT NULL,
    slot_id VARCHAR(100) NOT NULL COMMENT 'DynamoDB Slots.slot_id; not a SQL foreign key',
    start_at DATETIME(6) NOT NULL COMMENT 'UTC instant',
    end_at DATETIME(6) NOT NULL COMMENT 'UTC instant',
    fee_snapshot DECIMAL(10,2) NOT NULL COMMENT 'Fee at booking time; later price changes never alter it',
    status ENUM('CONFIRMED','COMPLETED','CANCELLED_BY_PATIENT','CANCELLED_BY_DOCTOR','NO_SHOW') NOT NULL DEFAULT 'CONFIRMED',
    reschedule_count TINYINT UNSIGNED NOT NULL DEFAULT 0,
    dependent_id BIGINT UNSIGNED NULL COMMENT 'NULL = for the account holder; otherwise the family member it is for',
    idempotency_key VARCHAR(200) NULL COMMENT 'Scoped client key BOOK#<patientId>#<key>; DB-level duplicate backstop',
    -- One CONFIRMED booking per PERSON per slot (account holder = dependent 0), enforced by the database
    -- (NULL for every other status, and a UNIQUE index allows any number of NULLs).
    active_patient_slot VARCHAR(170) GENERATED ALWAYS AS
        (CASE WHEN status = 'CONFIRMED' THEN CONCAT(patient_id, '#', COALESCE(dependent_id, 0), '#', slot_id) ELSE NULL END) STORED,
    cancelled_at DATETIME(6) NULL,
    cancellation_note VARCHAR(255) NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (appointment_id),
    UNIQUE KEY uq_appointments_idempotency_key (idempotency_key),
    UNIQUE KEY uq_appointments_active_patient_slot (active_patient_slot),
    -- Patient's list (upcoming / past) and the "max active appointments" + overlap checks. Also serves fk_appointments_patient.
    KEY ix_appointments_patient_start (patient_id, start_at),
    -- Doctor's list and the doctor schedule day view (doctor_id = ? AND start_at in a range).
    KEY ix_appointments_doctor_start (doctor_id, start_at),
    -- Doctor slot cancellation: every CONFIRMED appointment in one slot.
    KEY ix_appointments_slot_status (slot_id, status),
    -- "At most N bookings with the same doctor on one day" check.
    KEY ix_appointments_patient_doctor_status_start (patient_id, doctor_id, status, start_at),
    -- Serves the composite foreign key below (InnoDB needs an index whose leading columns match it).
    KEY ix_appointments_doctor_type (doctor_id, consultation_type_id),
    -- Serves fk_appointments_dependent ("does this family member still have upcoming appointments?").
    KEY ix_appointments_dependent (dependent_id),
    CONSTRAINT fk_appointments_patient FOREIGN KEY (patient_id)
        REFERENCES patients(patient_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_appointments_doctor FOREIGN KEY (doctor_id)
        REFERENCES doctors(doctor_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_appointments_doctor_consultation FOREIGN KEY (doctor_id, consultation_type_id)
        REFERENCES doctor_consultation_types(doctor_id, consultation_type_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_appointments_dependent FOREIGN KEY (dependent_id)
        REFERENCES patient_dependents(dependent_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT chk_appointments_time CHECK (end_at > start_at),
    CONSTRAINT chk_appointments_fee CHECK (fee_snapshot >= 0),
    CONSTRAINT chk_appointments_reschedules CHECK (reschedule_count <= 2)
) ENGINE=InnoDB;

-- One final ledger row per cancelled appointment (UNIQUE appointment_id): a second cancel can never pay twice.
CREATE TABLE IF NOT EXISTS refund_ledger (
    refund_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    appointment_id BIGINT UNSIGNED NOT NULL,
    fee_amount DECIMAL(10,2) NOT NULL COMMENT 'appointments.fee_snapshot the refund was calculated from',
    refund_amount DECIMAL(10,2) NOT NULL,
    deduction_amount DECIMAL(10,2) NOT NULL,
    refund_percentage DECIMAL(5,2) NOT NULL,
    refund_tier VARCHAR(30) NOT NULL COMMENT 'Tier name from the SSM policy, or DOCTOR_CANCELLED',
    cancellation_reason VARCHAR(30) NOT NULL,
    refund_policy JSON NULL COMMENT 'Snapshot of the tier policy used, for audit',
    calculated_at DATETIME(6) NOT NULL COMMENT 'UTC instant the tier was decided',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (refund_id),
    UNIQUE KEY uq_refund_ledger_appointment (appointment_id),
    CONSTRAINT fk_refund_ledger_appointment FOREIGN KEY (appointment_id)
        REFERENCES appointments(appointment_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT chk_refund_amounts CHECK (refund_amount >= 0 AND deduction_amount >= 0 AND refund_amount + deduction_amount = fee_amount),
    CONSTRAINT chk_refund_percentage CHECK (refund_percentage BETWEEN 0 AND 100),
    CONSTRAINT chk_refund_reason CHECK (cancellation_reason IN ('PATIENT_CANCEL', 'DOCTOR_CANCEL', 'NO_SHOW'))
) ENGINE=InnoDB;

-- Idempotent reference data: rerunning updates names/details without creating duplicates.
INSERT INTO hospitals
    (hospital_id, hospital_name, address, city, state, phone, is_active)
VALUES
    (1, 'Apollo Hospitals Jubilee Hills', 'Road No. 72, Film Nagar, Jubilee Hills', 'Hyderabad', 'Telangana', NULL, 1),
    (2, 'Yashoda Hospitals Somajiguda', 'Raj Bhavan Road, Somajiguda', 'Hyderabad', 'Telangana', NULL, 1),
    (3, 'KIMS Hospitals Secunderabad', 'Minister Road, Secunderabad', 'Hyderabad', 'Telangana', NULL, 1),
    (4, 'CARE Hospitals Banjara Hills', 'Road No. 1, Banjara Hills', 'Hyderabad', 'Telangana', NULL, 1),
    (5, 'AIG Hospitals Gachibowli', 'Mindspace Road, Gachibowli', 'Hyderabad', 'Telangana', NULL, 1),
    (6, 'Continental Hospitals', 'Financial District, Nanakramguda', 'Hyderabad', 'Telangana', NULL, 1),
    (7, 'Rainbow Children''s Hospital Banjara Hills', 'Road No. 10, Banjara Hills', 'Hyderabad', 'Telangana', NULL, 1),
    (8, 'NIMS Hyderabad', 'Punjagutta', 'Hyderabad', 'Telangana', NULL, 1),
    (9, 'Osmania General Hospital', 'Afzal Gunj', 'Hyderabad', 'Telangana', NULL, 1),
    (10, 'Gandhi Hospital', 'Musheerabad, Secunderabad', 'Hyderabad', 'Telangana', NULL, 1)
ON DUPLICATE KEY UPDATE
    hospital_name = VALUES(hospital_name),
    address = VALUES(address),
    city = VALUES(city),
    state = VALUES(state),
    is_active = VALUES(is_active);

INSERT INTO specializations (specialization_id, specialization_name, is_active)
VALUES
    (1, 'Cardiology', 1),
    (2, 'Dermatology', 1),
    (3, 'Endocrinology', 1),
    (4, 'Family Medicine', 1),
    (5, 'Gastroenterology', 1),
    (6, 'General Medicine', 1),
    (7, 'General Surgery', 1),
    (8, 'Geriatrics', 1),
    (9, 'Gynecology', 1),
    (10, 'Hematology', 1),
    (11, 'Nephrology', 1),
    (12, 'Neurology', 1),
    (13, 'Neurosurgery', 1),
    (14, 'Oncology', 1),
    (15, 'Ophthalmology', 1),
    (16, 'Orthopedics', 1),
    (17, 'Otolaryngology (ENT)', 1),
    (18, 'Pediatrics', 1),
    (19, 'Psychiatry', 1),
    (20, 'Pulmonology', 1),
    (21, 'Radiology', 1),
    (22, 'Rheumatology', 1),
    (23, 'Urology', 1),
    (24, 'Anesthesiology', 1),
    (25, 'Emergency Medicine', 1),
    (26, 'Infectious Disease', 1),
    (27, 'Physical Medicine and Rehabilitation', 1),
    (28, 'Plastic Surgery', 1),
    (29, 'Vascular Surgery', 1),
    (30, 'Obstetrics', 1)
ON DUPLICATE KEY UPDATE
    specialization_name = VALUES(specialization_name),
    is_active = VALUES(is_active);

-- The four standard consultation types (idempotent).
INSERT INTO consultation_types
    (consultation_type_id, name, duration_minutes, min_capacity, max_capacity, is_active)
VALUES
    (1, 'Quick Consult',       15, 1, 1,  1),
    (2, 'Standard Consult',    30, 1, 1,  1),
    (3, 'Detailed Consult',    60, 1, 1,  1),
    (4, 'Health Camp Session', 60, 2, 10, 1)
ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    duration_minutes = VALUES(duration_minutes),
    min_capacity = VALUES(min_capacity),
    max_capacity = VALUES(max_capacity),
    is_active = VALUES(is_active);
