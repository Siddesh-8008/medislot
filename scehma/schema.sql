-- MediSlot identity, doctor profile and reference-data schema.
-- Fresh database install: creates core MediSlot tables and Milestone 3 doctor-domain tables.
-- Run this file once against an empty medislot database.
-- Never store plaintext passwords.
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
    KEY ix_hospitals_active (is_active)
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
    KEY ix_doctors_specialization_id (specialization_id),
    KEY ix_doctors_hospital_id (hospital_id),
    KEY ix_doctors_name (last_name, first_name),
    CONSTRAINT fk_doctors_user FOREIGN KEY (doctor_id)
        REFERENCES users(user_id) ON DELETE CASCADE,
    CONSTRAINT fk_doctors_hospital FOREIGN KEY (hospital_id)
        REFERENCES hospitals(hospital_id),
    CONSTRAINT fk_doctors_specialization FOREIGN KEY (specialization_id)
        REFERENCES specializations(specialization_id)
) ENGINE=InnoDB;


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
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS availability_patterns (
    pattern_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    doctor_id BIGINT UNSIGNED NOT NULL,
    consultation_type_id BIGINT UNSIGNED NOT NULL,
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
    CONSTRAINT fk_availability_consultation FOREIGN KEY (consultation_type_id)
        REFERENCES consultation_types(consultation_type_id),
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
