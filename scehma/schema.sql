-- MediSlot identity schema. Runtime applies the same additive migration for older databases.
CREATE TABLE IF NOT EXISTS users (
  user_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, email VARCHAR(254) NOT NULL,
  password_hash VARCHAR(255) NOT NULL, role ENUM('PATIENT','DOCTOR') NOT NULL,
  full_name VARCHAR(150) NOT NULL, first_name VARCHAR(75) NULL, last_name VARCHAR(75) NULL,
  phone VARCHAR(25) NULL, date_of_birth DATE NULL, gender VARCHAR(30) NULL, address VARCHAR(500) NULL,
  specialization VARCHAR(120) NULL, license_number VARCHAR(32) NULL, years_experience SMALLINT UNSIGNED NULL,
  clinic_address VARCHAR(500) NULL, is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (user_id), UNIQUE KEY uq_users_email (email), UNIQUE KEY uq_users_license_number (license_number),
  KEY ix_users_role_active (role, is_active)
) ENGINE=InnoDB;
