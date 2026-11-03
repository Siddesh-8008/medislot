-- MediSlot Milestone 4: search indexes for an EXISTING database.
--
-- Run this ONCE if your tables were created before Milestone 4 and you do not want to drop them.
-- (A fresh install from scehma/schema.sql, or tables re-created by the deploy pipeline, already has these
--  indexes and does not need this file.) No data is changed or deleted. Each statement can safely be
-- re-run only if you first check `SHOW INDEX FROM <table>`: MySQL has no "IF NOT EXISTS" for indexes.
--
-- Why each index exists is explained in MILESTONE4.md ("Index choices").
-- Order matters: the new composite index is created BEFORE the old single-column one is dropped, because the
-- foreign key fk_doctors_specialization needs an index on specialization_id at all times.

-- 1) hospitals: city filter and the distinct-city list for the search form
ALTER TABLE hospitals
    ADD INDEX ix_hospitals_city (city, is_active);

-- 2) doctors: specialization (+ hospital) filter; first-name prefix search
ALTER TABLE doctors
    ADD INDEX ix_doctors_spec_hospital (specialization_id, hospital_id),
    ADD INDEX ix_doctors_first_name (first_name);
ALTER TABLE doctors
    DROP INDEX ix_doctors_specialization_id;

-- 3) doctor_consultation_types: make the (doctor_id, is_active) index covering for fee lookups
--    (fk_doctor_consultation_doctor stays served by uq_doctor_consultation, whose first column is doctor_id)
ALTER TABLE doctor_consultation_types
    DROP INDEX ix_doctor_consultation_active,
    ADD INDEX ix_doctor_consultation_active (doctor_id, is_active, fee);

-- Check the result:
--   SHOW INDEX FROM doctors;
--   SHOW INDEX FROM hospitals;
--   SHOW INDEX FROM doctor_consultation_types;
