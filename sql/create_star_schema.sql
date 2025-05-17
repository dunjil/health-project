-- Create diagnoses table
CREATE TABLE IF NOT EXISTS `radic-healthcare.healthcare_dataset.diagnoses` (
  `id` INT64,
  `icd_code` STRING NOT NULL,
  `description` STRING,
  `category` STRING,
  `is_chronic` BOOL DEFAULT FALSE,
  `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Create encounters table
CREATE TABLE IF NOT EXISTS `radic-healthcare.healthcare_dataset.encounters` (
  `id` INT64,
  `patient_id` INT64,
  `provider_id` INT64,
  `facility_id` INT64,
  `primary_diagnosis_id` INT64,
  `encounter_type` STRING NOT NULL,
  `admission_date` TIMESTAMP NOT NULL,
  `discharge_date` TIMESTAMP,
  `total_charges` NUMERIC,
  `total_payments` NUMERIC,
  `insurance_type` STRING,
  `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Create facilities table
CREATE TABLE IF NOT EXISTS `radic-healthcare.healthcare_dataset.facilities` (
  `id` INT64,
  `facility_name` STRING NOT NULL,
  `facility_type` STRING,
  `address` STRING,
  `city` STRING,
  `state` STRING,
  `zip_code` STRING,
  `phone_number` STRING,
  `bed_count` INT64,
  `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Create patients table
CREATE TABLE IF NOT EXISTS `radic-healthcare.healthcare_dataset.patients` (
  `id` INT64,
  `medical_record_number` STRING NOT NULL,
  `first_name` STRING NOT NULL,
  `last_name` STRING NOT NULL,
  `date_of_birth` DATE NOT NULL,
  `gender` STRING,
  `address_line1` STRING,
  `city` STRING,
  `state` STRING,
  `zip_code` STRING,
  `primary_phone` STRING,
  `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Create providers table
CREATE TABLE IF NOT EXISTS `radic-healthcare.healthcare_dataset.providers` (
  `id` INT64,
  `npi_number` STRING NOT NULL,
  `first_name` STRING NOT NULL,
  `last_name` STRING NOT NULL,
  `specialty` STRING,
  `department` STRING,
  `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
