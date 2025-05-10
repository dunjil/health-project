-- CREATE TABLE "healthcare"."diagnoses" ( 
--   "id" SERIAL,
--   "icd_code" VARCHAR(20) NOT NULL,
--   "description" TEXT NULL,
--   "category" VARCHAR(100) NULL,
--   "is_chronic" BOOLEAN NULL DEFAULT false ,
--   "last_updated" TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ,
--   CONSTRAINT "diagnoses_pkey" PRIMARY KEY ("id")
-- );




-- CREATE TABLE "healthcare"."encounters" ( 
--   "id" SERIAL,
--   "patient_id" INTEGER NULL,
--   "provider_id" INTEGER NULL,
--   "facility_id" INTEGER NULL,
--   "primary_diagnosis_id" INTEGER NULL,
--   "encounter_type" VARCHAR(50) NOT NULL,
--   "admission_date" TIMESTAMP NOT NULL,
--   "discharge_date" TIMESTAMP NULL,
--   "total_charges" NUMERIC NULL,
--   "total_payments" NUMERIC NULL,
--   "insurance_type" VARCHAR(50) NULL,
--   "last_updated" TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ,
--   CONSTRAINT "encounters_pkey" PRIMARY KEY ("id"),
--   CONSTRAINT "encounters_patient_id_fkey" FOREIGN KEY ("patient_id") REFERENCES "healthcare"."patients" ("id") ON DELETE NO ACTION ON UPDATE NO ACTION,
--   CONSTRAINT "encounters_provider_id_fkey" FOREIGN KEY ("provider_id") REFERENCES "healthcare"."providers" ("id") ON DELETE NO ACTION ON UPDATE NO ACTION,
--   CONSTRAINT "encounters_facility_id_fkey" FOREIGN KEY ("facility_id") REFERENCES "healthcare"."facilities" ("id") ON DELETE NO ACTION ON UPDATE NO ACTION,
--   CONSTRAINT "encounters_primary_diagnosis_id_fkey" FOREIGN KEY ("primary_diagnosis_id") REFERENCES "healthcare"."diagnoses" ("id") ON DELETE NO ACTION ON UPDATE NO ACTION
-- );
-- CREATE INDEX "idx_encounters_patient_id" 
-- ON "healthcare"."encounters" (
--   "patient_id" ASC
-- );
-- CREATE INDEX "idx_encounters_admission_date" 
-- ON "healthcare"."encounters" (
--   "admission_date" ASC
-- );



-- CREATE TABLE "healthcare"."facilities" ( 
--   "id" SERIAL,
--   "facility_name" VARCHAR(200) NOT NULL,
--   "facility_type" VARCHAR(100) NULL,
--   "address" VARCHAR(200) NULL,
--   "city" VARCHAR(100) NULL,
--   "state" VARCHAR(50) NULL,
--   "zip_code" VARCHAR(20) NULL,
--   "phone_number" VARCHAR(20) NULL,
--   "bed_count" INTEGER NULL,
--   "last_updated" TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ,
--   CONSTRAINT "facilities_pkey" PRIMARY KEY ("id")
-- );

-- CREATE TABLE "healthcare"."patients" ( 
--   "id" SERIAL,
--   "medical_record_number" VARCHAR(50) NOT NULL,
--   "first_name" VARCHAR(100) NOT NULL,
--   "last_name" VARCHAR(100) NOT NULL,
--   "date_of_birth" DATE NOT NULL,
--   "gender" VARCHAR(20) NULL,
--   "address_line1" VARCHAR(200) NULL,
--   "city" VARCHAR(100) NULL,
--   "state" VARCHAR(50) NULL,
--   "zip_code" VARCHAR(20) NULL,
--   "primary_phone" VARCHAR(20) NULL,
--   "last_updated" TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ,
--   CONSTRAINT "patients_pkey" PRIMARY KEY ("id"),
--   CONSTRAINT "patients_medical_record_number_key" UNIQUE ("medical_record_number")
-- );
-- CREATE INDEX "idx_patients_last_updated" 
-- ON "healthcare"."patients" (
--   "last_updated" ASC
-- );


-- CREATE TABLE "healthcare"."providers" ( 
--   "id" SERIAL,
--   "npi_number" VARCHAR(20) NOT NULL,
--   "first_name" VARCHAR(100) NOT NULL,
--   "last_name" VARCHAR(100) NOT NULL,
--   "specialty" VARCHAR(100) NULL,
--   "department" VARCHAR(100) NULL,
--   "last_updated" TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ,
--   CONSTRAINT "providers_pkey" PRIMARY KEY ("id"),
--   CONSTRAINT "providers_npi_number_key" UNIQUE ("npi_number")
-- );
-- CREATE INDEX "idx_providers_last_updated" 
-- ON "healthcare"."providers" (
--   "last_updated" ASC
-- );
