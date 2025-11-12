-- FULL corrected silver SQL (types normalized & merges fixed)

-- --------------------------------------------------------------------------------
-- Departments (unchanged)
-- --------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `gcp-healthcare-etl-2025.silver_dataset.departments` (
    Dept_Id STRING,
    SRC_Dept_Id STRING,
    Name STRING,
    datasource STRING,
    is_quarantined BOOLEAN
);

TRUNCATE TABLE `gcp-healthcare-etl-2025.silver_dataset.departments`;

INSERT INTO `gcp-healthcare-etl-2025.silver_dataset.departments`
SELECT DISTINCT 
    CONCAT(deptid, '-', datasource) AS Dept_Id,
    deptid AS SRC_Dept_Id,
    Name,
    datasource,
    CASE 
        WHEN deptid IS NULL OR Name IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_quarantined
FROM (
    SELECT DISTINCT *, 'hosa' AS datasource FROM `gcp-healthcare-etl-2025.bronze_dataset.departments_ha`
    UNION ALL
    SELECT DISTINCT *, 'hosb' AS datasource FROM `gcp-healthcare-etl-2025.bronze_dataset.departments_hb`
);

-- --------------------------------------------------------------------------------
-- Providers (unchanged except safe cast for NPI)
-- --------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `gcp-healthcare-etl-2025.silver_dataset.providers` (
    ProviderID STRING,
    FirstName STRING,
    LastName STRING,
    Specialization STRING,
    DeptID STRING,
    NPI INT64,
    datasource STRING,
    is_quarantined BOOLEAN
);

TRUNCATE TABLE `gcp-healthcare-etl-2025.silver_dataset.providers`;

INSERT INTO `gcp-healthcare-etl-2025.silver_dataset.providers`
SELECT DISTINCT 
    ProviderID,
    FirstName,
    LastName,
    Specialization,
    DeptID,
    SAFE_CAST(NPI AS INT64) AS NPI,
    datasource,
    CASE 
        WHEN ProviderID IS NULL OR DeptID IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_quarantined
FROM (
    SELECT DISTINCT *, 'hosa' AS datasource FROM `gcp-healthcare-etl-2025.bronze_dataset.providers_ha`
    UNION ALL
    SELECT DISTINCT *, 'hosb' AS datasource FROM `gcp-healthcare-etl-2025.bronze_dataset.providers_hb`
);

-- --------------------------------------------------------------------------------
-- Patients (DOB & SRC_ModifiedDate are TIMESTAMP)
-- --------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `gcp-healthcare-etl-2025.silver_dataset.patients` (
    Patient_Key STRING,
    SRC_PatientID STRING,
    FirstName STRING,
    LastName STRING,
    MiddleName STRING,
    SSN STRING,
    PhoneNumber STRING,
    Gender STRING,
    DOB TIMESTAMP,
    Address STRING,
    SRC_ModifiedDate TIMESTAMP,
    datasource STRING,
    is_quarantined BOOL,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOL
);

-- Build normalized quality_checks for patients (cast DOB/ModifiedDate to TIMESTAMP)
CREATE OR REPLACE TABLE `gcp-healthcare-etl-2025.silver_dataset.quality_checks` AS
SELECT DISTINCT 
    CONCAT(SRC_PatientID, '-', datasource) AS Patient_Key,
    SRC_PatientID,
    FirstName,
    LastName,
    MiddleName,
    SSN,
    PhoneNumber,
    Gender,
    -- Ensure DOB is TIMESTAMP (attempt cast if necessary)
    CAST(DOB AS TIMESTAMP) AS DOB,
    Address,
    CAST(ModifiedDate AS TIMESTAMP) AS SRC_ModifiedDate,
    datasource,
    CASE 
        WHEN SRC_PatientID IS NULL OR DOB IS NULL OR FirstName IS NULL OR LOWER(FirstName) = 'null' THEN TRUE
        ELSE FALSE
    END AS is_quarantined
FROM (
    SELECT DISTINCT 
        PatientID AS SRC_PatientID,
        FirstName,
        LastName,
        MiddleName,
        SSN,
        PhoneNumber,
        Gender,
        DOB,
        Address,
        ModifiedDate,
        'hosa' AS datasource
    FROM `gcp-healthcare-etl-2025.bronze_dataset.patients_ha`
    
    UNION ALL

    SELECT DISTINCT 
        ID AS SRC_PatientID,
        F_Name as FirstName,
        L_Name as LastName,
        M_Name as MiddleName,
        SSN,
        PhoneNumber,
        Gender,
        DOB,
        Address,
        ModifiedDate,
        'hosb' AS datasource
    FROM `gcp-healthcare-etl-2025.bronze_dataset.patients_hb`
);

-- Apply SCD2 merge (types now align)
MERGE INTO `gcp-healthcare-etl-2025.silver_dataset.patients` AS target
USING `gcp-healthcare-etl-2025.silver_dataset.quality_checks` AS source
ON target.Patient_Key = source.Patient_Key
AND target.is_current = TRUE 
WHEN MATCHED AND (
    target.SRC_PatientID <> source.SRC_PatientID OR
    target.FirstName <> source.FirstName OR
    target.LastName <> source.LastName OR
    target.MiddleName <> source.MiddleName OR
    target.SSN <> source.SSN OR
    target.PhoneNumber <> source.PhoneNumber OR
    target.Gender <> source.Gender OR
    target.DOB <> source.DOB OR
    target.Address <> source.Address OR
    target.SRC_ModifiedDate <> source.SRC_ModifiedDate OR
    target.datasource <> source.datasource OR
    target.is_quarantined <> source.is_quarantined
)
THEN UPDATE SET 
    target.is_current = FALSE,
    target.modified_date = CURRENT_TIMESTAMP()
WHEN NOT MATCHED 
THEN INSERT (
    Patient_Key,
    SRC_PatientID,
    FirstName,
    LastName,
    MiddleName,
    SSN,
    PhoneNumber,
    Gender,
    DOB,
    Address,
    SRC_ModifiedDate,
    datasource,
    is_quarantined,
    inserted_date,
    modified_date,
    is_current
)
VALUES (
    source.Patient_Key,
    source.SRC_PatientID,
    source.FirstName,
    source.LastName,
    source.MiddleName,
    source.SSN,
    source.PhoneNumber,
    source.Gender,
    source.DOB,
    source.Address,
    source.SRC_ModifiedDate,
    source.datasource,
    source.is_quarantined,
    CURRENT_TIMESTAMP(),  
    CURRENT_TIMESTAMP(),  
    TRUE 
);

DROP TABLE IF EXISTS `gcp-healthcare-etl-2025.silver_dataset.quality_checks`;

-- --------------------------------------------------------------------------------
-- Transactions
--   -> changed VisitDate/ServiceDate/PaidDate and SRC_InsertDate/SRC_ModifiedDate to TIMESTAMP
-- --------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `gcp-healthcare-etl-2025.silver_dataset.transactions` (
    Transaction_Key STRING,
    SRC_TransactionID STRING,
    EncounterID STRING,
    PatientID STRING,
    ProviderID STRING,
    DeptID STRING,
    VisitDate TIMESTAMP,
    ServiceDate TIMESTAMP,
    PaidDate TIMESTAMP,
    VisitType STRING,
    Amount FLOAT64,
    AmountType STRING,
    PaidAmount FLOAT64,
    ClaimID STRING,
    PayorID STRING,
    ProcedureCode INT64,
    ICDCode STRING,
    LineOfBusiness STRING,
    MedicaidID STRING,
    MedicareID STRING,
    SRC_InsertDate TIMESTAMP,
    SRC_ModifiedDate TIMESTAMP,
    datasource STRING,
    is_quarantined BOOL,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOL
);

-- Create normalized quality_checks for transactions (cast date fields to TIMESTAMP, amounts to FLOAT)
CREATE OR REPLACE TABLE `gcp-healthcare-etl-2025.silver_dataset.quality_checks` AS
SELECT DISTINCT 
    CONCAT(TransactionID, '-', datasource) AS Transaction_Key,
    TransactionID AS SRC_TransactionID,
    EncounterID,
    PatientID,
    ProviderID,
    DeptID,
    CAST(VisitDate AS TIMESTAMP) AS VisitDate,
    CAST(ServiceDate AS TIMESTAMP) AS ServiceDate,
    CAST(PaidDate AS TIMESTAMP) AS PaidDate,
    VisitType,
    SAFE_CAST(Amount AS FLOAT64) AS Amount,
    AmountType,
    SAFE_CAST(PaidAmount AS FLOAT64) AS PaidAmount,
    ClaimID,
    PayorID,
    ProcedureCode,
    ICDCode,
    LineOfBusiness,
    MedicaidID,
    MedicareID,
    CAST(InsertDate AS TIMESTAMP) AS SRC_InsertDate,
    CAST(ModifiedDate AS TIMESTAMP) AS SRC_ModifiedDate,
    datasource,
    CASE 
        WHEN EncounterID IS NULL OR PatientID IS NULL OR TransactionID IS NULL OR VisitDate IS NULL THEN TRUE
        ELSE FALSE
    END AS is_quarantined
FROM (
    SELECT DISTINCT *, 'hosa' AS datasource FROM `gcp-healthcare-etl-2025.bronze_dataset.transactions_ha`
    UNION ALL
    SELECT DISTINCT *, 'hosb' AS datasource FROM `gcp-healthcare-etl-2025.bronze_dataset.transactions_hb`
);

MERGE INTO `gcp-healthcare-etl-2025.silver_dataset.transactions` AS target
USING `gcp-healthcare-etl-2025.silver_dataset.quality_checks` AS source
ON target.Transaction_Key = source.Transaction_Key
AND target.is_current = TRUE 
WHEN MATCHED AND (
    target.SRC_TransactionID <> source.SRC_TransactionID OR
    target.EncounterID <> source.EncounterID OR
    target.PatientID <> source.PatientID OR
    target.ProviderID <> source.ProviderID OR
    target.DeptID <> source.DeptID OR
    target.VisitDate <> source.VisitDate OR
    target.ServiceDate <> source.ServiceDate OR
    target.PaidDate <> source.PaidDate OR
    target.VisitType <> source.VisitType OR
    target.Amount <> source.Amount OR
    target.AmountType <> source.AmountType OR
    target.PaidAmount <> source.PaidAmount OR
    target.ClaimID <> source.ClaimID OR
    target.PayorID <> source.PayorID OR
    target.ProcedureCode <> source.ProcedureCode OR
    target.ICDCode <> source.ICDCode OR
    target.LineOfBusiness <> source.LineOfBusiness OR
    target.MedicaidID <> source.MedicaidID OR
    target.MedicareID <> source.MedicareID OR
    target.SRC_InsertDate <> source.SRC_InsertDate OR
    target.SRC_ModifiedDate <> source.SRC_ModifiedDate OR
    target.datasource <> source.datasource OR
    target.is_quarantined <> source.is_quarantined
)
THEN UPDATE SET 
    target.is_current = FALSE,
    target.modified_date = CURRENT_TIMESTAMP()
WHEN NOT MATCHED 
THEN INSERT (
    Transaction_Key,
    SRC_TransactionID,
    EncounterID,
    PatientID,
    ProviderID,
    DeptID,
    VisitDate,
    ServiceDate,
    PaidDate,
    VisitType,
    Amount,
    AmountType,
    PaidAmount,
    ClaimID,
    PayorID,
    ProcedureCode,
    ICDCode,
    LineOfBusiness,
    MedicaidID,
    MedicareID,
    SRC_InsertDate,
    SRC_ModifiedDate,
    datasource,
    is_quarantined,
    inserted_date,
    modified_date,
    is_current
)
VALUES (
    source.Transaction_Key,
    source.SRC_TransactionID,
    source.EncounterID,
    source.PatientID,
    source.ProviderID,
    source.DeptID,
    source.VisitDate,
    source.ServiceDate,
    source.PaidDate,
    source.VisitType,
    source.Amount,
    source.AmountType,
    source.PaidAmount,
    source.ClaimID,
    source.PayorID,
    source.ProcedureCode,
    source.ICDCode,
    source.LineOfBusiness,
    source.MedicaidID,
    source.MedicareID,
    source.SRC_InsertDate,
    source.SRC_ModifiedDate,
    source.datasource,
    source.is_quarantined,
    CURRENT_TIMESTAMP(),  
    CURRENT_TIMESTAMP(),  
    TRUE 
);

DROP TABLE IF EXISTS `gcp-healthcare-etl-2025.silver_dataset.quality_checks`;

-- --------------------------------------------------------------------------------
-- Encounters (make EncounterDate & SRC_ModifiedDate TIMESTAMP)
-- --------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `gcp-healthcare-etl-2025.silver_dataset.encounters` (
    Encounter_Key STRING,
    SRC_EncounterID STRING,
    PatientID STRING,
    ProviderID STRING,
    DepartmentID STRING,
    EncounterDate TIMESTAMP,
    EncounterType STRING,
    ProcedureCode INT64,
    SRC_ModifiedDate TIMESTAMP,
    datasource STRING,
    is_quarantined BOOL,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOL
);

CREATE OR REPLACE TABLE `gcp-healthcare-etl-2025.silver_dataset.quality_checks_encounters` AS
SELECT DISTINCT 
    CONCAT(SRC_EncounterID, '-', datasource) AS Encounter_Key,
    SRC_EncounterID,
    PatientID,
    ProviderID,
    DepartmentID,
    CAST(EncounterDate AS TIMESTAMP) AS EncounterDate,
    EncounterType,
    ProcedureCode,
    CAST(ModifiedDate AS TIMESTAMP) AS SRC_ModifiedDate,
    datasource,
    CASE 
        WHEN SRC_EncounterID IS NULL OR PatientID IS NULL OR EncounterDate IS NULL OR LOWER(EncounterType) = 'null' THEN TRUE
        ELSE FALSE
    END AS is_quarantined
FROM (
    SELECT DISTINCT 
        EncounterID AS SRC_EncounterID,
        PatientID,
        ProviderID,
        DepartmentID,
        EncounterDate,
        EncounterType,
        ProcedureCode,
        ModifiedDate,
        'hosa' AS datasource
    FROM `gcp-healthcare-etl-2025.bronze_dataset.encounters_ha`
    
    UNION ALL

    SELECT DISTINCT 
        EncounterID AS SRC_EncounterID,
        PatientID,
        ProviderID,
        DepartmentID,
        EncounterDate,
        EncounterType,
        ProcedureCode,
        ModifiedDate,
        'hosb' AS datasource
    FROM `gcp-healthcare-etl-2025.bronze_dataset.encounters_hb`
);

MERGE INTO `gcp-healthcare-etl-2025.silver_dataset.encounters` AS target
USING `gcp-healthcare-etl-2025.silver_dataset.quality_checks_encounters` AS source
ON target.Encounter_Key = source.Encounter_Key
AND target.is_current = TRUE 
WHEN MATCHED AND (
    target.SRC_EncounterID <> source.SRC_EncounterID OR
    target.PatientID <> source.PatientID OR
    target.ProviderID <> source.ProviderID OR
    target.DepartmentID <> source.DepartmentID OR
    target.EncounterDate <> source.EncounterDate OR
    target.EncounterType <> source.EncounterType OR
    target.ProcedureCode <> source.ProcedureCode OR
    target.SRC_ModifiedDate <> source.SRC_ModifiedDate OR
    target.datasource <> source.datasource OR
    target.is_quarantined <> source.is_quarantined
)
THEN UPDATE SET 
    target.is_current = FALSE,
    target.modified_date = CURRENT_TIMESTAMP()
WHEN NOT MATCHED 
THEN INSERT (
    Encounter_Key,
    SRC_EncounterID,
    PatientID,
    ProviderID,
    DepartmentID,
    EncounterDate,
    EncounterType,
    ProcedureCode,
    SRC_ModifiedDate,
    datasource,
    is_quarantined,
    inserted_date,
    modified_date,
    is_current
)
VALUES (
    source.Encounter_Key,
    source.SRC_EncounterID,
    source.PatientID,
    source.ProviderID,
    source.DepartmentID,
    source.EncounterDate,
    source.EncounterType,
    source.ProcedureCode,
    source.SRC_ModifiedDate,
    source.datasource,
    source.is_quarantined,
    CURRENT_TIMESTAMP(),  
    CURRENT_TIMESTAMP(),  
    TRUE 
);

DROP TABLE IF EXISTS `gcp-healthcare-etl-2025.silver_dataset.quality_checks_encounters`;

-- --------------------------------------------------------------------------------
-- Claims
--   -> convert monetary strings to FLOAT64 (SAFE_CAST)
--   -> timestamps normalized to TIMESTAMP
-- --------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `gcp-healthcare-etl-2025.silver_dataset.claims` (
    Claim_Key STRING,
    SRC_ClaimID STRING,
    TransactionID STRING,
    PatientID STRING,
    EncounterID STRING,
    ProviderID STRING,
    DeptID STRING,
    ServiceDate TIMESTAMP,
    ClaimDate TIMESTAMP,
    PayorID STRING,
    ClaimAmount FLOAT64,
    PaidAmount FLOAT64,
    ClaimStatus STRING,
    PayorType STRING,
    Deductible FLOAT64,
    Coinsurance FLOAT64,
    Copay FLOAT64,
    SRC_InsertDate TIMESTAMP,
    SRC_ModifiedDate TIMESTAMP,
    datasource STRING,
    is_quarantined BOOLEAN,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOLEAN
);

CREATE OR REPLACE TABLE `gcp-healthcare-etl-2025.silver_dataset.quality_checks_claims` AS
SELECT 
    CONCAT(SRC_ClaimID, '-', datasource) AS Claim_Key,
    SRC_ClaimID,
    TransactionID,
    PatientID,
    EncounterID,
    ProviderID,
    DeptID,
    CAST(ServiceDate AS TIMESTAMP) AS ServiceDate,
    CAST(ClaimDate AS TIMESTAMP) AS ClaimDate,
    PayorID,
    SAFE_CAST(ClaimAmount AS FLOAT64) AS ClaimAmount,
    SAFE_CAST(PaidAmount AS FLOAT64) AS PaidAmount,
    ClaimStatus,
    PayorType,
    SAFE_CAST(Deductible AS FLOAT64) AS Deductible,
    SAFE_CAST(Coinsurance AS FLOAT64) AS Coinsurance,
    SAFE_CAST(Copay AS FLOAT64) AS Copay,
    CAST(InsertDate AS TIMESTAMP) AS SRC_InsertDate,
    CAST(ModifiedDate AS TIMESTAMP) AS SRC_ModifiedDate,
    datasource,
    CASE 
        WHEN SRC_ClaimID IS NULL OR PatientID IS NULL OR TransactionID IS NULL OR LOWER(ClaimStatus) = 'null' THEN TRUE
        ELSE FALSE
    END AS is_quarantined
FROM (
    SELECT 
        ClaimID AS SRC_ClaimID,
        TransactionID,
        PatientID,
        EncounterID,
        ProviderID,
        DeptID,
        ServiceDate,
        ClaimDate,
        PayorID,
        ClaimAmount,
        PaidAmount,
        ClaimStatus,
        PayorType,
        Deductible,
        Coinsurance,
        Copay,
        InsertDate,
        ModifiedDate,
        'hosa' AS datasource
    FROM `gcp-healthcare-etl-2025.bronze_dataset.claims`
);

MERGE INTO `gcp-healthcare-etl-2025.silver_dataset.claims` AS target
USING `gcp-healthcare-etl-2025.silver_dataset.quality_checks_claims` AS source
ON target.Claim_Key = source.Claim_Key
AND target.is_current = TRUE 
WHEN MATCHED AND (
    target.SRC_ClaimID <> source.SRC_ClaimID OR
    target.TransactionID <> source.TransactionID OR
    target.PatientID <> source.PatientID OR
    target.EncounterID <> source.EncounterID OR
    target.ProviderID <> source.ProviderID OR
    target.DeptID <> source.DeptID OR
    target.ServiceDate <> source.ServiceDate OR
    target.ClaimDate <> source.ClaimDate OR
    target.PayorID <> source.PayorID OR
    target.ClaimAmount <> source.ClaimAmount OR
    target.PaidAmount <> source.PaidAmount OR
    target.ClaimStatus <> source.ClaimStatus OR
    target.PayorType <> source.PayorType OR
    target.Deductible <> source.Deductible OR
    target.Coinsurance <> source.Coinsurance OR
    target.Copay <> source.Copay OR
    target.SRC_ModifiedDate <> source.SRC_ModifiedDate OR
    target.datasource <> source.datasource OR
    target.is_quarantined <> source.is_quarantined
)
THEN UPDATE SET 
    target.is_current = FALSE,
    target.modified_date = CURRENT_TIMESTAMP()
WHEN NOT MATCHED 
THEN INSERT (
    Claim_Key,
    SRC_ClaimID,
    TransactionID,
    PatientID,
    EncounterID,
    ProviderID,
    DeptID,
    ServiceDate,
    ClaimDate,
    PayorID,
    ClaimAmount,
    PaidAmount,
    ClaimStatus,
    PayorType,
    Deductible,
    Coinsurance,
    Copay,
    SRC_InsertDate,
    SRC_ModifiedDate,
    datasource,
    is_quarantined,
    inserted_date,
    modified_date,
    is_current
)
VALUES (
    source.Claim_Key,
    source.SRC_ClaimID,
    source.TransactionID,
    source.PatientID,
    source.EncounterID,
    source.ProviderID,
    source.DeptID,
    source.ServiceDate,
    source.ClaimDate,
    source.PayorID,
    source.ClaimAmount,
    source.PaidAmount,
    source.ClaimStatus,
    source.PayorType,
    source.Deductible,
    source.Coinsurance,
    source.Copay,
    source.SRC_InsertDate,
    source.SRC_ModifiedDate,
    source.datasource,
    source.is_quarantined,
    CURRENT_TIMESTAMP(),  
    CURRENT_TIMESTAMP(),  
    TRUE 
);

DROP TABLE IF EXISTS `gcp-healthcare-etl-2025.silver_dataset.quality_checks_claims`;

-- --------------------------------------------------------------------------------
-- CPT Codes (unchanged)
-- --------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `gcp-healthcare-etl-2025.silver_dataset.cpt_codes` (
    CP_Code_Key STRING,
    procedure_code_category STRING,
    cpt_codes STRING,
    procedure_code_descriptions STRING,
    code_status STRING,
    datasource STRING,
    is_quarantined BOOLEAN,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_current BOOLEAN
);

CREATE OR REPLACE TABLE `gcp-healthcare-etl-2025.silver_dataset.quality_checks_cpt_codes` AS
SELECT 
    CONCAT(cpt_codes, '-', datasource) AS CP_Code_Key,
    procedure_code_category,
    cpt_codes,
    procedure_code_descriptions,
    code_status,
    datasource,
    CASE 
        WHEN cpt_codes IS NULL OR LOWER(code_status) = 'null' THEN TRUE
        ELSE FALSE
    END AS is_quarantined
FROM (
    SELECT 
        procedure_code_category,
        cpt_codes,
        procedure_code_descriptions,
        code_status,
        'hosa' AS datasource
    FROM `gcp-healthcare-etl-2025.bronze_dataset.cpt_codes`
);

MERGE INTO `gcp-healthcare-etl-2025.silver_dataset.cpt_codes` AS target
USING `gcp-healthcare-etl-2025.silver_dataset.quality_checks_cpt_codes` AS source
ON target.CP_Code_Key = source.CP_Code_Key
AND target.is_current = TRUE 
WHEN MATCHED AND (
    target.procedure_code_category <> source.procedure_code_category OR
    target.cpt_codes <> source.cpt_codes OR
    target.procedure_code_descriptions <> source.procedure_code_descriptions OR
    target.code_status <> source.code_status OR
    target.datasource <> source.datasource OR
    target.is_quarantined <> source.is_quarantined
)
THEN UPDATE SET 
    target.is_current = FALSE,
    target.modified_date = CURRENT_TIMESTAMP()
WHEN NOT MATCHED 
THEN INSERT (
    CP_Code_Key,
    procedure_code_category,
    cpt_codes,
    procedure_code_descriptions,
    code_status,
    datasource,
    is_quarantined,
    inserted_date,
    modified_date,
    is_current
)
VALUES (
    source.CP_Code_Key,
    source.procedure_code_category,
    source.cpt_codes,
    source.procedure_code_descriptions,
    source.code_status,
    source.datasource,
    source.is_quarantined,
    CURRENT_TIMESTAMP(),  
    CURRENT_TIMESTAMP(),  
    TRUE 
);

DROP TABLE IF EXISTS `gcp-healthcare-etl-2025.silver_dataset.quality_checks_cpt_codes`;
