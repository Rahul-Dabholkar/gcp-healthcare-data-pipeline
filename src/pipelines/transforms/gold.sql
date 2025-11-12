-- ====================================================================================
-- 1) Provider Charge Summary
--    Total charge (billed Amount) per provider by department
-- ====================================================================================
CREATE TABLE IF NOT EXISTS `gcp-healthcare-etl-2025.gold_dataset.provider_charge_summary` (
    Provider_Name STRING,
    Dept_Name STRING,
    Amount FLOAT64
);

TRUNCATE TABLE `gcp-healthcare-etl-2025.gold_dataset.provider_charge_summary`;

INSERT INTO `gcp-healthcare-etl-2025.gold_dataset.provider_charge_summary`
SELECT 
    CONCAT(COALESCE(p.FirstName, ''), ' ', COALESCE(p.LastName, '')) AS Provider_Name,
    d.Name AS Dept_Name,
    SUM(COALESCE(t.Amount, 0.0)) AS Amount
FROM `gcp-healthcare-etl-2025.silver_dataset.transactions` t
LEFT JOIN `gcp-healthcare-etl-2025.silver_dataset.providers` p 
    ON p.ProviderID = t.ProviderID                                    -- use raw ProviderID (no split)
LEFT JOIN `gcp-healthcare-etl-2025.silver_dataset.departments` d 
    ON SPLIT(d.Dept_Id, "-")[SAFE_OFFSET(0)] = p.DeptID               -- Dept_Id = concat(deptid, '-', datasource)
WHERE t.is_quarantined = FALSE
  AND d.Name IS NOT NULL
GROUP BY Provider_Name, Dept_Name;


-- ====================================================================================
-- 2) Patient History (Gold)
--    Builds a denormalized historical view per patient: visits, encounters, transactions, claims
-- ====================================================================================
CREATE TABLE IF NOT EXISTS `gcp-healthcare-etl-2025.gold_dataset.patient_history` (
    Patient_Key STRING,
    SRC_PatientID STRING,
    FirstName STRING,
    LastName STRING,
    Gender STRING,
    DOB TIMESTAMP,
    Address STRING,
    EncounterDate TIMESTAMP,
    EncounterType STRING,
    Transaction_Key STRING,
    VisitDate TIMESTAMP,
    ServiceDate TIMESTAMP,
    BilledAmount FLOAT64,
    PaidAmount FLOAT64,
    ClaimStatus STRING,
    ClaimAmount FLOAT64,
    ClaimPaidAmount FLOAT64,
    PayorType STRING
);

TRUNCATE TABLE `gcp-healthcare-etl-2025.gold_dataset.patient_history`;

INSERT INTO `gcp-healthcare-etl-2025.gold_dataset.patient_history`
SELECT 
    p.Patient_Key,
    p.SRC_PatientID,
    p.FirstName,
    p.LastName,
    p.Gender,
    p.DOB,
    p.Address,
    e.EncounterDate,
    e.EncounterType,
    t.Transaction_Key,
    t.VisitDate,
    t.ServiceDate,
    COALESCE(t.Amount, 0.0) AS BilledAmount,
    COALESCE(t.PaidAmount, 0.0) AS PaidAmount,
    c.ClaimStatus,
    COALESCE(c.ClaimAmount, 0.0) AS ClaimAmount,
    COALESCE(c.PaidAmount, 0.0) AS ClaimPaidAmount,
    c.PayorType
FROM `gcp-healthcare-etl-2025.silver_dataset.patients` p
LEFT JOIN `gcp-healthcare-etl-2025.silver_dataset.encounters` e 
    ON p.SRC_PatientID = e.PatientID                       -- join on original source PatientID
LEFT JOIN `gcp-healthcare-etl-2025.silver_dataset.transactions` t 
    ON p.SRC_PatientID = t.PatientID                       -- join on original source PatientID
LEFT JOIN `gcp-healthcare-etl-2025.silver_dataset.claims` c 
    ON t.SRC_TransactionID = c.TransactionID
WHERE p.is_current = TRUE;


-- ====================================================================================
-- 3) Provider Performance Summary
--    Counts encounters, transactions, billed/paid totals, and claim approval rate per provider
-- ====================================================================================
CREATE TABLE IF NOT EXISTS `gcp-healthcare-etl-2025.gold_dataset.provider_performance` (
    ProviderID STRING,
    FirstName STRING,
    LastName STRING,
    Specialization STRING,
    TotalEncounters INT64,
    TotalTransactions INT64,
    TotalBilledAmount FLOAT64,
    TotalPaidAmount FLOAT64,
    ApprovedClaims INT64,
    TotalClaims INT64,
    ClaimApprovalRate FLOAT64
);

TRUNCATE TABLE `gcp-healthcare-etl-2025.gold_dataset.provider_performance`;

INSERT INTO `gcp-healthcare-etl-2025.gold_dataset.provider_performance`
SELECT 
    pr.ProviderID,
    pr.FirstName,
    pr.LastName,
    pr.Specialization,
    COUNT(DISTINCT e.Encounter_Key) AS TotalEncounters,
    COUNT(DISTINCT t.Transaction_Key) AS TotalTransactions,
    SUM(COALESCE(t.Amount, 0.0)) AS TotalBilledAmount,
    SUM(COALESCE(t.PaidAmount, 0.0)) AS TotalPaidAmount,
    COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) AS ApprovedClaims,
    COUNT(DISTINCT c.Claim_Key) AS TotalClaims,
    -- Claim approval rate as percentage (0-100). NULLIF avoids division by zero.
    ROUND(
      (SAFE_DIVIDE(COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END), NULLIF(COUNT(DISTINCT c.Claim_Key), 0)) * 100)
    , 2) AS ClaimApprovalRate
FROM `gcp-healthcare-etl-2025.silver_dataset.providers` pr
LEFT JOIN `gcp-healthcare-etl-2025.silver_dataset.encounters` e 
    ON pr.ProviderID = e.ProviderID
LEFT JOIN `gcp-healthcare-etl-2025.silver_dataset.transactions` t 
    ON pr.ProviderID = t.ProviderID
LEFT JOIN `gcp-healthcare-etl-2025.silver_dataset.claims` c 
    ON t.SRC_TransactionID = c.TransactionID
GROUP BY pr.ProviderID, pr.FirstName, pr.LastName, pr.Specialization;


-- ====================================================================================
-- 4) Department Performance Analytics
--    Department KPIs: encounters, transactions, billed/paid totals, avg payment per transaction
-- ====================================================================================
CREATE TABLE IF NOT EXISTS `gcp-healthcare-etl-2025.gold_dataset.department_performance` (
    Dept_Id STRING,
    DepartmentName STRING,
    TotalEncounters INT64,
    TotalTransactions INT64,
    TotalBilledAmount FLOAT64,
    TotalPaidAmount FLOAT64,
    AvgPaymentPerTransaction FLOAT64
);

TRUNCATE TABLE `gcp-healthcare-etl-2025.gold_dataset.department_performance`;

INSERT INTO `gcp-healthcare-etl-2025.gold_dataset.department_performance`
SELECT 
    d.Dept_Id,
    d.Name AS DepartmentName,
    COUNT(DISTINCT e.Encounter_Key) AS TotalEncounters,
    COUNT(DISTINCT t.Transaction_Key) AS TotalTransactions,
    SUM(COALESCE(t.Amount, 0.0)) AS TotalBilledAmount,
    SUM(COALESCE(t.PaidAmount, 0.0)) AS TotalPaidAmount,
    AVG(COALESCE(t.PaidAmount, 0.0)) AS AvgPaymentPerTransaction
FROM `gcp-healthcare-etl-2025.silver_dataset.departments` d
LEFT JOIN `gcp-healthcare-etl-2025.silver_dataset.encounters` e 
    ON SPLIT(d.Dept_Id, "-")[SAFE_OFFSET(0)] = e.DepartmentID  -- Dept_Id = "<deptid>-<datasource>"
LEFT JOIN `gcp-healthcare-etl-2025.silver_dataset.transactions` t 
    ON SPLIT(d.Dept_Id, "-")[SAFE_OFFSET(0)] = t.DeptID
WHERE d.is_quarantined = FALSE
GROUP BY d.Dept_Id, d.Name;


-- ====================================================================================
-- 5 & 6: Placeholder notes
--    You mentioned Financial Metrics and Payor Performance & Claims Summary.
--    Use the same approach: align types (timestamps, floats) and join on SRC_* ids.
-- ====================================================================================
