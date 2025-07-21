-- MMR Flat File SQL Parser
-- Ingests and parses fixed-width MMR flat file records into structured data
-- Compatible with Databricks SQL and most modern SQL engines

-- Step 1: Create table to hold raw flat file data
CREATE OR REPLACE TEMPORARY VIEW mmr_raw_data AS
SELECT 
    monotonically_increasing_id() as record_number,
    value as raw_record
FROM VALUES 
    -- Replace this with actual file reading method for your environment
    -- For Databricks: SELECT * FROM text.`/path/to/mmr_file.txt`
    ('H1234202507012025062345678901AB SMITH   JF19450315    12345Y Y    Y 1 Y   3 1.23456 2.34567121260202507012025073100000000000000000 00001234.5600001567.89   123.45T   0000.00   456.78100659    001C Y1123 Y   123.45   234.56   345.67   456.78   567.89   678.90   789.01                                                                                                                                       12   9876.54   1234.56 1.12345 0.98765   1111.11   2222.2200    0.00D1    1800.00   1900.00   2000.00TICK123456')
AS t(value);

-- Step 2: Create the main parsing query
CREATE OR REPLACE TEMPORARY VIEW mmr_parsed AS
SELECT 
    record_number,
    
    -- Basic Identification Fields (Positions 1-48)
    TRIM(SUBSTRING(raw_record, 1, 5)) as contract_number,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 6, 8)) != '' 
        THEN DATE_FORMAT(TO_DATE(SUBSTRING(raw_record, 6, 8), 'yyyyMMdd'), 'yyyy-MM-dd')
        ELSE NULL 
    END as run_date,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 14, 6)) != '' 
        THEN SUBSTRING(raw_record, 14, 4) || '-' || SUBSTRING(raw_record, 18, 2)
        ELSE NULL 
    END as payment_date,
    TRIM(SUBSTRING(raw_record, 20, 12)) as beneficiary_id,
    TRIM(SUBSTRING(raw_record, 32, 7)) as surname,
    TRIM(SUBSTRING(raw_record, 39, 1)) as first_initial,
    TRIM(SUBSTRING(raw_record, 40, 1)) as sex_code,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 41, 8)) != '' 
        THEN DATE_FORMAT(TO_DATE(SUBSTRING(raw_record, 41, 8), 'yyyyMMdd'), 'yyyy-MM-dd')
        ELSE NULL 
    END as date_of_birth,
    
    -- Geographic and Status Fields (Positions 53-70)
    TRIM(SUBSTRING(raw_record, 53, 5)) as state_county_code,
    CASE WHEN TRIM(SUBSTRING(raw_record, 58, 1)) = 'Y' THEN 1 
         WHEN TRIM(SUBSTRING(raw_record, 58, 1)) = 'N' THEN 0 
         ELSE NULL END as out_of_area_indicator,
    CASE WHEN TRIM(SUBSTRING(raw_record, 59, 1)) = 'Y' THEN 1 
         WHEN TRIM(SUBSTRING(raw_record, 59, 1)) = 'N' THEN 0 
         ELSE NULL END as part_a_entitlement,
    CASE WHEN TRIM(SUBSTRING(raw_record, 60, 1)) = 'Y' THEN 1 
         WHEN TRIM(SUBSTRING(raw_record, 60, 1)) = 'N' THEN 0 
         ELSE NULL END as part_b_entitlement,
    CASE WHEN TRIM(SUBSTRING(raw_record, 61, 1)) = 'Y' THEN 1 
         WHEN TRIM(SUBSTRING(raw_record, 61, 1)) = 'N' THEN 0 
         ELSE NULL END as hospice,
    CASE WHEN TRIM(SUBSTRING(raw_record, 62, 1)) = 'Y' THEN 1 
         WHEN TRIM(SUBSTRING(raw_record, 62, 1)) = 'N' THEN 0 
         ELSE NULL END as esrd,
    CASE WHEN TRIM(SUBSTRING(raw_record, 63, 1)) = 'Y' THEN 1 
         WHEN TRIM(SUBSTRING(raw_record, 63, 1)) = 'N' THEN 0 
         ELSE NULL END as aged_disabled_msp,
    CASE WHEN TRIM(SUBSTRING(raw_record, 66, 1)) = 'Y' THEN 1 
         WHEN TRIM(SUBSTRING(raw_record, 66, 1)) = 'N' THEN 0 
         ELSE NULL END as new_medicare_medicaid_flag,
    CASE WHEN TRIM(SUBSTRING(raw_record, 67, 1)) = 'Y' THEN 1 
         WHEN TRIM(SUBSTRING(raw_record, 67, 1)) = 'N' THEN 0 
         ELSE NULL END as lti_flag,
    CASE WHEN TRIM(SUBSTRING(raw_record, 68, 1)) = 'Y' THEN 1 
         WHEN TRIM(SUBSTRING(raw_record, 68, 1)) = 'N' THEN 0 
         ELSE NULL END as medicaid_addon_indicator,
    
    -- Risk Adjustment Fields (Positions 71-89)
    TRIM(SUBSTRING(raw_record, 71, 1)) as default_risk_factor_code,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 72, 7)) != '' 
        THEN CAST(SUBSTRING(raw_record, 72, 7) AS DECIMAL(7,5))
        ELSE NULL 
    END as risk_adjustment_factor_a,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 79, 7)) != '' 
        THEN CAST(SUBSTRING(raw_record, 79, 7) AS DECIMAL(7,5))
        ELSE NULL 
    END as risk_adjustment_factor_b,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 86, 2)) != '' 
        THEN CAST(SUBSTRING(raw_record, 86, 2) AS INT)
        ELSE NULL 
    END as payment_months_part_a,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 88, 2)) != '' 
        THEN CAST(SUBSTRING(raw_record, 88, 2) AS INT)
        ELSE NULL 
    END as payment_months_part_b,
    TRIM(SUBSTRING(raw_record, 90, 2)) as adjustment_reason_code,
    
    -- Payment Date Fields (Positions 92-107)
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 92, 8)) != '' 
        THEN DATE_FORMAT(TO_DATE(SUBSTRING(raw_record, 92, 8), 'yyyyMMdd'), 'yyyy-MM-dd')
        ELSE NULL 
    END as payment_start_date,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 100, 8)) != '' 
        THEN DATE_FORMAT(TO_DATE(SUBSTRING(raw_record, 100, 8), 'yyyyMMdd'), 'yyyy-MM-dd')
        ELSE NULL 
    END as payment_end_date,
    
    -- Financial Amount Fields (Positions 126-170)
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 126, 9)) != '' 
        THEN CAST(SUBSTRING(raw_record, 126, 9) AS DECIMAL(9,2))
        ELSE NULL 
    END as monthly_risk_adjusted_amount_a,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 135, 9)) != '' 
        THEN CAST(SUBSTRING(raw_record, 135, 9) AS DECIMAL(9,2))
        ELSE NULL 
    END as monthly_risk_adjusted_amount_b,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 144, 8)) != '' 
        THEN CAST(SUBSTRING(raw_record, 144, 8) AS DECIMAL(8,2))
        ELSE NULL 
    END as lis_premium_subsidy,
    TRIM(SUBSTRING(raw_record, 152, 1)) as esrd_msp_flag,
    
    -- Special Program Amounts (Set to zero per requirements)
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 153, 10)) != '' 
        THEN CAST(SUBSTRING(raw_record, 153, 10) AS DECIMAL(10,2))
        ELSE NULL 
    END as mtm_addon,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 163, 8)) != '' 
        THEN CAST(SUBSTRING(raw_record, 163, 8) AS DECIMAL(8,2))
        ELSE NULL 
    END as part_d_manufacturer_discount,
    
    -- Additional Status Fields (Positions 171-196)
    TRIM(SUBSTRING(raw_record, 171, 1)) as medicaid_dual_status,
    TRIM(SUBSTRING(raw_record, 172, 4)) as risk_adjustment_age_group,
    TRIM(SUBSTRING(raw_record, 185, 3)) as plan_benefit_package_id,
    TRIM(SUBSTRING(raw_record, 189, 2)) as risk_adjustment_factor_type,
    CASE WHEN TRIM(SUBSTRING(raw_record, 191, 1)) = 'Y' THEN 1 
         WHEN TRIM(SUBSTRING(raw_record, 191, 1)) = 'N' THEN 0 
         ELSE NULL END as frailty_indicator,
    TRIM(SUBSTRING(raw_record, 192, 1)) as orec,
    TRIM(SUBSTRING(raw_record, 194, 3)) as segment_number,
    CASE WHEN TRIM(SUBSTRING(raw_record, 198, 1)) = 'Y' THEN 1 
         WHEN TRIM(SUBSTRING(raw_record, 198, 1)) = 'N' THEN 0 
         ELSE NULL END as eghp_flag,
    
    -- Premium and Rebate Fields (Positions 199-254)
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 199, 8)) != '' 
        THEN CAST(SUBSTRING(raw_record, 199, 8) AS DECIMAL(8,2))
        ELSE NULL 
    END as part_c_basic_premium_a,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 207, 8)) != '' 
        THEN CAST(SUBSTRING(raw_record, 207, 8) AS DECIMAL(8,2))
        ELSE NULL 
    END as part_c_basic_premium_b,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 215, 8)) != '' 
        THEN CAST(SUBSTRING(raw_record, 215, 8) AS DECIMAL(8,2))
        ELSE NULL 
    END as rebate_part_a_cost_sharing,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 223, 8)) != '' 
        THEN CAST(SUBSTRING(raw_record, 223, 8) AS DECIMAL(8,2))
        ELSE NULL 
    END as rebate_part_b_cost_sharing,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 231, 8)) != '' 
        THEN CAST(SUBSTRING(raw_record, 231, 8) AS DECIMAL(8,2))
        ELSE NULL 
    END as rebate_part_a_supplemental,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 239, 8)) != '' 
        THEN CAST(SUBSTRING(raw_record, 239, 8) AS DECIMAL(8,2))
        ELSE NULL 
    END as rebate_part_b_supplemental,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 247, 8)) != '' 
        THEN CAST(SUBSTRING(raw_record, 247, 8) AS DECIMAL(8,2))
        ELSE NULL 
    END as rebate_part_b_premium_reduction_a,
    
    -- Part D and Additional Fields (Positions 390-445)
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 390, 2)) != '' 
        THEN CAST(SUBSTRING(raw_record, 390, 2) AS INT)
        ELSE NULL 
    END as payment_months_part_d,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 392, 10)) != '' 
        THEN CAST(SUBSTRING(raw_record, 392, 10) AS DECIMAL(10,2))
        ELSE NULL 
    END as pace_premium_addon,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 402, 10)) != '' 
        THEN CAST(SUBSTRING(raw_record, 402, 10) AS DECIMAL(10,2))
        ELSE NULL 
    END as pace_cost_sharing_addon,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 412, 7)) != '' 
        THEN CAST(SUBSTRING(raw_record, 412, 7) AS DECIMAL(7,5))
        ELSE NULL 
    END as part_c_frailty_factor,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 419, 7)) != '' 
        THEN CAST(SUBSTRING(raw_record, 419, 7) AS DECIMAL(7,5))
        ELSE NULL 
    END as msp_reduction_factor,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 426, 10)) != '' 
        THEN CAST(SUBSTRING(raw_record, 426, 10) AS DECIMAL(10,2))
        ELSE NULL 
    END as msp_reduction_amount_a,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 436, 10)) != '' 
        THEN CAST(SUBSTRING(raw_record, 436, 10) AS DECIMAL(10,2))
        ELSE NULL 
    END as msp_reduction_amount_b,
    TRIM(SUBSTRING(raw_record, 446, 2)) as medicaid_dual_status_code,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 448, 8)) != '' 
        THEN CAST(SUBSTRING(raw_record, 448, 8) AS DECIMAL(8,2))
        ELSE NULL 
    END as part_d_coverage_gap_discount,
    TRIM(SUBSTRING(raw_record, 456, 2)) as part_d_risk_adjustment_type,
    
    -- Monthly Rate Fields (Positions 459-485)
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 459, 9)) != '' 
        THEN CAST(SUBSTRING(raw_record, 459, 9) AS DECIMAL(9,2))
        ELSE NULL 
    END as part_a_monthly_rate,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 468, 9)) != '' 
        THEN CAST(SUBSTRING(raw_record, 468, 9) AS DECIMAL(9,2))
        ELSE NULL 
    END as part_b_monthly_rate,
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 477, 9)) != '' 
        THEN CAST(SUBSTRING(raw_record, 477, 9) AS DECIMAL(9,2))
        ELSE NULL 
    END as part_d_monthly_rate,
    
    -- Cleanup ID (Positions 486-495)
    TRIM(SUBSTRING(raw_record, 486, 10)) as cleanup_id,
    
    -- Add calculated fields
    CASE 
        WHEN TRIM(SUBSTRING(raw_record, 41, 8)) != '' 
        THEN YEAR(CURRENT_DATE()) - YEAR(TO_DATE(SUBSTRING(raw_record, 41, 8), 'yyyyMMdd'))
        ELSE NULL 
    END as calculated_age,
    
    -- Original raw record for reference
    raw_record
    
FROM mmr_raw_data;

-- Step 3: Create summary statistics view
CREATE OR REPLACE TEMPORARY VIEW mmr_field_summary AS
SELECT 
    'contract_number' as field_name,
    'str' as data_type,
    'Plan Contract Number' as description,
    COUNT(*) as total_records,
    COUNT(contract_number) as non_null_count,
    COUNT(*) - COUNT(contract_number) as null_count,
    COUNT(DISTINCT contract_number) as unique_values,
    COLLECT_LIST(contract_number)[0:3] as sample_values
FROM mmr_parsed

UNION ALL

SELECT 
    'surname' as field_name,
    'str' as data_type,
    'Beneficiary last name' as description,
    COUNT(*) as total_records,
    COUNT(surname) as non_null_count,
    COUNT(*) - COUNT(surname) as null_count,
    COUNT(DISTINCT surname) as unique_values,
    COLLECT_LIST(surname)[0:3] as sample_values
FROM mmr_parsed

UNION ALL

SELECT 
    'sex_code' as field_name,
    'str' as data_type,
    'Beneficiary Sex Code: M/F' as description,
    COUNT(*) as total_records,
    COUNT(sex_code) as non_null_count,
    COUNT(*) - COUNT(sex_code) as null_count,
    COUNT(DISTINCT sex_code) as unique_values,
    COLLECT_LIST(sex_code)[0:3] as sample_values
FROM mmr_parsed

UNION ALL

SELECT 
    'monthly_risk_adjusted_amount_a' as field_name,
    'amount' as data_type,
    'Monthly Risk Adjusted Amount Part A' as description,
    COUNT(*) as total_records,
    COUNT(monthly_risk_adjusted_amount_a) as non_null_count,
    COUNT(*) - COUNT(monthly_risk_adjusted_amount_a) as null_count,
    COUNT(DISTINCT monthly_risk_adjusted_amount_a) as unique_values,
    ARRAY(CAST(AVG(monthly_risk_adjusted_amount_a) AS STRING), 
          CAST(MIN(monthly_risk_adjusted_amount_a) AS STRING), 
          CAST(MAX(monthly_risk_adjusted_amount_a) AS STRING)) as sample_values
FROM mmr_parsed;

-- Step 4: File ingestion examples for different environments

-- For Databricks with file upload:
-- REPLACE TABLE mmr_raw_data AS
-- SELECT monotonically_increasing_id() as record_number, value as raw_record
-- FROM text.`/FileStore/shared_uploads/mmr_mock_flatfile.txt`;

-- For reading from DBFS:
-- CREATE OR REPLACE TEMPORARY VIEW mmr_raw_data AS
-- SELECT monotonically_increasing_id() as record_number, value as raw_record
-- FROM text.`dbfs:/path/to/mmr_mock_flatfile.txt`;

-- For reading from external storage (S3, Azure, etc.):
-- CREATE OR REPLACE TEMPORARY VIEW mmr_raw_data AS
-- SELECT monotonically_increasing_id() as record_number, value as raw_record
-- FROM text.`s3://bucket/path/mmr_mock_flatfile.txt`;

-- Step 5: Quality checks and validation queries

-- Record length validation
SELECT 
    record_number,
    LENGTH(raw_record) as record_length,
    CASE WHEN LENGTH(raw_record) = 495 THEN 'VALID' ELSE 'INVALID' END as length_status
FROM mmr_raw_data
WHERE LENGTH(raw_record) != 495
LIMIT 10;

-- Data quality summary
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT beneficiary_id) as unique_beneficiaries,
    COUNT(DISTINCT contract_number) as unique_contracts,
    SUM(CASE WHEN sex_code IN ('M', 'F') THEN 1 ELSE 0 END) as valid_sex_codes,
    SUM(CASE WHEN calculated_age BETWEEN 0 AND 120 THEN 1 ELSE 0 END) as valid_ages,
    AVG(monthly_risk_adjusted_amount_a) as avg_part_a_amount,
    AVG(monthly_risk_adjusted_amount_b) as avg_part_b_amount
FROM mmr_parsed;

-- Show parsed sample records
SELECT * FROM mmr_parsed LIMIT 5;

-- Show field summary
SELECT * FROM mmr_field_summary;