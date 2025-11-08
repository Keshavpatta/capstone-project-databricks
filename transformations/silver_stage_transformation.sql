CREATE OR REFRESH STREAMING TABLE capstone_project.silver.dim_customer;
CREATE FLOW customer_flow
AS AUTO CDC INTO
  capstone_project.silver.dim_customer
FROM stream(capstone_project.bronze.customers)
  KEYS (aadhar_number)
  SEQUENCE BY customer_id
  COLUMNS * EXCEPT (_rescued_data,ingestion_date)
  STORED AS SCD TYPE 1;

CREATE STREAMING TABLE capstone_project.silver.customer_silver_pk
(
  customer_id STRING,
  first_name STRING,
  last_name STRING,
  masked_email STRING,
  phone LONG,
  masked_date_of_birth STRING,
  standard_gender STRING,
  annual_income DOUBLE,
  masked_pan STRING,
  masked_aadhar STRING,
  city STRING,
  state STRING,
  pincode INTEGER,
  customer_since DATE,
  kyc_status STRING,
  credit_score INT,
  risk_category STRING,
  is_active BOOLEAN,
  CONSTRAINT id_and_name_not_null EXPECT (
    customer_id IS NOT NULL AND first_name IS NOT NULL
  ) ON VIOLATION DROP ROW,
  CONSTRAINT cust_id_pk PRIMARY KEY (customer_id)
)CLUSTER BY AUTO
AS
SELECT
  customer_id,
  first_name,
  last_name,
  CONCAT(
    SUBSTRING(email, 1, 1),
    REPEAT('*', LENGTH(SPLIT(email, '@')[0]) - 1),
    '@',
    SPLIT(email, '@')[1]
  ) AS masked_email,
  phone,
  CAST(YEAR(date_of_birth) AS STRING) AS masked_date_of_birth,
  CASE
    WHEN LOWER(gender) IN ('m', 'male') THEN 'Male'
    WHEN LOWER(gender) IN ('f', 'female') THEN 'Female'
    ELSE gender
  END AS standard_gender,
  annual_income,
  CONCAT(REPEAT('*', LENGTH(pan_number) - 4), RIGHT(pan_number, 4)) AS masked_pan,
  CONCAT(REPEAT('*', LENGTH(aadhar_number) - 4), RIGHT(aadhar_number, 4)) AS masked_aadhar,
  city,
  state,
  pincode,
  customer_since,
  kyc_status,
  credit_score,
  risk_category,
  is_active
FROM STREAM (capstone_project.silver.dim_customer);

-- **************** Branch Streaming Table Creation ***************
CREATE OR REFRESH STREAMING TABLE capstone_project.silver.branch_silver_pk
(
  branch_code STRING NOT NULL,
  branch_name STRING,
  branch_type STRING,
  compliance_score DOUBLE,
  address STRUCT<
    city: STRING,
    landmark: STRING,
    pincode: STRING,
    state: STRING,
    street_address: STRING
  >,
  masked_contact_phone ARRAY<STRING>,
  masked_email STRING,
  fax STRING,
  created_timestamp STRING,
  establishment_date STRING,
  ifsc_code STRING,
  is_active BOOLEAN,
  last_audit_date STRING,
  last_updated STRING,
  license_number STRING,
  micr_code STRING,
  operational_details STRUCT<
    atm_available: BOOLEAN,
    closing_time: STRING,
    opening_time: STRING,
    parking_available: BOOLEAN,
    wheelchair_accessible: BOOLEAN,
    working_days: STRING
  >,
  region STRING,
  services_offered ARRAY<STRING>,
  staff_details STRUCT<
    branch_manager: STRING,
    customer_service_officers: BIGINT,
    total_employees: BIGINT
  >,
  ingestion_ts TIMESTAMP NOT NULL,
  CONSTRAINT branch_code_pk PRIMARY KEY (branch_code)
)
AS
SELECT
  branch_code,
  branch_name AS branch_name,
  branch_type,
  compliance_score,
  address,
  TRANSFORM(
    contact_details.phone_numbers,
    phone -> CONCAT(REPEAT('*', LENGTH(phone) - 4), RIGHT(phone, 4))
  ) AS masked_contact_phone,
  CONCAT(
    SUBSTRING(contact_details.email, 1, 1),
    REPEAT('*', LENGTH(SPLIT(contact_details.email, '@')[0]) - 1),
    '@',
    SPLIT(contact_details.email, '@')[1]
  ) AS masked_email,
  contact_details.fax AS fax,
  created_timestamp,
  establishment_date,
  ifsc_code,
  is_active,
  last_audit_date,
  last_updated,
  license_number,
  micr_code,
  operational_details,
  region,
  services_offered,
  staff_details,
  current_timestamp() AS ingestion_ts
FROM STREAM(capstone_project.bronze.branch)
WHERE branch_code IS NOT NULL AND branch_name IS NOT NULL;


-- -- ******** Accounts Streaming Table Creation
CREATE STREAMING TABLE capstone_project.silver.accounts_silver_pk
(
  account_id STRING NOT NULL,
  account_number STRING NOT NULL,
  account_status STRING,
  account_type STRING,
  available_balance DOUBLE,
  branch_code STRING,
  created_timestamp STRING,
  current_balance DOUBLE,
  customer_id STRING,
  interest_rate DOUBLE,
  is_joint_account BOOLEAN,
  last_transaction_date DATE,
  minimum_balance DOUBLE,
  opening_date DATE,
  overdraft_limit DOUBLE,
  nominee_name STRING,
  nominee_relationship STRING,
  _rescued_data STRING,
  ingestion_date DATE,
  CONSTRAINT id_and_number_not_null EXPECT (
    account_id IS NOT NULL AND account_number IS NOT NULL
  ) ON VIOLATION DROP ROW,
  CONSTRAINT acct_id_pk PRIMARY KEY (account_id),
  CONSTRAINT fk_branch_code FOREIGN KEY (branch_code) REFERENCES capstone_project.silver.branch_silver_pk(branch_code),
  CONSTRAINT fk_cust_id FOREIGN KEY (customer_id) REFERENCES capstone_project.silver.customer_silver_pk(customer_id)
)CLUSTER BY AUTO
AS
SELECT
  account_id,
  account_number,
  account_status,
  account_type,
  available_balance,
  branch_code,
  created_timestamp,
  current_balance,
  customer_id,
  interest_rate,
  is_joint_account,
  CAST(last_transaction_date AS DATE) AS last_transaction_date,
  CAST(minimum_balance AS DOUBLE) AS minimum_balance,
  CAST(opening_date AS DATE) AS opening_date,
  overdraft_limit,
  nominee_details.nominee_name AS nominee_name,
  nominee_details.relationship AS nominee_relationship,
  _rescued_data,
  ingestion_date
FROM STREAM capstone_project.bronze.accounts
WHERE account_id IS NOT NULL AND account_number IS NOT NULL;


-- *********** Loans Streaming Table *************

CREATE STREAMING TABLE capstone_project.silver.loans_silver_pk
(
  loan_id STRING NOT NULL,
  customer_id STRING,
  branch_code STRING,
  loan_amount DOUBLE,
  interest_rate DOUBLE,
  loan_type STRING,
  start_date DATE,
  end_date DATE,
  loan_status STRING,
  _rescued_data STRING,
  ingestion_date DATE,
  CONSTRAINT code_and_name_not_null EXPECT (
    branch_code IS NOT NULL
  ) ON VIOLATION DROP ROW,
  CONSTRAINT loan_id_pk PRIMARY KEY (loan_id),
  CONSTRAINT fk_cust_id2 FOREIGN KEY (customer_id) REFERENCES capstone_project.silver.customer_silver_pk(customer_id)
)CLUSTER BY AUTO
AS
SELECT
  loan_id,
  customer_id,
  branch_code,
  loan_amount,
  interest_rate,
  loan_type,
  CAST(approval_date AS DATE) AS start_date,
  CAST(maturity_date AS DATE) AS end_date,
  loan_status,
  _rescued_data,
  ingestion_date
FROM STREAM capstone_project.bronze.loans
WHERE branch_code IS NOT NULL;


-- **************** Transaction Streaming Table *************

CREATE STREAMING TABLE capstone_project.silver.transaction_silver_pk
(
  transaction_id STRING NOT NULL,
  from_account_id STRING,
  to_account_id STRING,
  transaction_type STRING,
  transaction_amount DOUBLE,
  transaction_date DATE,
  transaction_timestamp TIMESTAMP,
  channel STRING,
  merchant_name STRING,
  merchant_category STRING,
  description STRING,
  reference_number STRING,
  status STRING,
  currency STRING,
  exchange_rate DOUBLE,
  fee_amount DOUBLE,
  location_city STRING,
  location_state STRING,
  device_type STRING,
  transaction_status STRING,
  remarks STRING,
  _rescued_data STRING,
  ingestion_date DATE,
  CONSTRAINT trans_id_pk PRIMARY KEY (transaction_id),
  CONSTRAINT fk_acct_idfrom FOREIGN KEY (from_account_id) REFERENCES capstone_project.silver.accounts_silver_pk(account_id),
  CONSTRAINT fk_acct_idto FOREIGN KEY (to_account_id) REFERENCES capstone_project.silver.accounts_silver_pk(account_id)
)CLUSTER BY AUTO
AS
SELECT
  transaction_id,
  from_account_id,
  to_account_id,
  transaction_type,
  amount AS transaction_amount,
  CAST(transaction_date AS DATE) AS transaction_date,
  transaction_timestamp,
  channel,
  merchant_name,
  merchant_category,
  description,
  reference_number,
  status,
  currency,
  exchange_rate,
  fee_amount,
  location_city,
  location_state,
  device_type,
  status AS transaction_status,
  description AS remarks,
  _rescued_data,
  ingestion_date
FROM STREAM capstone_project.bronze.transactions
WHERE transaction_id IS NOT NULL;


-- ALTER TABLE transaction_silver_pk CLUSTER BY AUTO;
-- alter table loans_silver_pk CLUSTER BY AUTO;
-- alter table accounts_silver_pk CLUSTER BY AUTO;
-- alter table customer_silver_pk CLUSTER BY AUTO;