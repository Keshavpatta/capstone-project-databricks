CREATE OR REFRESH MATERIALIZED VIEW capstone_project.gold.customer_360_mv
AS
SELECT
  c.customer_id,
  c.first_name,
  c.last_name,
  c.masked_email,
  c.phone,
  c.masked_date_of_birth,
  c.standard_gender,
  c.annual_income,
  c.city,
  c.state,
  c.pincode,
  c.customer_since,
  c.kyc_status,
  c.credit_score,
  c.risk_category,
  c.is_active,
  -- Account summary
  COUNT(a.account_id) AS num_accounts,
  SUM(a.current_balance) AS total_balance,
  -- Loan summary
  COUNT(l.loan_id) AS num_loans,
  SUM(l.loan_amount) AS total_loan_amount,
  -- Activity segment (example: based on total_balance)
  CASE
    WHEN SUM(a.current_balance) >= 100000 THEN 'High Value'
    WHEN SUM(a.current_balance) >= 50000 THEN 'Medium Value'
    ELSE 'Low Value'
  END AS activity_segment
FROM capstone_project.silver.customer_silver_pk c
LEFT JOIN capstone_project.silver.accounts_silver_pk a
  ON c.customer_id = a.customer_id
LEFT JOIN capstone_project.silver.loans_silver_pk l
  ON c.customer_id = l.customer_id
GROUP BY
  c.customer_id,
  c.first_name,
  c.last_name,
  c.masked_email,
  c.phone,
  c.masked_date_of_birth,
  c.standard_gender,
  c.annual_income,
  c.city,
  c.state,
  c.pincode,
  c.customer_since,
  c.kyc_status,
  c.credit_score,
  c.risk_category,
  c.is_active
;

CREATE OR REFRESH MATERIALIZED VIEW capstone_project.gold.transaction_kpi_mv
AS
SELECT
  CAST(transaction_date AS DATE) AS txn_date,
  COUNT(transaction_id) AS total_transactions,
  COUNT_IF(lower(status) = 'completed') AS successful_transactions,
  COUNT_IF(lower(status) = 'failed' or lower(status) = 'cancelled') AS failed_transactions,
  COUNT_IF(lower(status) = 'pending') AS pending_transactions,
  CASE WHEN COUNT(transaction_id) > 0
    THEN COUNT_IF(lower(status) = 'failed' or lower(status) = 'cancelled') / COUNT(transaction_id)
    ELSE 0
  END AS failure_rate,
  COUNT_IF(lower(channel) != 'branch' OR lower(channel) != 'atm') AS online_transactions,
  COUNT_IF(lower(channel) = 'branch') AS branch_transactions,
  AVG(transaction_amount) AS avg_amount,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY transaction_amount) AS median_amount
FROM capstone_project.silver.transaction_silver_pk
GROUP BY
  CAST(transaction_date AS DATE)
;

CREATE OR REFRESH MATERIALIZED VIEW capstone_project.gold.loan_kpi_mv
AS
SELECT
  CAST(end_date AS DATE) AS loan_end_date,
  COUNT(loan_id) AS total_loans,
  COUNT_IF(lower(loan_status) = 'defaulted') AS defaulted_loans,
  CASE WHEN COUNT(loan_id) > 0
    THEN COUNT_IF(lower(loan_status) = 'defaulted') / COUNT(loan_id)
    ELSE 0
  END AS default_rate,
  SUM(loan_amount) AS total_loan_amount,
  AVG(loan_amount) AS avg_loan_amount,
  MAX(loan_amount) AS max_loan_amount,
  MIN(loan_amount) AS min_loan_amount,
  AVG(interest_rate) AS avg_interest_rate,
  MAX(interest_rate) AS max_interest_rate,
  MIN(interest_rate) AS min_interest_rate,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY interest_rate) AS median_interest_rate
FROM capstone_project.silver.loans_silver_pk
GROUP BY
  CAST(end_date AS DATE)
;

CREATE OR REFRESH MATERIALIZED VIEW capstone_project.gold.loan_type_status_kpi_mv
AS
SELECT
  loan_type,
  lower(loan_status) AS loan_status,
  COUNT(loan_id) AS total_loans,
  SUM(loan_amount) AS total_loan_amount,
  AVG(loan_amount) AS avg_loan_amount,
  AVG(interest_rate) AS avg_interest_rate,
  COUNT_IF(lower(loan_status) = 'defaultted') AS defaulted_loans,
  CASE WHEN COUNT(loan_id) > 0
    THEN COUNT_IF(lower(loan_status) = 'defaultted') / COUNT(loan_id)
    ELSE 0
  END AS default_rate
FROM capstone_project.silver.loans_silver_pk
GROUP BY
  loan_type,
  lower(loan_status)
;

