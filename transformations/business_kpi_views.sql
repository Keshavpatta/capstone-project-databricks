-- Active Customers by Month
CREATE OR REFRESH MATERIALIZED VIEW capstone_project.gold.active_customers_by_month_mv
AS
SELECT
  date_trunc('month', customer_since) AS month,
  COUNT(DISTINCT customer_id) AS active_customers
FROM capstone_project.silver.customer_silver_pk
WHERE lower(is_active) = true
GROUP BY date_trunc('month', customer_since)
;

-- Monthly Transaction volume and failure rate
CREATE OR REFRESH MATERIALIZED VIEW capstone_project.gold.monthly_transaction_kpi_mv
AS
SELECT
  date_trunc('month', transaction_date) AS month,
  COUNT(transaction_id) AS total_transactions,
  COUNT_IF(lower(status) = 'failed' OR lower(status) = 'cancelled') AS failed_transactions,
  CASE WHEN COUNT(transaction_id) > 0
    THEN COUNT_IF(lower(status) = 'failed' OR lower(status) = 'cancelled') / COUNT(transaction_id)
    ELSE 0
  END AS failure_rate
FROM capstone_project.silver.transaction_silver_pk
GROUP BY date_trunc('month', transaction_date)
;

CREATE OR REFRESH MATERIALIZED VIEW capstone_project.gold.customer_wealth_segment_mv
AS
SELECT
  c.customer_id,
  c.first_name,
  c.last_name,
  c.masked_email,
  c.phone,
  c.city,
  c.state,
  c.annual_income,
  SUM(a.current_balance) AS total_balance,
  -- Wealth segment assignment based on total balance and annual income
  CASE
    WHEN SUM(a.current_balance) >= 1000000 OR c.annual_income >= 1000000 THEN 'Ultra High Net Worth'
    WHEN SUM(a.current_balance) >= 500000 OR c.annual_income >= 500000 THEN 'High Net Worth'
    WHEN SUM(a.current_balance) >= 100000 OR c.annual_income >= 100000 THEN 'Affluent'
    ELSE 'Mass Market'
  END AS wealth_segment
FROM capstone_project.silver.customer_silver_pk c
LEFT JOIN capstone_project.silver.accounts_silver_pk a
  ON c.customer_id = a.customer_id
GROUP BY
  c.customer_id,
  c.first_name,
  c.last_name,
  c.masked_email,
  c.phone,
  c.city,
  c.state,
  c.annual_income
;

CREATE OR REFRESH MATERIALIZED VIEW capstone_project.gold.credit_score_distribution_mv
AS
SELECT
  -- Credit score bucket
  CASE
    WHEN credit_score >= 800 THEN 'Excellent'
    WHEN credit_score >= 740 THEN 'Very Good'
    WHEN credit_score >= 670 THEN 'Good'
    WHEN credit_score >= 580 THEN 'Fair'
    WHEN credit_score IS NOT NULL THEN 'Poor'
    ELSE 'Unknown'
  END AS credit_score_segment,
  risk_category,
  COUNT(customer_id) AS customer_count
FROM capstone_project.silver.customer_silver_pk
GROUP BY
  CASE
    WHEN credit_score >= 800 THEN 'Excellent'
    WHEN credit_score >= 740 THEN 'Very Good'
    WHEN credit_score >= 670 THEN 'Good'
    WHEN credit_score >= 580 THEN 'Fair'
    WHEN credit_score IS NOT NULL THEN 'Poor'
    ELSE 'Unknown'
  END,
  risk_category
;



