-- =====================================================================
-- Federation tests on the PKDD'99 financial dataset
--
-- Catalogs used (as defined in config/sources_example.yml, env = dev):
--   fed_pkdd_financial_pg_dev.ctu_financial   Lakehouse Federation -> PostgreSQL copy
--   jdbc_pkdd_financial_pg_dev.ctu_financial  JDBC views          -> same PostgreSQL copy
--   jdbc_ctulab_dev.ctu_financial             JDBC views          -> original MariaDB (CTU)
--   conn_denodo_29090_dev                     JDBC connection     -> Denodo (remote_query)
--
-- Put EXPLAIN in front of each query to see what is pushed down to each source.
-- =====================================================================


-- Test 1 - Data consistency across copies
-- Same aggregate on three copies: rows with the same status must match.
-- With EXPLAIN: PostgreSQL paths push the GROUP BY (PushedAggregates), MariaDB does not.
SELECT 'A. jdbc mariadb (CTU)' AS path, status, COUNT(*) AS loans, SUM(amount) AS total_amount
FROM jdbc_ctulab_dev.ctu_financial.loan GROUP BY status
UNION ALL
SELECT 'B. jdbc postgres', status, COUNT(*), SUM(amount)
FROM jdbc_pkdd_financial_pg_dev.ctu_financial.loan GROUP BY status
UNION ALL
SELECT 'C. federation postgres', status, COUNT(*), SUM(amount)
FROM fed_pkdd_financial_pg_dev.ctu_financial.loan GROUP BY status
ORDER BY status, path;


-- Test 2 - Cross-source join
-- Loans and accounts from PostgreSQL (federation), districts from MariaDB (JDBC).
-- Each source receives its own filtered query; join and aggregation run in Databricks.
-- CTU district columns: A2 = district name, A3 = region.
SELECT d.A3                       AS region,
       COUNT(*)                   AS loans,
       ROUND(AVG(l.amount), 0)    AS avg_loan_amount,
       ROUND(100 * AVG(CASE WHEN l.status IN ('B', 'D') THEN 1 ELSE 0 END), 1) AS bad_loan_pct
FROM fed_pkdd_financial_pg_dev.ctu_financial.loan    l
JOIN fed_pkdd_financial_pg_dev.ctu_financial.account a ON a.account_id = l.account_id
JOIN jdbc_ctulab_dev.ctu_financial.district          d ON d.district_id = a.district_id
WHERE l.duration >= 24
GROUP BY d.A3
ORDER BY bad_loan_pct DESC;


-- Test 3 - Semantic layer (Denodo metric view) vs raw data in Databricks
-- Denodo computes the metric (remote_query passes VQL unchanged, evaluate_metric included);
-- Databricks counts the same transactions on the PostgreSQL copy. diff must be 0.
-- Adapt the metric view name to your Denodo virtual database.
WITH denodo AS (
  SELECT trans_year, n AS denodo_count
  FROM remote_query('conn_denodo_29090_dev', query =>
    'SELECT trans_year, evaluate_metric(transaction_count) AS n
       FROM mv_yuga_fin_account_activity
      GROUP BY trans_year')
),
databricks AS (
  SELECT year(`date`) AS trans_year, COUNT(*) AS databricks_count
  FROM fed_pkdd_financial_pg_dev.ctu_financial.trans
  GROUP BY year(`date`)
)
SELECT COALESCE(d.trans_year, b.trans_year) AS trans_year,
       d.denodo_count, b.databricks_count,
       d.denodo_count - b.databricks_count AS diff
FROM denodo d FULL OUTER JOIN databricks b ON d.trans_year = b.trans_year
ORDER BY trans_year;
