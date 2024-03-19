
  
    

    create or replace table `airflow-retail-416720`.`retail`.`report_customer_invoices`
    
    

    OPTIONS()
    as (
      -- report_customer_invoices.sql
SELECT
  c.country,
  c.iso,
  COUNT(fi.invoice_id) AS total_invoices,
  SUM(fi.total) AS total_revenue
FROM `airflow-retail-416720`.`retail`.`fct_invoices` fi
JOIN `airflow-retail-416720`.`retail`.`dim_customer` c ON fi.customer_id = c.customer_id
GROUP BY c.country, c.iso
ORDER BY total_revenue DESC
LIMIT 10
    );
  