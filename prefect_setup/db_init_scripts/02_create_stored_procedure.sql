CREATE OR REPLACE PROCEDURE transform_and_load_sales()
LANGUAGE plpgsql
AS $BODY$
		BEGIN
		    TRUNCATE TABLE clean_sales;
				INSERT INTO clean_sales ( order_id, order_date, ship_date, ship_mode, customer_id, customer_name,
				segment, country, city, state, postal_code, region, product_id, category, sub_category, product_name, sales)
				SELECT
				    "Order ID" AS order_id,
				    TO_DATE("Order Date", 'DD/MM/YYYY') AS order_date,
    		    TO_DATE("Ship Date", 'DD/MM/YYYY') AS ship_date,
    		    "Ship Mode" AS ship_mode,
    		    "Customer ID" AS customer_id,
    		    "Customer Name" AS customer_name,
    		    "Segment" AS segment,
    		    "Country" AS country,
    		    "City" AS city,
    		    "State" AS state,
    		    "Postal Code"::text AS postal_code,
    		    "Region" AS region,
    		    "Product ID" AS product_id,
    		    "Category" AS category,
    		    "Sub-Category" AS sub_category,
    		    "Product Name" AS product_name,
    		    "Sales" AS sales
FROM
    raw_sales;

TRUNCATE TABLE monthly_sales_analysis;

    INSERT INTO monthly_sales_analysis
    WITH monthly_data AS (
        SELECT
            date_trunc('month', order_date) AS month_timestamp,
            SUM(sales) AS total_sales
        FROM
            clean_sales
        GROUP BY
            month_timestamp
    ),
    monthly_lag AS (
        SELECT
            month_timestamp,
            total_sales,
            LAG(total_sales, 1) OVER (ORDER BY month_timestamp) AS previous_month_sales
        FROM
            monthly_data
    )
    SELECT
        TO_CHAR(month_timestamp, 'YYYY-MM') AS months,
        total_sales,
        previous_month_sales,
        CASE
            WHEN previous_month_sales IS NULL OR previous_month_sales = 0 THEN NULL
            ELSE (total_sales - previous_month_sales) / previous_month_sales
        END AS pct_change
    FROM
        monthly_lag;

    TRUNCATE TABLE yearly_sales_analysis;

    INSERT INTO yearly_sales_analysis
    WITH yearly_data AS (
        SELECT
            date_trunc('year', order_date) AS year_timestamp,
            SUM(sales) AS total_sales
        FROM
            clean_sales
        GROUP BY
            year_timestamp
    ),
    yearly_lag AS (
        SELECT
            year_timestamp,
            total_sales,
            LAG(total_sales, 1) OVER (ORDER BY year_timestamp) AS previous_year_sales
        FROM
            yearly_data
    )
    SELECT
        TO_CHAR(year_timestamp, 'YYYY') AS years,
        total_sales,
        previous_year_sales,
        CASE
            WHEN previous_year_sales IS NULL OR previous_year_sales = 0 THEN NULL
            ELSE (total_sales - previous_year_sales) / previous_year_sales
        END AS pct_change
    FROM
        yearly_lag;

TRUNCATE TABLE category_sales_analysis;

INSERT INTO category_sales_analysis (
    category,
    sub_category,
    total_sales
)
SELECT
    category,
    sub_category,
    ROUND(SUM(sales)::numeric, 2) AS total_sales
FROM
    clean_sales
GROUP BY
    category,
    sub_category;

TRUNCATE TABLE kpi_summary;

INSERT INTO kpi_summary (
    total_revenue,
    average_order_value,
    average_daily_revenue
)
SELECT
    ROUND(SUM(sales)::numeric, 2) AS total_revenue,
    ROUND(AVG(sales)::numeric, 2) AS average_order_value,
    ROUND(
        SUM(sales)::numeric / (MAX(order_date) - MIN(order_date) + 1),
        2
    ) AS average_daily_revenue
FROM
    clean_sales;

		END;
$BODY$