CREATE TABLE clean_sales (
    order_id TEXT,
    order_date DATE,
    ship_date DATE,
    ship_mode TEXT,
    customer_id TEXT,
    customer_name TEXT,
    segment TEXT,
    country TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    product_id TEXT,
		region TEXT,
    category TEXT,
    sub_category TEXT,
    product_name TEXT,
    sales float
);

CREATE TABLE monthly_sales_analysis (
    months TEXT,
    total_sales NUMERIC,
    previous_month_sales NUMERIC,
    pct_change NUMERIC
);

CREATE TABLE yearly_sales_analysis (
    years TEXT,
    total_sales NUMERIC,
    previous_year_sales NUMERIC,
    pct_change NUMERIC
);

CREATE TABLE category_sales_analysis (
    category TEXT,
    sub_category TEXT,
    total_sales NUMERIC
);

CREATE TABLE kpi_summary (
    total_revenue NUMERIC,
    average_order_value NUMERIC,
    average_daily_revenue NUMERIC
);