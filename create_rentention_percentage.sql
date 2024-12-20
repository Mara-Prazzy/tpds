CREATE OR REPLACE VIEW `tpds-445105.subscriptions_analysis.retention_percentage_cleaned` AS
    WITH cohorts AS (
        SELECT
            DATE_TRUNC(subscription_start_date, MONTH) AS cohort_month,
            subscription_plan_type,
            COUNT(*) AS user_count
        FROM `tpds-445105.subscriptions_analysis.subscription_cleaned`
        GROUP BY cohort_month, subscription_plan_type
    ),
    retention AS (
        SELECT
            c.cohort_month,
            c.subscription_plan_type,
            c.user_count,
            COUNT(*) AS retained_users
        FROM `tpds-445105.subscriptions_analysis.subscription_cleaned` r
        JOIN cohorts c
        ON DATE_TRUNC(r.subscription_start_date, MONTH) = c.cohort_month
        AND r.subscription_plan_type = c.subscription_plan_type
        WHERE r.cancel_date IS NULL OR r.cancel_date > CURRENT_DATE()
        GROUP BY c.cohort_month, c.subscription_plan_type, c.user_count
    )
    SELECT
        cohort_month,
        subscription_plan_type,
        user_count,
        retained_users,
        ROUND((retained_users / user_count) * 100, 2) AS retention_rate
    FROM retention;