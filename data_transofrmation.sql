CREATE OR REPLACE TABLE `tpds-445105.subscriptions_analysis.subscription_cleaned` AS
SELECT
DATE(subscription_started) AS subscription_start_date,
DATE(cancel_time) AS cancel_date,
subscription_plan_type
FROM `tpds-445105.subscriptions_analysis.subscription_raw`
WHERE
subscription_started IS NOT NULL
AND (cancel_time IS NULL OR cancel_time >= subscription_started)
AND subscription_started <= CURRENT_TIMESTAMP();