from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryInsertJobOperator
from datetime import datetime, timedelta
import csv
import gspread
from google.oauth2.service_account import Credentials
from google.cloud import storage

# Default arguments for the DAG
default_args = {
    'owner': 'Kumara_TPDS',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# DAG definition
# Runs every 8 hours: '0 */8 * * *'
with DAG(
    'subscription_retention_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 12, 18),
    schedule_interval='0 */8 * * *',
    catchup=False
) as dag:

    # -------------------------
    # Task 1: Extract Data and Upload to GCS
    # -------------------------
    def extract_and_upload():
        SERVICE_ACCOUNT_FILE = 'tpds-445105-aafa6b454353.json'
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        gc = gspread.authorize(creds)

        SHEET_ID = '1TU4xxs1PeGvZv6NH9g0uPv4Io8oLTum276cyccsska8'
        sh = gc.open_by_key(SHEET_ID)
        worksheet = sh.get_worksheet(0)  # first sheet/tab
        data = worksheet.get_all_values()

        header = data[0]
        rows = data[1:]

        CSV_FILENAME = 'subscriptions_data.csv'
        with open(CSV_FILENAME, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows)

        print(f"CSV file '{CSV_FILENAME}' created successfully.")

        # Upload to GCS
        storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
        BUCKET_NAME = 'tpdsbucket'
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(CSV_FILENAME)
        blob.upload_from_filename(CSV_FILENAME)

        print(f"File {CSV_FILENAME} uploaded to GCS bucket {BUCKET_NAME}.")

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_and_upload
    )

    # -------------------------
    # Task 2: Load CSV from GCS into BigQuery
    # -------------------------
    load_to_bq = BigQueryInsertJobOperator(
        task_id='load_to_bq',
        configuration={
            "load": {
                "sourceUris": ["gs://tpdsbucket/subscriptions_data.csv"],
                "destinationTable": {
                    "projectId": "tpds-445105",
                    "datasetId": "subscriptions_analysis",
                    "tableId": "subscriptions_raw"
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "autodetect": True,
                "sourceFormat": "CSV",
                "skipLeadingRows": 1
            }
        },
        location="northamerica-northeast2"
    )

    # -------------------------
    # Task 3: Create Cleaned Data Table
    # -------------------------
    create_cleaned_data = BigQueryExecuteQueryOperator(
        task_id='create_cleaned_data',
        sql='''
        CREATE OR REPLACE TABLE `tpds-445105.subscriptions_analysis.subscription_cleaned` AS
        SELECT
          *
        FROM `tpds-445105.subscriptions_analysis.subscriptions_raw`
        WHERE
          subscription_started IS NOT NULL
          AND (cancel_time IS NULL OR cancel_time >= subscription_started)
          AND subscription_started <= CURRENT_TIMESTAMP();
        ''',
        use_legacy_sql=False
    )

    # -------------------------
    # Task 4: Create Waterfall Retention View (Cleaned)
    # -------------------------
    create_waterfall_view = BigQueryExecuteQueryOperator(
        task_id='create_waterfall_view',
        sql='''
        CREATE OR REPLACE VIEW `tpds-445105.subscriptions_analysis.waterfall_retention_view_cleaned` AS
        WITH cte AS (
            SELECT
                DATE_TRUNC(subscription_started, MONTH) AS cohort_month,
                DATE_DIFF(
                  COALESCE(cancel_time, CURRENT_DATE()),
                  DATE_TRUNC(subscription_started, MONTH),
                  MONTH
                ) + 1 AS total_active_months
            FROM `tpds-445105.subscriptions_analysis.subscription_cleaned`
        )
        SELECT
            FORMAT_TIMESTAMP('%Y/%m', TIMESTAMP(cohort_month)) AS subscription_started,
            COUNTIF(total_active_months >= 1) AS Month_1,
            COUNTIF(total_active_months >= 2) AS Month_2,
            COUNTIF(total_active_months >= 3) AS Month_3,
            COUNTIF(total_active_months >= 4) AS Month_4,
            COUNTIF(total_active_months >= 5) AS Month_5,
            COUNTIF(total_active_months >= 6) AS Month_6,
            COUNTIF(total_active_months >= 7) AS Month_7,
            COUNTIF(total_active_months >= 8) AS Month_8,
            COUNTIF(total_active_months >= 9) AS Month_9,
            COUNTIF(total_active_months >= 10) AS Month_10,
            COUNTIF(total_active_months >= 11) AS Month_11,
            COUNTIF(total_active_months >= 12) AS Month_12,
            COUNTIF(total_active_months >= 13) AS Month_13,
            COUNTIF(total_active_months >= 14) AS Month_14,
            COUNTIF(total_active_months >= 15) AS Month_15,
            COUNTIF(total_active_months >= 16) AS Month_16,
            COUNTIF(total_active_months >= 17) AS Month_17,
            COUNTIF(total_active_months >= 18) AS Month_18,
            COUNTIF(total_active_months >= 19) AS Month_19,
            COUNTIF(total_active_months >= 20) AS Month_20,
            COUNTIF(total_active_months >= 21) AS Month_21,
            COUNTIF(total_active_months >= 22) AS Month_22,
            COUNTIF(total_active_months >= 23) AS Month_23,
            COUNTIF(total_active_months >= 24) AS Month_24,
            COUNTIF(total_active_months >= 25) AS Month_25,
            COUNTIF(total_active_months >= 26) AS Month_26,
            COUNTIF(total_active_months >= 27) AS Month_27,
            COUNTIF(total_active_months >= 28) AS Month_28,
            COUNTIF(total_active_months >= 29) AS Month_29,
            COUNTIF(total_active_months >= 30) AS Month_30,
            COUNTIF(total_active_months >= 31) AS Month_31,
            COUNTIF(total_active_months >= 32) AS Month_32,
            COUNTIF(total_active_months >= 33) AS Month_33,
            COUNTIF(total_active_months >= 34) AS Month_34,
            COUNTIF(total_active_months >= 35) AS Month_35,
            COUNTIF(total_active_months >= 36) AS Month_36,
            COUNTIF(total_active_months >= 37) AS Month_37,
            COUNTIF(total_active_months >= 38) AS Month_38,
            COUNTIF(total_active_months >= 39) AS Month_39,
            COUNTIF(total_active_months >= 40) AS Month_40,
            COUNTIF(total_active_months >= 41) AS Month_41,
            COUNTIF(total_active_months >= 42) AS Month_42,
            COUNTIF(total_active_months >= 43) AS Month_43,
            COUNTIF(total_active_months >= 44) AS Month_44,
            COUNTIF(total_active_months >= 45) AS Month_45,
            COUNTIF(total_active_months >= 46) AS Month_46,
            COUNTIF(total_active_months >= 47) AS Month_47,
            COUNTIF(total_active_months >= 48) AS Month_48,
            COUNTIF(total_active_months >= 49) AS Month_49,
            COUNTIF(total_active_months >= 50) AS Month_50,
            COUNTIF(total_active_months >= 51) AS Month_51,
            COUNTIF(total_active_months >= 52) AS Month_52,
            COUNTIF(total_active_months >= 53) AS Month_53,
            COUNTIF(total_active_months >= 54) AS Month_54,
            COUNTIF(total_active_months >= 55) AS Month_55,
            COUNTIF(total_active_months >= 56) AS Month_56,
            COUNTIF(total_active_months >= 57) AS Month_57,
            COUNTIF(total_active_months >= 58) AS Month_58,
            COUNTIF(total_active_months >= 59) AS Month_59,
            COUNTIF(total_active_months >= 60) AS Month_60,
            COUNTIF(total_active_months >= 61) AS Month_61,
            COUNTIF(total_active_months >= 62) AS Month_62,
            COUNTIF(total_active_months >= 63) AS Month_63
        FROM cte
        GROUP BY subscription_started
        ORDER BY subscription_started;
        ''',
        use_legacy_sql=False
    )

    # -------------------------
    # Task 5: Create Retention Percentage View (Cleaned)
    # This runs in parallel to the waterfall view after cleaned data is ready.
    # -------------------------
    create_retention_percentage = BigQueryExecuteQueryOperator(
        task_id='create_retention_percentage',
        sql='''
        CREATE OR REPLACE VIEW `tpds-445105.subscriptions_analysis.retention_percentage_cleaned` AS
        WITH cohorts AS (
            SELECT
                DATE_TRUNC(subscription_started, MONTH) AS cohort_month,
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
            ON DATE_TRUNC(r.subscription_started, MONTH) = c.cohort_month
              AND r.subscription_plan_type = c.subscription_plan_type
            WHERE r.cancel_time IS NULL OR r.cancel_time > CURRENT_DATE()
            GROUP BY c.cohort_month, c.subscription_plan_type, c.user_count
        )
        SELECT
            cohort_month,
            subscription_plan_type,
            user_count,
            retained_users,
            ROUND((retained_users / user_count) * 100, 2) AS retention_rate
        FROM retention;
        ''',
        use_legacy_sql=False
    )

    # Set task dependencies:
    # extract_data -> load_to_bq -> create_cleaned_data -> create_waterfall_view
    #                                            \
    #                                             -> create_retention_percentage
    extract_data >> load_to_bq >> create_cleaned_data >> [create_waterfall_view, create_retention_percentage]
