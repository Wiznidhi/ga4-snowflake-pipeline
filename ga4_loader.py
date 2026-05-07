# =========================================================
# GA4 → SNOWFLAKE SILVER MASTER LOADER (FINAL CLEAN)
# =========================================================

import os
import json
from datetime import date, timedelta

import pandas as pd
import snowflake.connector

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.oauth2 import service_account


# ==============================
# DATE (YESTERDAY LOAD)
# ==============================
yesterday = date.today() - timedelta(days=1)
START_DATE = yesterday.strftime("%Y-%m-%d")
END_DATE = yesterday.strftime("%Y-%m-%d")


# ==============================
# VALIDATE ENV
# ==============================
required_env = [
    "GA4_SERVICE_ACCOUNT_JSON",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA"
]

for var in required_env:
    if not os.getenv(var):
        raise Exception(f"Missing ENV variable: {var}")


# ==============================
# AUTH
# ==============================
ga4_json = json.loads(os.getenv("GA4_SERVICE_ACCOUNT_JSON"))
credentials = service_account.Credentials.from_service_account_info(ga4_json)

data_client = BetaAnalyticsDataClient(credentials=credentials)


# ==============================
# CONFIG
# ==============================
PAYLOAD_CONFIGS = {
    "traffic_audience": {
        "dimensions": [
            {"name": "date"},
            {"name": "country"},
            {"name": "sessionDefaultChannelGroup"},
            {"name": "deviceCategory"}
        ],
        "metrics": [
            {"name": "totalUsers"},
            {"name": "newUsers"},
            {"name": "sessions"},
            {"name": "engagedSessions"}
        ]
    },
    "content_events": {
        "dimensions": [
            {"name": "date"},
            {"name": "pagePath"},
            {"name": "deviceCategory"},
            {"name": "eventName"}
        ],
        "metrics": [
            {"name": "eventCount"},
            {"name": "keyEvents"}
        ]
    },
    "ecommerce": {
        "dimensions": [
            {"name": "date"},
            {"name": "itemName"},
            {"name": "deviceCategory"}
        ],
        "metrics": [
            {"name": "itemRevenue"}  # ✅ FIXED
        ]
    }
}


# ==============================
# STATIC PROPERTIES
# ==============================
properties = [
    {
        "opco_name": "sakura",
        "property_id": "529120987",
        "property_name": "main_property"
    }
]

df_accounts = pd.DataFrame(properties)


# ==============================
# HELPER
# ==============================
def parse_ga4_row(row, dimensions, metrics):
    record = {}

    for i, dim in enumerate(dimensions):
        record[dim["name"]] = row.dimension_values[i].value

    for i, met in enumerate(metrics):
        record[met["name"]] = float(row.metric_values[i].value)

    return record


# ==============================
# FETCH DATA
# ==============================
all_data = {k: [] for k in PAYLOAD_CONFIGS.keys()}

for _, acc_row in df_accounts.iterrows():
    property_id = acc_row["property_id"]

    for payload_name, config in PAYLOAD_CONFIGS.items():
        print(f"{payload_name} → {property_id}")

        request = {
            "property": f"properties/{property_id}",
            "dimensions": config["dimensions"],
            "metrics": config["metrics"],
            "date_ranges": [{"start_date": START_DATE, "end_date": END_DATE}]
        }

        try:
            response = data_client.run_report(request)

            if not response.rows:
                continue

            for row in response.rows:
                record = parse_ga4_row(row, config["dimensions"], config["metrics"])

                record["opco_name"] = acc_row["opco_name"]
                record["property_id"] = property_id
                record["property_name"] = acc_row["property_name"]

                record["report_date"] = pd.to_datetime(
                    record["date"], format="%Y%m%d"
                ).date()

                all_data[payload_name].append(record)

        except Exception as e:
            print(f"FAILED → {payload_name} | {property_id}")
            print(e)


# ==============================
# DATAFRAMES
# ==============================
df_traffic = pd.DataFrame(all_data["traffic_audience"])
df_events = pd.DataFrame(all_data["content_events"])
df_ecom = pd.DataFrame(all_data["ecommerce"])


# ==============================
# STANDARDIZE TRAFFIC
# ==============================
if not df_traffic.empty:
    df_traffic["channelGroup"] = df_traffic["sessionDefaultChannelGroup"]

    df_traffic_final = df_traffic[[
        "report_date","opco_name","property_id","property_name",
        "deviceCategory","country","channelGroup",
        "totalUsers","newUsers","sessions","engagedSessions"
    ]].copy()

    df_traffic_final["pagePath"] = None
    df_traffic_final["itemName"] = None
    df_traffic_final["eventName"] = None
    df_traffic_final["eventCount"] = 0
    df_traffic_final["keyEvents"] = 0
    df_traffic_final["totalRevenue"] = 0


# ==============================
# STANDARDIZE EVENTS
# ==============================
if not df_events.empty:
    df_events_final = df_events[[
        "report_date","opco_name","property_id","property_name",
        "deviceCategory","pagePath","eventName","eventCount","keyEvents"
    ]].copy()

    df_events_final["country"] = None
    df_events_final["channelGroup"] = None
    df_events_final["itemName"] = None
    df_events_final["totalUsers"] = 0
    df_events_final["newUsers"] = 0
    df_events_final["sessions"] = 0
    df_events_final["engagedSessions"] = 0
    df_events_final["totalRevenue"] = 0


# ==============================
# STANDARDIZE ECOMMERCE
# ==============================
if not df_ecom.empty:
    df_ecom["totalRevenue"] = df_ecom["itemRevenue"]  # ✅ FIX

    df_ecom_final = df_ecom[[
        "report_date","opco_name","property_id","property_name",
        "deviceCategory","itemName","totalRevenue"
    ]].copy()

    df_ecom_final["country"] = None
    df_ecom_final["channelGroup"] = None
    df_ecom_final["pagePath"] = None
    df_ecom_final["eventName"] = None
    df_ecom_final["totalUsers"] = 0
    df_ecom_final["newUsers"] = 0
    df_ecom_final["sessions"] = 0
    df_ecom_final["engagedSessions"] = 0
    df_ecom_final["eventCount"] = 0
    df_ecom_final["keyEvents"] = 0


# ==============================
# UNION
# ==============================
df_list = []

if 'df_traffic_final' in locals():
    df_list.append(df_traffic_final)

if 'df_events_final' in locals():
    df_list.append(df_events_final)

if 'df_ecom_final' in locals():
    df_list.append(df_ecom_final)

if not df_list:
    raise Exception("No data fetched from GA4")

df_final = pd.concat(df_list, ignore_index=True)


# ==============================
# TECH COLUMNS
# ==============================
df_final["ingestion_timestamp"] = pd.Timestamp.now()
df_final["source_system"] = "ga4"
df_final["load_date"] = pd.Timestamp.now().date()


# ==============================
# SNOWFLAKE LOAD
# ==============================
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS ga4_silver_master_v2 (
    report_date DATE,
    opco_name STRING,
    property_id STRING,
    property_name STRING,
    deviceCategory STRING,
    country STRING,
    channelGroup STRING,
    pagePath STRING,
    itemName STRING,
    eventName STRING,
    totalUsers NUMBER,
    newUsers NUMBER,
    sessions NUMBER,
    engagedSessions NUMBER,
    eventCount NUMBER,
    keyEvents NUMBER,
    totalRevenue FLOAT,
    ingestion_timestamp TIMESTAMP,
    source_system STRING,
    load_date DATE
)
""")

# Dedup
cursor.execute(f"""
DELETE FROM ga4_silver_master_v2
WHERE report_date = '{START_DATE}'
""")

# Insert
data = [tuple(row) for row in df_final.to_numpy()]

cursor.executemany("""
INSERT INTO ga4_silver_master_v2 VALUES (
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s
)
""", data)

conn.commit()
conn.close()

print("SILVER MASTER LOAD COMPLETE")