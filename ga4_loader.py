# =========================================================
# GA4 → SNOWFLAKE SILVER MASTER LOADER
# =========================================================

import os
import json
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import snowflake.connector

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.oauth2 import service_account


# =========================================================
# DATE RANGE
# =========================================================
# Pull last 30 days excluding latest 2 days
# because GA4 data can be delayed

start_day = date.today() - timedelta(days=30)
end_day = date.today() - timedelta(days=2)

START_DATE = start_day.strftime("%Y-%m-%d")
END_DATE = end_day.strftime("%Y-%m-%d")


# =========================================================
# VALIDATE ENV VARIABLES
# =========================================================

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


# =========================================================
# AUTHENTICATION
# =========================================================

ga4_json = json.loads(
    os.getenv("GA4_SERVICE_ACCOUNT_JSON")
)

credentials = service_account.Credentials.from_service_account_info(
    ga4_json
)

data_client = BetaAnalyticsDataClient(
    credentials=credentials
)


# =========================================================
# PROPERTY CONFIG
# =========================================================

properties = [
    {
        "opco_name": "sakura",
        "property_id": "529120987",
        "property_name": "main_property"
    }
]

df_accounts = pd.DataFrame(properties)


# =========================================================
# PAYLOAD CONFIG
# =========================================================

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

    # OPTIONAL
    # Remove if ecommerce not enabled
    "ecommerce": {

        "dimensions": [
            {"name": "date"},
            {"name": "itemName"},
            {"name": "deviceCategory"}
        ],

        "metrics": [
            {"name": "itemRevenue"}
        ]
    }
}


# =========================================================
# HELPER FUNCTION
# =========================================================

def parse_ga4_row(row, dimensions, metrics):
    """
    Convert GA4 response row into dictionary.
    """

    record = {}

    # Dimensions
    for i, dim in enumerate(dimensions):

        record[dim["name"]] = (
            row.dimension_values[i].value
        )

    # Metrics
    for i, met in enumerate(metrics):

        value = row.metric_values[i].value

        try:
            record[met["name"]] = float(value)

        except Exception:
            record[met["name"]] = 0

    return record


# =========================================================
# FETCH DATA
# =========================================================

all_data = {
    k: [] for k in PAYLOAD_CONFIGS.keys()
}

for _, acc_row in df_accounts.iterrows():

    property_id = acc_row["property_id"]

    for payload_name, config in PAYLOAD_CONFIGS.items():

        print(
            f"\nRUNNING → {payload_name} | {property_id}"
        )

        request = {

            "property": f"properties/{property_id}",

            "dimensions": config["dimensions"],

            "metrics": config["metrics"],

            "date_ranges": [{
                "start_date": START_DATE,
                "end_date": END_DATE
            }]
        }

        try:

            response = data_client.run_report(request)

            if not response.rows:

                print(f"NO ROWS → {payload_name}")
                continue

            print(
                f"ROWS FETCHED → {len(response.rows)}"
            )

            for row in response.rows:

                record = parse_ga4_row(
                    row,
                    config["dimensions"],
                    config["metrics"]
                )

                # Metadata
                record["opco_name"] = (
                    acc_row["opco_name"]
                )

                record["property_id"] = (
                    property_id
                )

                record["property_name"] = (
                    acc_row["property_name"]
                )

                # Convert GA4 date string
                record["report_date"] = (
                    pd.to_datetime(
                        record["date"],
                        format="%Y%m%d"
                    ).date()
                )

                all_data[payload_name].append(record)

        except Exception as e:

            print(f"FAILED → {payload_name}")
            print(str(e))

            continue


# =========================================================
# CREATE DATAFRAMES
# =========================================================

df_traffic = pd.DataFrame(
    all_data["traffic_audience"]
)

df_events = pd.DataFrame(
    all_data["content_events"]
)

df_ecom = pd.DataFrame(
    all_data["ecommerce"]
)


# =========================================================
# STANDARDIZE TRAFFIC
# =========================================================

if not df_traffic.empty:

    df_traffic["channelGroup"] = (
        df_traffic["sessionDefaultChannelGroup"]
    )

    df_traffic_final = df_traffic[[

        "report_date",
        "opco_name",
        "property_id",
        "property_name",

        "deviceCategory",
        "country",
        "channelGroup",

        "totalUsers",
        "newUsers",
        "sessions",
        "engagedSessions"

    ]].copy()

    df_traffic_final["pagePath"] = None
    df_traffic_final["itemName"] = None
    df_traffic_final["eventName"] = None

    df_traffic_final["eventCount"] = 0
    df_traffic_final["keyEvents"] = 0
    df_traffic_final["totalRevenue"] = 0


# =========================================================
# STANDARDIZE EVENTS
# =========================================================

if not df_events.empty:

    df_events_final = df_events[[

        "report_date",
        "opco_name",
        "property_id",
        "property_name",

        "deviceCategory",
        "pagePath",
        "eventName",

        "eventCount",
        "keyEvents"

    ]].copy()

    df_events_final["country"] = None
    df_events_final["channelGroup"] = None
    df_events_final["itemName"] = None

    df_events_final["totalUsers"] = 0
    df_events_final["newUsers"] = 0
    df_events_final["sessions"] = 0
    df_events_final["engagedSessions"] = 0
    df_events_final["totalRevenue"] = 0


# =========================================================
# STANDARDIZE ECOMMERCE
# =========================================================

if not df_ecom.empty:

    df_ecom["totalRevenue"] = (
        df_ecom["itemRevenue"]
    )

    df_ecom_final = df_ecom[[

        "report_date",
        "opco_name",
        "property_id",
        "property_name",

        "deviceCategory",
        "itemName",
        "totalRevenue"

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


# =========================================================
# COLUMN ALIGNMENT
# =========================================================

final_cols = [

    "report_date",

    "opco_name",
    "property_id",
    "property_name",

    "deviceCategory",
    "country",
    "channelGroup",

    "pagePath",
    "itemName",
    "eventName",

    "totalUsers",
    "newUsers",
    "sessions",
    "engagedSessions",

    "eventCount",
    "keyEvents",

    "totalRevenue"
]

df_list = []

if "df_traffic_final" in locals():

    df_list.append(
        df_traffic_final[final_cols]
    )

if "df_events_final" in locals():

    df_list.append(
        df_events_final[final_cols]
    )

if "df_ecom_final" in locals():

    df_list.append(
        df_ecom_final[final_cols]
    )

if not df_list:
    raise Exception("No data fetched from GA4")

df_final = pd.concat(
    df_list,
    ignore_index=True
)


# =========================================================
# TECH COLUMNS
# =========================================================

df_final["ingestion_timestamp"] = (
    datetime.now()
)

df_final["source_system"] = "ga4"

df_final["load_date"] = (
    date.today()
)


# =========================================================
# REPLACE NaN WITH NULL
# =========================================================

df_final = df_final.where(
    pd.notnull(df_final),
    None
)


# =========================================================
# SNOWFLAKE CONNECTION
# =========================================================

conn = snowflake.connector.connect(

    user=os.getenv("SNOWFLAKE_USER"),

    password=os.getenv(
        "SNOWFLAKE_PASSWORD"
    ),

    account=os.getenv(
        "SNOWFLAKE_ACCOUNT"
    ),

    warehouse=os.getenv(
        "SNOWFLAKE_WAREHOUSE"
    ),

    database=os.getenv(
        "SNOWFLAKE_DATABASE"
    ),

    schema=os.getenv(
        "SNOWFLAKE_SCHEMA"
    )
)

cursor = conn.cursor()


# =========================================================
# CREATE TABLE
# =========================================================

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


# =========================================================
# DEDUP EXISTING DATE RANGE
# =========================================================

cursor.execute(f"""
DELETE FROM ga4_silver_master_v2
WHERE report_date BETWEEN '{START_DATE}'
AND '{END_DATE}'
""")


# =========================================================
# CLEAN VALUES FOR SNOWFLAKE
# =========================================================

def clean_value(value):
    """
    Convert pandas/numpy objects into
    Snowflake-safe native Python objects.
    """

    # NULL handling
    if pd.isna(value):
        return None

    # pandas timestamp
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    # python datetime
    if isinstance(value, datetime):
        return value

    # python date
    if isinstance(value, date):
        return value

    # numpy integer
    if isinstance(value, np.integer):
        return int(value)

    # numpy float
    if isinstance(value, np.floating):
        return float(value)

    # numpy bool
    if isinstance(value, np.bool_):
        return bool(value)

    return value


# =========================================================
# PREPARE INSERT DATA
# =========================================================

data = []

for row in df_final.itertuples(
    index=False,
    name=None
):

    clean_row = tuple(
        clean_value(v) for v in row
    )

    data.append(clean_row)


# =========================================================
# INSERT DATA
# =========================================================

cursor.executemany("""
INSERT INTO ga4_silver_master_v2 VALUES (

    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s
)
""", data)


# =========================================================
# COMMIT + CLOSE
# =========================================================

conn.commit()

cursor.close()
conn.close()


# =========================================================
# SUCCESS MESSAGE
# =========================================================

print("\nSILVER MASTER LOAD COMPLETE")

print(
    f"ROWS LOADED → {len(df_final)}"
)