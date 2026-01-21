import requests
import json
import base64
import re
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session

# -----------------------------
#  Snowflake + Pipeline Setup
# -----------------------------
# Matillion DPC automatically provides the active Snowpark session
session = snowpark.context.get_active_session()

# Retrieve Variables from Matillion Environment
# Ensure these variables are defined in your Matillion Job or Environment
hostname = UKG_Hostname
username = UKG_Username
password = UKG_Password
api_key = UKG_API_Key
target_database = Target_Database
target_schema = Target_Schema

# Target Table Names
target_table = "UKG_EMPLOYMENT_DETAILS"
full_table_name = f"{target_database}.{target_schema}.{target_table}"
flat_table_name = f"{full_table_name}_FLAT"

# Pagination Configuration
page_size = int(Page_Size)   # Example: 100
max_pages = 20000            # Safety limit to prevent infinite loops

# -----------------------------
#  API Setup
# -----------------------------
endpoint = "/personnel/v1/employment-details"
base_url = f"https://{hostname}{endpoint}"

# Encode Credentials for Basic Auth
credentials = f"{username}:{password}"
encoded_credentials = base64.b64encode(credentials.encode()).decode()

headers = {
    "Authorization": f"Basic {encoded_credentials}",
    "US-Customer-Api-Key": api_key,
    "Accept": "application/json"
}

# -----------------------------
# Utility Functions
# -----------------------------
def sanitize_column_name(name: str) -> str:
    """Sanitizes JSON keys to create valid Snowflake column names."""
    if not name:
        return "COL_UNNAMED"
    # Replace non-alphanumeric characters with underscores
    col = re.sub(r"[^A-Za-z0-9_]", "_", name).upper()
    # Ensure column doesn't start with a number
    if re.match(r"^[0-9]", col):
        col = "_" + col
    return col

def is_excluded_column(col_name: str) -> bool:
    """
    SECURITY FILTER: Returns True if the column name contains sensitive
    keywords like SALARY, PAY, or COMPENSATION.
    """
    excluded_keywords = [
        'SALARY', 'HOURLY', 'RATE', 'PAY', 'COMPENSATION',
        'WAGE', 'EARNING', 'AMOUNT', 'BASE_PAY'
    ]
    col_upper = col_name.upper()
    return any(keyword in col_upper for keyword in excluded_keywords)

def map_json_type_to_snowflake(json_type: str) -> str:
    """Maps JSON data types to Snowflake data types."""
    jt = (json_type or "").upper()
    if jt == "NUMBER":
        return "NUMBER"
    if jt == "BOOLEAN":
        return "BOOLEAN"
    if jt == "VARCHAR":
        return "VARCHAR"
    return "VARIANT"  # Default for arrays, objects, or nulls

# -----------------------------
#  Step 1: Create RAW Staging Table
# -----------------------------
session.sql(f"""
    CREATE OR REPLACE TABLE {full_table_name} (
        RAW_DATA VARIANT,
        LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
""").collect()

print(f"\n🚀 Created staging table: {full_table_name}")
print("Beginning API pagination...\n")

# -----------------------------
#  Step 2: Pagination Loop (Extract & Load)
# -----------------------------
page = 1
total_loaded = 0

while page <= max_pages:

    url = f"{base_url}?page={page}&per_page={page_size}"
    print(f"➡️  Fetching Page {page} (limit {page_size}) ...")

    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ API Error on page {page}: {e}")
        raise

    # Normalize data structure (handle dict vs list responses)
    if isinstance(data, dict):
        page_data = data.get("entities") or data.get("data") or data.get("items") or []
    else:
        page_data = data

    if not page_data:
        print("✔ No more records found — stopping pagination.\n")
        break

    total_loaded += len(page_data)
    print(f"   → Retrieved {len(page_data)} records")

    # Insert batch into Snowflake
    # Escape single quotes to prevent SQL injection/errors in valid JSON
    json_str = json.dumps(page_data).replace("'", "''")

    session.sql(f"""
        INSERT INTO {full_table_name} (RAW_DATA)
        SELECT PARSE_JSON(column1)
        FROM VALUES ('{json_str}')
    """).collect()

    page += 1

print(f"\n🎉 COMPLETED: Loaded {total_loaded} total records into {full_table_name}\n")

# -----------------------------
#  Step 3: Flatten & Transform
# -----------------------------
print("📌 Inferring schema for flattening...")

# Dynamically infer schema using Snowflake LATERAL FLATTEN
schema_rows = session.sql(f"""
    SELECT
        k.key AS json_key,
        ANY_VALUE(TYPEOF(k.value)) AS json_type
    FROM {full_table_name} t,
         LATERAL FLATTEN(input => t.RAW_DATA) arr,
         LATERAL FLATTEN(input => arr.value) k
    GROUP BY k.key
""").collect()

if not schema_rows:
    print("⚠️ No schema detected, skipping FLAT table creation.")
else:
    columns_ddl = []
    select_clauses = []
    excluded_count = 0
    company_col_json_key = None

    for row in schema_rows:
        json_key = row["JSON_KEY"]
        col_name = sanitize_column_name(json_key)
        
        # Check for company code column for later filtering
        if "COMPANY" in json_key.upper() and "CODE" in json_key.upper():
            company_col_json_key = json_key

        # SECURITY CHECK: Skip salary/compensation columns
        if is_excluded_column(col_name):
            print(f"  ⚠️ Excluding sensitive column: {col_name}")
            excluded_count += 1
            continue
        
        sf_type = map_json_type_to_snowflake(row["JSON_TYPE"])

        columns_ddl.append(f"{col_name} {sf_type}")
        select_clauses.append(f"arr.value:{json.dumps(json_key)}::{sf_type} AS {col_name}")
    
    print(f"📊 Excluded {excluded_count} sensitive columns.")

    # Create the clean flattened table
    session.sql(f"""
        CREATE OR REPLACE TABLE {flat_table_name} (
            {", ".join(columns_ddl)}
        )
    """).collect()

    print(f"📄 Created flattened table: {flat_table_name}")

    # -----------------------------
    #  Step 4: Populate with Filter
    # -----------------------------
    if company_col_json_key:
        print(f"🔍 Filtering for 'GXLLC' using column: {company_col_json_key}")
        
        # Populate flat table with WHERE clause
        session.sql(f"""
            INSERT INTO {flat_table_name}
            SELECT {", ".join(select_clauses)}
            FROM {full_table_name} t,
                 LATERAL FLATTEN(input => t.RAW_DATA) arr
            WHERE arr.value:{json.dumps(company_col_json_key)}::VARCHAR = 'GXLLC'
        """).collect()
        
        count = session.sql(f"SELECT COUNT(*) as cnt FROM {flat_table_name}").collect()[0]["CNT"]
        print(f"✅ Populated flat table with {count} GXLLC-only records.")
    else:
        print("⚠️ Company code column not found, loading all records.")
        
        # Populate flat table with ALL records
        session.sql(f"""
            INSERT INTO {flat_table_name}
            SELECT {", ".join(select_clauses)}
            FROM {full_table_name} t,
                 LATERAL FLATTEN(input => t.RAW_DATA) arr
        """).collect()
        print(f"✅ Populated flat table with all records.")

# -----------------------------
#  Step 5: Cleanup
# -----------------------------
print("\n🗑️ Truncating RAW table to save space...")
session.sql(f"TRUNCATE TABLE {full_table_name}").collect()

print("\n✔ PROCESS COMPLETE.\n")
