"""
HubSpot → BigQuery extract
Replicates Power Query filter logic and field selection.

Usage:
    cp .env.example .env  # fill in your values
    uv run hubspot-extract
"""

import os
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

HUBSPOT_TOKEN        = os.environ["HUBSPOT_TOKEN"]
GCP_PROJECT          = os.environ.get("GCP_PROJECT", "reporting-adc")
BQ_DATASET           = os.environ.get("BQ_DATASET", "hubspot")
BQ_TABLE             = os.environ.get("BQ_TABLE", "deals_test")
SERVICE_ACCOUNT_JSON = os.environ["SERVICE_ACCOUNT_JSON"]

HUBSPOT_API_URL  = "https://api.hubapi.com/crm/v3/objects/deals/search"
PAGE_SIZE        = 100
MAX_PAGES        = 50
FIRST_RUN_CUTOFF = datetime(2024, 7, 30, tzinfo=timezone.utc)

PROPERTIES = [
    "dealname", "amount", "hs_deal_probability", "dealstage", "pipeline",
    "closedate", "createdate", "dealtype",
    "hs_lastmodifieddate", "hs_is_closed_won",
    "adc_office", "adc_practice",
    "hubspot_team_id", "lead_initiator", "relationship_manager",
    "proposal_coordinator", "solution_architect", "hs_all_collaborator_owner_ids"
]

STAGE_MAP = {
    "311641563":             "Lead",
    "appointmentscheduled":  "Appointment Scheduled",
    "qualifiedtobuy":        "Needs Defined",
    "decisionmakerboughtin": "Negotiation",
    "986684659":             "Lead",
    "986684658":             "Appointment Scheduled",
    "986684660":             "Needs Defined",
    "presentationscheduled": "Proposal Sent",
    "contractsent":          "Contract Sent",
    "closedlost":            "Closed Lost",
    "closedwon":             "Closed Won",
}


def get_watermark(client):
    """Return the most recent hs_lastmodifieddate already in BQ, or None on first run."""
    try:
        result = client.query(
            f"SELECT MAX(hs_lastmodifieddate) AS watermark FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"
        ).result()
        return next(iter(result)).watermark  # datetime or None if table is empty
    except NotFound:
        return None  # table doesn't exist yet


def build_filter_groups(watermark=None):
    effective = watermark or FIRST_RUN_CUTOFF
    date_filter = [{"propertyName": "hs_lastmodifieddate", "operator": "GT", "value": str(int(effective.timestamp() * 1000))}]

    return [
        # Group A: Open deals (not closed lost, not lead)
        {"filters": [
            {"propertyName": "hs_is_closed_won", "operator": "EQ",     "value": "false"},
            {"propertyName": "dealstage",        "operator": "NOT_IN", "values": ["closedlost", "311641563"]},
            *date_filter,
        ]},
        # Group B: All closed-won deals
        {"filters": [
            {"propertyName": "hs_is_closed_won", "operator": "EQ", "value": "true"},
            *date_filter,
        ]},
        # Group C: Lead stage
        {"filters": [
            {"propertyName": "dealstage", "operator": "EQ", "value": "311641563"},
            *date_filter,
        ]},
        # Group D: Closed-lost
        {"filters": [
            {"propertyName": "dealstage", "operator": "EQ", "value": "closedlost"},
            *date_filter,
        ]},
    ]


def fetch_all_deals(filter_groups):
    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json"
    }
    all_deals = []
    after = None

    for page_num in range(1, MAX_PAGES + 1):
        body = {
            "limit": PAGE_SIZE,
            "properties": PROPERTIES,
            "filterGroups": filter_groups,
            "sorts": [{"propertyName": "hs_lastmodifieddate", "direction": "DESCENDING"}]
        }
        if after:
            body["after"] = after

        response = requests.post(HUBSPOT_API_URL, headers=headers, json=body)

        if response.status_code == 429:
            raise Exception("HubSpot rate limit hit. Wait a moment and retry.")
        if response.status_code != 200:
            raise Exception(f"HubSpot API error {response.status_code}: {response.text}")

        data = response.json()
        results = data.get("results", [])
        all_deals.extend(results)

        print(f"Page {page_num}: fetched {len(results)} deals (total so far: {len(all_deals)})")

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

    return all_deals


def transform_deals(raw_deals, extracted_at):
    rows = []
    for deal in raw_deals:
        props = deal.get("properties", {})
        dealstage_raw = props.get("dealstage")

        row = {
            "id":                   deal.get("id"),
            "dealname":             props.get("dealname"),
            "pipeline":             props.get("pipeline"),
            "dealtype":             props.get("dealtype"),
            "adc_office":                    props.get("adc_office"),
            "adc_practice":                  props.get("adc_practice"),
            "hubspot_team_id":               props.get("hubspot_team_id"),
            "lead_initiator":                props.get("lead_initiator"),
            "relationship_manager":          props.get("relationship_manager"),
            "proposal_coordinator":          props.get("proposal_coordinator"),
            "solution_architect":            props.get("solution_architect"),
            "hs_all_collaborator_owner_ids": props.get("hs_all_collaborator_owner_ids"),
            "amount":               float(props["amount"]) if props.get("amount") else None,
            "probability":          float(props["hs_deal_probability"]) if props.get("hs_deal_probability") else None,
            "hs_is_closed_won":     props.get("hs_is_closed_won") == "true",
            "createdate":           props.get("createdate"),
            "closedate":            props.get("closedate"),
            "hs_lastmodifieddate":  props.get("hs_lastmodifieddate"),
            "deal_stage":           STAGE_MAP.get(dealstage_raw, dealstage_raw),
            "extracted_at":         extracted_at,
        }
        rows.append(row)
    return rows


# Fields to compare against last known values in BQ — add any field here to make it trigger an append on change.
TRACKED_FIELDS = ["deal_stage", "amount", "probability", "closedate"]


def get_last_known_values(client, deal_ids):
    """Return {deal_id: {field: value}} for the most recent row per deal in BQ."""
    if not deal_ids:
        return {}
    ids_list = ", ".join(f"'{id}'" for id in deal_ids)
    fields = ", ".join(TRACKED_FIELDS)
    query = f"""
        SELECT id, {fields}
        FROM (
            SELECT id, {fields},
                   ROW_NUMBER() OVER (PARTITION BY id ORDER BY extracted_at DESC) AS rn
            FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
            WHERE id IN ({ids_list})
        )
        WHERE rn = 1
    """
    try:
        result = client.query(query).result()
        return {row.id: {field: getattr(row, field) for field in TRACKED_FIELDS} for row in result}
    except NotFound:
        return {}


def has_relevant_change(row, last_known):
    """Return True if any tracked field changed compared to the last known BQ values."""
    if row["id"] not in last_known:
        return True  # new deal, always append
    return any(row[field] != last_known[row["id"]][field] for field in TRACKED_FIELDS)


def load_to_bigquery(client, rows):
    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    schema = [
        bigquery.SchemaField("id",                  "STRING"),
        bigquery.SchemaField("dealname",             "STRING"),
        bigquery.SchemaField("pipeline",             "STRING"),
        bigquery.SchemaField("dealtype",             "STRING"),
        bigquery.SchemaField("adc_office",                    "STRING"),
        bigquery.SchemaField("adc_practice",                  "STRING"),
        bigquery.SchemaField("hubspot_team_id",               "STRING"),
        bigquery.SchemaField("lead_initiator",                "STRING"),
        bigquery.SchemaField("relationship_manager",          "STRING"),
        bigquery.SchemaField("proposal_coordinator",          "STRING"),
        bigquery.SchemaField("solution_architect",            "STRING"),
        bigquery.SchemaField("hs_all_collaborator_owner_ids", "STRING"),
        bigquery.SchemaField("amount",               "FLOAT"),
        bigquery.SchemaField("probability",          "FLOAT"),
        bigquery.SchemaField("hs_is_closed_won",     "BOOLEAN"),
        bigquery.SchemaField("createdate",           "TIMESTAMP"),
        bigquery.SchemaField("closedate",            "TIMESTAMP"),
        bigquery.SchemaField("hs_lastmodifieddate",  "TIMESTAMP"),
        bigquery.SchemaField("deal_stage",           "STRING"),
        bigquery.SchemaField("extracted_at",         "TIMESTAMP"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )

    job = client.load_table_from_json(rows, table_ref, job_config=job_config)
    job.result()

    table = client.get_table(table_ref)
    print(f"\nLoaded {len(rows)} new rows — table now has {table.num_rows} total rows in {table_ref}")


def main():
    extracted_at = datetime.now(timezone.utc).isoformat()
    print(f"Starting extract at {extracted_at}\n")

    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON)
    client = bigquery.Client(project=GCP_PROJECT, credentials=credentials)

    watermark = get_watermark(client)
    if watermark:
        print(f"Watermark: fetching deals modified after {watermark.isoformat()}")
    else:
        print(f"No watermark found — fetching deals modified after {FIRST_RUN_CUTOFF.date()} (first run)")

    filter_groups = build_filter_groups(watermark)

    print("\nFetching deals from HubSpot...")
    raw_deals = fetch_all_deals(filter_groups)
    print(f"\nTotal deals fetched: {len(raw_deals)}")

    if not raw_deals:
        print("No new or updated deals since last run.")
        return

    print("\nTransforming data...")
    rows = transform_deals(raw_deals, extracted_at)

    print("\nChecking for relevant changes...")
    last_known = get_last_known_values(client, [row["id"] for row in rows])
    rows = [row for row in rows if has_relevant_change(row, last_known)]
    print(f"{len(rows)} deals with relevant changes (tracked fields: {TRACKED_FIELDS})")

    if not rows:
        print("No relevant changes since last run.")
        return

    print(f"\nAppending {len(rows)} rows to BigQuery...")
    load_to_bigquery(client, rows)

    print("\nDone.")


if __name__ == "__main__":
    main()
