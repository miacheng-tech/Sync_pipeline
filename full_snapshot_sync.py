import sqlite3
import argparse
import json
import requests
from datetime import datetime, timezone
from pathlib import Path



def parse_args():
    parser = argparse.ArgumentParser(description="Parse input and output from CLI")

    parser.add_argument(
        "--db",
        required=True,
        type=Path,
        help="path to local staging database"
    )

    parser.add_argument(
        "--run-id",
        required=True,
        help="a tracking batch to track records belonging to it"
    )

    parser.add_argument(
        "--base-url",
        required=True,
        help="Base url of mock ERP API, e.g. http://127.0.0.1:8000"
    )

    parser.add_argument(
        "--missing-policy",
        choices=["inactive", "delete"],
        default="inactive",
        help="How to handle staging missing from the latest full API snapshot"
    )

    return parser.parse_args()


# Generating UTC time in Python
def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# Ensure that the parent folder for the local database exists
def ensure_data_folder(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)


def create_tables(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sync_runs(
            run_id TEXT PRIMARY KEY ,
            source_name TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            rows_total INTEGER,
            rows_processed INTEGER,
            rows_failed INTEGER
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS inbound_staging_records(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            erp_record_id TEXT NOT NULL UNIQUE,
            invoice_no TEXT,
            vendor_key TEXT,
            raw_json TEXT,
            status TEXT NOT NULL,
            error_message TEXT,
            created_at TEXT,
            processed_at TEXT
        );
    """)



def start_run(conn: sqlite3.Connection, run_id: str) -> None:
    cursor = conn.cursor()


    cursor.execute(
        """
        INSERT INTO sync_runs
        (run_id, source_name, status, started_at, finished_at, rows_total, rows_processed, rows_failed)
        VALUES
        (?, ?, ?, ?, ?, ?, ?, ?);      
        """,
        (run_id, "mock_incoming_records", "running", utc_now(), None, 0, 0, 0,)
    )


def fetch_records_from_api(base_url: str) -> list[dict]:
    url = f"{base_url.rstrip('/')}/erp/invoices"

    try:
        response =  requests.get(url, timeout=10)
        response.raise_for_status()
        records = response.json()

    # Request timed out
    except requests.exceptions.Timeout as exc:
        raise RuntimeError(f"API request timed out: {url}") from exc
    
    # Unable to connect to the API
    except requests.exceptions.ConnectionError as exc:
        raise RuntimeError(f"Could not connect to API: {url}") from exc
    
    # HTTP errors such as 404 or 500 are typically triggered by `response.raise_for_status()`.
    except requests.exceptions.HTTPError as exc:
        raise RuntimeError(f"API return HTTP error: {exc}") from exc
    
    # The response body is not valid JSON, so `response.json()` fails to parse it.
    except requests.exceptions.JSONDecodeError as exc:
        raise RuntimeError(f"API response was not valid JSON: {url}") from exc

    # response shape validation
    if not isinstance(records, list):
        raise ValueError("API response must be a list of records")
    
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("Each API record must be a dict")
    
    return records 


# record-level validation
def validate_api_records(incoming_records: list[dict]) -> None:
    seen_erp_record_ids: set[str] = set()

    for record_num, record in enumerate(incoming_records, start=1):
        erp_record_id = record.get("erp_record_id")

        if not isinstance(erp_record_id, str):
            raise ValueError(f"Record {record_num}: erp_record_id must be a string")

        cleaned_erp_record_id = erp_record_id.strip()

        if not cleaned_erp_record_id:
            raise ValueError(f"Record {record_num}: erp_record_id cannot be empty")

        if cleaned_erp_record_id in seen_erp_record_ids:
            raise ValueError(f"Record {record_num}: duplicate erp_record_id: {erp_record_id}")
        else:
            seen_erp_record_ids.add(cleaned_erp_record_id)

        # Normalize the ID used by downstream matching.
        record["erp_record_id"] = cleaned_erp_record_id


# Use the business key (`erp_record_id`) to check the local staging table to see if this external record has been stored previously; if not found, return `None`. 
# Ensure that the relevant business fields—in this pipeline, `invoice_no` and `vendor_key`—are included in the `SELECT` statement.
def find_existing_record_by_external_id(conn: sqlite3.Connection, erp_record_id: str) -> sqlite3.Row | None:
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
        id,
        erp_record_id,
        invoice_no,
        vendor_key,
        raw_json,
        status
        FROM inbound_staging_records
        WHERE erp_record_id = ?; 
        """,
        (erp_record_id,),
    )

    return cursor.fetchone()


# Compare only the business fields, not the entire `raw_json`. If the `erp_record_id` remains the same but the `invoice_no` or `vendor_key` changes, consider the source data to have changed.
def record_has_changed(existing_record:sqlite3.Row, incoming_record: dict) -> bool:
    return(
        existing_record["invoice_no"] != incoming_record.get("invoice_no")
        or existing_record["vendor_key"] != incoming_record.get("vendor_key")
    )


# A "business key" is used for matching, while a "local id" is used to update the matched row; the "id" itself does not change as a result of an UPDATE operation.
def update_changed_staging_record(conn: sqlite3.Connection, staging_record_id: int, record: dict, run_id: str) -> None:
    cursor = conn.cursor()

    # Change status back to new here, because changed record requires re-entering select_new_records() → process_records() 
    # UPDATE You need to select this run_id, otherwise the subsequent select_new_records(conn, run_id) cannot select it for processing, because the status is already "new", but the run_id is still the old serial number
    cursor.execute(
        """
        UPDATE inbound_staging_records
        SET run_id = ?,
            invoice_no = ?,
            vendor_key = ?,
            raw_json = ?,
            status = 'new',  
            error_message = NULL,
            processed_at = NULL
        WHERE id = ?;
        """,
        (
            run_id,
            record.get("invoice_no"),
            record.get("vendor_key"),
            json.dumps(record),
            staging_record_id,
        ),
    )


# missing records -> inactive
def mark_missing_records_inactive(conn: sqlite3.Connection, incoming_erp_record_ids: set[str]) -> int:
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT erp_record_id
        FROM inbound_staging_records;
        """
    )


    existing_erp_record_ids = {row["erp_record_id"]for row in cursor.fetchall()}
    # Subtracting one set from another yields the missing `erp_record_ids`; `missing_ids` is also a set.
    missing_erp_record_ids = existing_erp_record_ids - incoming_erp_record_ids

    for erp_record_id in missing_erp_record_ids:
        cursor.execute(
            """
            UPDATE inbound_staging_records
            SET status = 'inactive',
                error_message = 'Record not returned by latest full snapshot',
                processed_at = NULL
            WHERE erp_record_id = ?;
            """,
            (erp_record_id,),
        )

    return len(missing_erp_record_ids)


# missing records -> delete
def delete_missing_records(conn: sqlite3.Connection, incoming_erp_record_ids: set[str]) -> int:
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT erp_record_id
        FROM inbound_staging_records
        """
    )

    existing_erp_record_ids = {row["erp_record_id"]for row in cursor.fetchall()}

    missing_erp_record_ids = existing_erp_record_ids - incoming_erp_record_ids

    for erp_record_id in missing_erp_record_ids:
        cursor.execute(
            """
            DELETE FROM inbound_staging_records
            WHERE erp_record_id = ?;
            """,
            (erp_record_id,),
        )

    return len(missing_erp_record_ids)




"""
Sync process:

`erp_record_id` from the incoming record
↓
Look up the record in the staging table using `erp_record_id`
↓
Locate the `existing_record`
↓
Retrieve the `id` of the `existing_record`
↓
Perform a precise `UPDATE` on the corresponding local staging row using that `id`

Typically, this layer does not involve complex business logic. 
Its primary responsibility is to ingest records into the staging table — preserving the original input while populating the necessary tracking fields.
"""

# Store the `list[dict]` returned by `get_mock_incoming_records()` into an SQLite staging table; as each incoming record is inserted into the table, append the tracking fields generated by the pipeline itself.
def sync_staging_records(conn: sqlite3.Connection, run_id: str, records: list[dict], missing_policy: str) -> dict[str, int]:
    cursor = conn.cursor()

    inserted_count = 0
    skipped_count = 0
    updated_count = 0
    missing_count = 0

    incoming_erp_record_ids = set()


    for record in records:
        erp_record_id = record.get("erp_record_id")

        # Collect the `erp_record_id` ​​returned by this API call.
        incoming_erp_record_ids.add(erp_record_id)

        existing_record = find_existing_record_by_external_id(conn, erp_record_id)


        # The INSERT is executed only if this record does not exist in the local staging environment.
        if existing_record is None:
            cursor.execute(
                """
                INSERT INTO inbound_staging_records( 
                    run_id,
                    erp_record_id,
                    invoice_no,
                    vendor_key,
                    raw_json,
                    status,
                    error_message,
                    created_at,
                    processed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    run_id,
                    erp_record_id,
                    record.get("invoice_no"),
                    record.get("vendor_key"),
                    json.dumps(record),
                    "new",
                    None,
                    utc_now(),
                    None,
                ),
            )

            inserted_count += 1


        # This record has already been modified in the local staging environment, or it is a record that was marked as inactive during a previous round.
        elif (record_has_changed(existing_record, record) or existing_record["status"] == "inactive"):
            # Auto-incrementing ID in a local SQLite staging table
            staging_record_id = existing_record["id"]

            update_changed_staging_record(conn, staging_record_id, record, run_id)
            updated_count += 1

        # This record has not changed，skip
        else:
            skipped_count += 1


    """ missing_policy 是基于 API response = latest full snapshot 这种情况 """
    # missing -> inactive
    if missing_policy == "inactive":
        missing_count = mark_missing_records_inactive(conn, incoming_erp_record_ids)

    # missing -> delete
    elif missing_policy == "delete":
        missing_count = delete_missing_records(conn, incoming_erp_record_ids)


    return {
            "inserted": inserted_count,
            "skipped": skipped_count,
            "updated": updated_count,
            "missing": missing_count,
        }



""" Business validation """
# The staging table already contains records, each with a status of 'new'. Now, Python needs to retrieve the 'new' records that require processing for the current run.
def select_new_records(conn: sqlite3.Connection, run_id: str,) -> list[sqlite3.Row]:
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            id,
            run_id,
            erp_record_id,
            invoice_no,
            vendor_key,
            raw_json,
            status,
            error_message,
            created_at,
            processed_at
        FROM inbound_staging_records
        WHERE run_id = ?
          AND status = 'new'
        ORDER BY id;
        """,
        (run_id,),  
    )

    return cursor.fetchall()


def mark_processed(conn: sqlite3.Connection, record_id: int,) -> None:
    cursor = conn.cursor()

    # SELECT a batch of records → use `run_id`; UPDATE a single record → use `id`
    cursor.execute(
        """
        UPDATE inbound_staging_records
        SET status = 'processed',
            error_message = NULL,
            processed_at = ?
        WHERE id = ?
          AND status = 'new';
        """,
        (utc_now(), record_id,),
    )



def mark_failed(conn: sqlite3.Connection, record_id: int, error_message: str,) -> None:
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE inbound_staging_records
        SET status = 'failed',
            error_message = ?,
            processed_at = ?
        WHERE id = ?
          AND status = 'new';
        """,
        (error_message, utc_now(), record_id,),
    )



# validate each record
def process_records(conn: sqlite3.Connection, records: list[sqlite3.Row],) -> None:
    for record in records:
        invoice_no = record["invoice_no"]   # sqlite3.Row 没有 .get()

        if not isinstance(invoice_no, str) or not invoice_no.strip():
            mark_failed(conn, record["id"], "Missing or invalid invoice_no")
        else:
            mark_processed(conn, record["id"])



def update_run_summary(conn: sqlite3.Connection, run_id: str) -> None:
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            COUNT(*) AS rows_total,
            COALESCE(SUM(CASE WHEN status = 'processed' THEN 1 ELSE 0 END), 0) AS rows_processed,
            COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0) AS rows_failed
        FROM inbound_staging_records
        WHERE run_id = ?;
        """,
        (run_id,),
    )

    # Retrieve the first row from the results of the SELECT query that was just executed.
    summary = cursor.fetchone()

    rows_total = summary["rows_total"]
    rows_processed = summary["rows_processed"]
    rows_failed = summary["rows_failed"]

    if rows_failed > 0:
        final_status = "completed_with_errors"
    else:
        final_status = "completed"

    # update summary into sync_runs
    cursor.execute(
        """
        UPDATE sync_runs
        SET status = ?,
            finished_at = ?,
            rows_total = ?,
            rows_processed = ?,
            rows_failed =?
        WHERE run_id = ?
          AND status = 'running';
        """,
        (final_status, utc_now(), rows_total, rows_processed, rows_failed, run_id,),        
    )



# This function is to let us see the final result in the terminal.
def print_final_tables(conn: sqlite3.Connection, run_id: str) -> None:
    cursor = conn.cursor()

    print("\n=== sync_runs ===")
    cursor.execute(
        """
        SELECT
            run_id,
            source_name,
            status,
            started_at,
            finished_at,
            rows_total,
            rows_processed,
            rows_failed
        FROM sync_runs
        WHERE run_id = ?
        ORDER BY started_at;
        """,
        (run_id,),
    )


    for row in cursor.fetchall():
        print(dict(row))


    print("\n=== inbound_staging_records ===")
    cursor.execute(
        """
        SELECT
            id,
            run_id,
            erp_record_id,
            invoice_no,
            vendor_key,
            status,
            error_message,
            created_at,
            processed_at
        FROM inbound_staging_records
        WHERE run_id = ?
        ORDER BY id;
        """,
        (run_id,),
    )

    current_rows = cursor.fetchall()
    
    if not current_rows:
        print("(no new staging records inserted in this run)")
    else:
        for row in current_rows:
            print(dict(row))


    print("\n=== current staging snapshot ===")
    cursor.execute(
        """
        SELECT
            id,
            run_id,
            erp_record_id,
            invoice_no,
            vendor_key,
            status,
            error_message,
            created_at,
            processed_at
        FROM inbound_staging_records
        ORDER BY id;
        """
    )

    for row in cursor.fetchall():
        print(dict(row))

                  


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)
    run_id = args.run_id
    base_url = args.base_url
    missing_policy = args.missing_policy

    ensure_data_folder(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row

        create_tables(conn)

        try:
            start_run(conn, run_id)

            incoming_records = fetch_records_from_api(base_url)           
            print(f"Fetched mock ERP API records: {len(incoming_records)}")

            validate_api_records(incoming_records)

            # sync_result here is a dict
            sync_result = sync_staging_records(conn, run_id, incoming_records, missing_policy)
            print(f"Inserted staging records: {sync_result['inserted']}")
            print(f"Skipped unchanged records: {sync_result['skipped']}")
            print(f"Updated changed records: {sync_result['updated']}")
            print(f"Missing records handled: {sync_result['missing']}")


            # new_records = list[sqlite3.Row]
            new_records = select_new_records(conn, run_id)
            print(f"Selected new records: {len(new_records)}")
            
            process_records(conn, new_records)

            update_run_summary(conn, run_id)

            conn.commit()

            print_final_tables(conn, run_id)

            return 0

        except (RuntimeError, ValueError, sqlite3.Error) as exc:
            conn.rollback()
            print(f"Pipeline failed, rolled back changes: {exc}")

            return 1



if __name__ == "__main__":
    raise SystemExit(main())