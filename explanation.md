---                                                                                                                          
  The project in plain English
                                                                                                                               
  What it does in one sentence: Every month, NYC taxi trip data gets downloaded, cleaned, privacy-tokenized, quality-checked,
  and built into a queryable data warehouse — automatically, reliably, and with a full paper trail.                            
   
  ---                                                                                                                          
  The big picture — why does this exist?                                                                     
                                        
  You're a data engineer. The city of NYC publishes Yellow Taxi trip records every month — millions of rows, one Parquet file
  per month. Your job is to take that raw file and turn it into something analysts can actually query: clean tables, aggregated
   metrics, historical trends. And you need to do it in a way that:
  - Doesn't break if you run the same month twice                                                                              
  - Doesn't lose data if a quality check fails                                                                                 
  - Doesn't expose PII even though the source has none (we simulate that this data has passenger emails/phone numbers)
  - Tells you what happened, how long it took, how many rows passed or failed                                                  
                                                                                                                               
  ---                                                                                                                          
  Layer by layer — what each stage does and why                                                                                
                                                                                                                               
  Landing — "the loading dock"                                                                               
  The file arrives from TLC's website and sits here untouched. A _manifest.json is written next to it recording the URL, the   
  SHA-256 hash of the file, and the row count. This is your receipt. If anything goes wrong downstream, you can prove exactly  
  what came in.                                                                                                                
                                                                                                                               
  Bronze — "the raw vault"                                                                                                     
  Spark reads the Parquet, casts columns to proper types (TLC stores passenger_count as a DOUBLE for some reason — we cast it
  to INT), attaches metadata columns (_run_id, _ingested_at, _source_url), and writes an Apache Iceberg table. Bronze is the   
  source of truth — it's never cleaned, never aggregated, never filtered. If a row has a negative fare, it stays.
                                                                                                                               
  The key write pattern: INSERT OVERWRITE on the monthly partition. That means re-running January 2023 exactly replaces January
   2023 — no duplicates, no appends. This is idempotency: same input always produces the same output, no matter how many times
  you run it.                                                                                                                  
                                                                                                             
  PII tokenization — "the privacy layer"                                                                                       
  Because real pipelines deal with personal data (names, emails, phone numbers), we simulate that. A lookup table of fake PII
  is generated (deterministically — same person always gets the same fake email), then each value is hashed: SHA-256(value +   
  secret_salt) → a 64-character hex string. That hash goes into Bronze; the original value never does. If you need to erase
  someone (right-to-erasure), you rotate the salt — all old hashes become unresolvable instantly.                              
                                                                                                             
  Great Expectations Bronze gate — "the bouncer"
  Before anything moves to Silver, GE runs 12 checks on Bronze: is the row count reasonable? Are all PULocationID values
  between 1 and 265? Do dropoffs happen after pickups? Are the token columns exactly 64 chars (which confirms tokenization     
  actually ran)? If any check fails, the month gets routed to quarantine instead of Silver — a separate Iceberg table with
  extra columns explaining why it failed. Nothing broken ever reaches analysts.                                                
                                                                                                             
  Silver — "the cleaned version"
  dbt transforms Bronze into Silver. The two big jobs here:
  1. COALESCE(congestion_surcharge, 0) — this column didn't exist before 2019, so older months have NULLs. Silver converts them
   to 0 so aggregations don't silently drop rows.                                                                              
  2. The incremental strategy: dbt only rewrites the one month that changed, not the entire table.                             
                                                                                                                               
  Gold — "the star schema analysts actually use"                                                                               
  More dbt models build the dimensional model: fct_trips (one row per trip), fct_trips_daily (aggregated by date + zone +      
  payment type), fct_zone_revenue_monthly (revenue per zone per month). Dimensions like dim_date, dim_vendor, dim_taxi_zone    
  (with SCD2 — tracks zone name changes over time with valid-from/valid-to timestamps).                                        
                                                                                                                               
  ---                                                                                                        
  The key patterns and why they work
                                    
  Idempotency by partition overwrite. INSERT OVERWRITE partition(year=2023, month=1) atomically swaps all data for that
  partition. Running it 10 times has the same result as running it once.                                                       
   
  Data contract at each layer. Bronze guarantees typed columns + ingestion metadata. Silver guarantees no NULLs on evolved     
  columns + dbt structural tests pass. Gold guarantees surrogate key uniqueness + all FK relationships exist. Each layer's
  _schema.yml documents this explicitly.                                                                                       
                                                                                                             
  dbt vs Great Expectations split. dbt handles structural quality: "is this column always non-null? does this FK exist in the  
  dimension?" GE handles distributional quality: "is the average fare this month unusually low? is the p99 trip distance
  suddenly 258,000 miles (sensor error)?" You can't express the second kind in dbt tests.                                      
                                                                                                             
  The audit table. Every Airflow task writes one row to ops.pipeline_audit when it finishes: how many rows came in, how many   

  ---
  The key patterns and why they work

  Idempotency by partition overwrite. INSERT OVERWRITE partition(year=2023, month=1) atomically swaps all data for that
  partition. Running it 10 times has the same result as running it once.

  Data contract at each layer. Bronze guarantees typed columns + ingestion metadata. Silver guarantees no NULLs on evolved
  columns + dbt structural tests pass. Gold guarantees surrogate key uniqueness + all FK relationships exist. Each layer's
  _schema.yml documents this explicitly.

  dbt vs Great Expectations split. dbt handles structural quality: "is this column always non-null? does this FK exist in the
  dimension?" GE handles distributional quality: "is the average fare this month unusually low? is the p99 trip distance
  suddenly 258,000 miles (sensor error)?" You can't express the second kind in dbt tests.

  The audit table. Every Airflow task writes one row to ops.pipeline_audit when it finishes: how many rows came in, how many
  went out, how long it took, did GE pass. This is queryable SQL — "show me every month where the quarantine rate exceeded 1%."

  Factory function DAGs. The Airflow DAG file (yellow_taxi_monthly_ingest.py) is ~40 lines. All the actual task configuration
  lives in airflow/dags/shared/tasks_*.py as functions that return configured operators. The DAG just calls
  make_download_task(year, month) and wires the results together. This means all 3 DAGs can share the same building blocks
  without copy-pasting.

  ---