# 🗳️ RDH Voter File Processing & Redshift External Table Setup

## Overview

This project automates the ingestion and preparation of L2 voter file and voter history data into Redshift using **external tables**, based on real post-election snapshots. 

### Why This Change Was Needed

#### Previously:
- Files were manually selected and uploaded to Redshift when needed (e.g., by Adriana).
- Only loaded the **current** version of each state, without historical tracking.
- Files were queried based on **the date they were loaded**, not whether they reflected the elections of interest.
- No automation existed for detecting whether a file actually included data **after an election**.

#### Now:
- L2 ZIPs are synced continuously to `s3://datahub-redshift-ohio/l2_updates/`, organized by state and date.
- Our pipeline:
  - **Finds the first update after a specific election** (e.g., 2024 primary/general),
  - **Verifies** whether that file contains the relevant election columns,
  - **Converts** the `.tab` files to `.parquet`, and
  - **Creates Redshift external tables** pointing to the converted files.

This ensures Redshift queries reflect actual turnout after an election, not a random snapshot.

---

## Workflow Summary

1. **Election Dates of Interest**
   - 2024 Primary (e.g., May 21, 2024)
   - 2024 General (e.g., November 5, 2024)

2. **File Selection Logic**
   - Sort ZIPs in `l2_updates/` for a given state (e.g., KY)
   - For each election type:
     - Select the **first file dated after** the target election date
     - Unzip and scan `VOTEHISTORY.tab` headers
     - Check if it contains election fields (e.g., `2024-05-21_Primary` or `2024-11-05_General`)

3. **Conversion**
   - Convert both `DEMOGRAPHIC.tab` and `VOTEHISTORY.tab` to `.parquet`
   - Store locally or upload to S3 in `parquet/` path

4. **Redshift Integration**
   - Use Redshift `CREATE EXTERNAL TABLE` to define schemas
   - Reference `.parquet` file in S3
   - No data is ingested into Redshift storage; it queries S3 directly

---

## Current Issues

In past workflows, such as the `national_vf_and_vh_20210923` batch, the L2 voter file and vote history data were pre-processed and uploaded to S3 in a clean, structured format using Parquet. Each data type was stored in its own subdirectory: demo_clean_parquet/ and vh_spectrum_parquet/.  


These folders were then referenced directly by external tables in Redshift using the Spectrum feature, allowing immediate querying without ingesting data into Redshift storage.

However, the current workflow is significantly more manual and resource-intensive. Files in S3 are now stored as raw ZIP archives. To determine whether a file contains data for the desired election (e.g., 2024 primary or general), each ZIP must be downloaded, unzipped, and manually inspected. This process is both time-consuming and inefficient—especially since unzipping multi-gigabyte files locally consumes significant memory and often overwhelms my personal machines. After verification, the appropriate files must then be converted to Parquet and re-uploaded to S3 before being referenced via external tables.



