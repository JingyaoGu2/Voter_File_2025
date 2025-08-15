# RDH Voter File Processing & Redshift External Table Setup & Election Turnouts Calculations

## Overview

This project automates the ingestion and preparation of L2 voter file(demographics) and voter history(vh) data into Redshift using **external tables**, based on real post-election snapshots. 

### Why This Change Was Needed

#### Previously:
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
Different states have different primary dates and the same general dates. Double check with this pdf here(https://www.fvap.gov/uploads/FVAP/VAO/PrimaryElectionsCalendar.pdf)
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

## Current Pipeline as of August 14

### Step 1: Connect to EC2
Because voter files for certain states are huge, eg. CA, TX, FL, it is impossible to use personal machine to unzip and process. So we set up an EC2 instance to use virtual machines to help us.  

Go to your terminal and type(credentials may change, create a set of public and private keys on your machine and let admin know)
```
% ssh -i ~/priv_key ubuntu@10.11.65.4
```

Do some basic sanity checks and updates
```
whoami && uname -a
df -h
sudo apt update && sudo apt upgrade -y
```

Install Python & dependencies
```
sudo apt install -y python3-pip python3-venv git unzip tmux awscli build-essential
python3 -m pip install --upgrade pip
```

Create a virtual python env & Install packages
```
mkdir -p ~/vf && cd ~/vf
python3 -m venv .venv
source .venv/bin/activate
pip install pandas pyarrow fastparquet boto3 awswrangler
```

Install AWS CLI v2 manually to use AWS
```
sudo apt update && sudo apt install -y unzip curl
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip"
unzip -q /tmp/awscliv2.zip -d /tmp
sudo /tmp/aws/install
aws --version
```

Should be able to see something like:
```
ubuntu@ip-10-11-65-4:~$ aws --version aws-cli/2.28.8 Python/3.13.4 Linux/6.8.0-1029-aws exe/x86_64.ubuntu.24
```

Force to use your personal aws profile to gain access:
```
export AWS_PROFILE=jingyao
aws sts get-caller-identity
```

Test aws by:
```
aws s3 ls s3://datahub-redshift-ohio/l2_updates/ --profile jingyao 
```

Should see:
```
2024-10-16 19:19:00 128334864 VM2--AK--2024-10-02.zip 2024-12-04 18:28:52 127601206 VM2--AK--2024-12-03.zip 2024-12-24 21:38:41 130383073 VM2--AK--2024-12-24.zip 2025-01-13 04:27:28 130383130 VM2--AK--2025-01-10.zip
```


### Step 2: Scan all the voter files from L2 and find the ones right after the election

All l2 updates are stored in s3: s3://datahub-redshift-ohio/l2_updates/

Create a script called scan_votehistory_2024_allstates.py in your EC2 console to get a list of files(2 for each state, 1 that first contains 2024 primary and 1 that contains 2024 general)

```
#!/usr/bin/env python3
import os, io, re, sys, json, zipfile, argparse, time
import boto3, pandas as pd

RE_DD  = re.compile(r'(?:^|/)[^/]*VOTEHISTORY[_-]?DataDictionary\.csv$', re.I)
RE_VH  = re.compile(r'(?:^|/)[^/]*VOTEHISTORY[^/]*\.csv$', re.I)
ZIP_DT = re.compile(r'--(\d{4}-\d{2}-\d{2})\.zip$')
DEFAULT_STATES = "AK,AL,AR,AZ,CA,CO,CT,DC,DE,FL,GA,HI,IA,ID,IL,IN,KS,KY,LA,MA,MD,ME,MI,MN,MO,MS,MT,NC,ND,NE,NH,NJ,NM,NV,NY,OH,OK,OR,PA,RI,SC,SD,TN,TX,UT,VA,VT,WA,WI,WV,WY"

def s3_client(profile: str | None):
    return (boto3.Session(profile_name=profile) if profile else boto3.Session()).client("s3")

def list_state_zips(s3, bucket, prefix, st):
    token=None
    while True:
        kw={'Bucket':bucket,'Prefix':prefix}
        if token: kw['ContinuationToken']=token
        r=s3.list_objects_v2(**kw)
        for o in r.get('Contents',[]):
            k=o['Key']
            if k.endswith('.zip') and f'--{st}--' in k:
                yield k
        if not r.get('IsTruncated'): break
        token=r['NextContinuationToken']

def sort_by_zipdate(keys):  # earliest first by filename date
    return sorted(keys, key=lambda k: (ZIP_DT.search(k).group(1) if ZIP_DT.search(k) else '9999-99-99'))

def get_fields_from_zip(s3, bucket, key):
    data = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
    zf = zipfile.ZipFile(io.BytesIO(data))
    # Prefer DataDictionary
    dd = next((n for n in zf.namelist() if RE_DD.search(n)), None)
    if dd:
        with zf.open(dd) as f:
            txt = f.read().decode('utf-8','replace')
        lines = txt.splitlines()
        start = next((i for i,L in enumerate(lines) if 'Field' in L and 'Description' in L), 0)
        df = pd.read_csv(io.StringIO('\n'.join(lines[start:])))
        for col in ('Field','Column','Name'):
            if col in df.columns:
                return [str(x) for x in df[col].dropna()], f'dictionary:{dd}'
    # Fallback: header of largest VOTEHISTORY CSV
    vhs = [inf for inf in zf.infolist() if RE_VH.search(inf.filename) and not inf.filename.lower().endswith('dictionary.csv')]
    if vhs:
        vh = max(vhs, key=lambda x: x.file_size)
        with zf.open(vh) as f:
            head = f.read(128*1024).decode('utf-8','replace')
        try:
            cols = pd.read_csv(io.StringIO(head+'\n'), nrows=0).columns.tolist()
            return [str(c) for c in cols], f'csv_header:{vh.filename}'
        except Exception:
            pass
    return [], 'none'

def pick(cols, prefix):  # 'General' or 'Primary'
    pref = re.compile(rf'^{prefix}_2024', re.I)
    bad  = re.compile(r'(Special|Runoff)', re.I)
    return [c for c in cols if pref.match(c) and not bad.search(c)]

def ensure_txt_header(path):
    if not os.path.exists(path) or os.path.getsize(path)==0:
        with open(path, "w") as f:
            f.write("state\tgeneral_zip\tgeneral_sample_col\tprimary_zip\tprimary_sample_col\tsource\n")

def append_txt(path, state, g, p):
    g_zip  = g['zip']   if g else ""
    g_col  = (g['columns'][0] if g and g.get('columns') else "")
    p_zip  = p['zip']   if p else ""
    p_col  = (p['columns'][0] if p and p.get('columns') else "")
    source = (g.get('source') or p.get('source')) if (g or p) else ""
    with open(path, "a", buffering=1) as f:
        f.write(f"{state}\t{g_zip}\t{g_col}\t{p_zip}\t{p_col}\t{source}\n")

def upload_to_s3(s3, local_path, s3_uri):
    if not s3_uri: return
    assert s3_uri.startswith("s3://")
    _, _, rest = s3_uri.partition("s3://")
    bkt, _, key = rest.partition("/")
    with open(local_path, "rb") as f:
        s3.put_object(Bucket=bkt, Key=key, Body=f.read())

def main():
    ap = argparse.ArgumentParser(description="Scan states for any General_2024*/Primary_2024* columns in VOTEHISTORY; write JSONL + TXT.")
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--profile", default=os.environ.get("AWS_PROFILE"))
    ap.add_argument("--out", default="vf_scan_2024.jsonl")
    ap.add_argument("--txt-out", default="vf_scan_2024.txt")
    ap.add_argument("--s3-out-txt", default="", help="Optional S3 URI for TXT upload at end")
    ap.add_argument("--s3-out-jsonl", default="", help="Optional S3 URI for JSONL upload at end")
    ap.add_argument("--states", default="AK,AL,AR,AZ,CA,CO,CT,DC,DE,FL,GA,HI,IA,ID,IL,IN,KS,KY,LA,MA,MD,ME,MI,MN,MO,MS,MT,NC,ND,NE,NH,NJ,NM,NV,NY,OH,OK,OR,PA,RI,SC,SD,TN,TX,UT,VA,VT,WA,WI,WV,WY")
    args = ap.parse_args()

    s3 = s3_client(args.profile)
    states = [s.strip().upper() for s in args.states.split(",") if s.strip()]

    # Resume: skip states already present in JSONL
    done = set()
    if os.path.exists(args.out):
        with open(args.out, "r") as f:
            for line in f:
                try:
                    rec = json.loads(line); done.add(rec.get("state"))
                except Exception:
                    pass

    log_path = args.out + ".log"
    def log(msg):
        print(msg, flush=True)
        with open(log_path, "a", buffering=1) as lf:
            lf.write(msg + "\n")

    ensure_txt_header(args.txt_out)

    for st in states:
        if st in done:
            log(f"[SKIP] {st} already in {args.out}")
            continue

        log(f"[START] {st}")
        first_general = first_primary = None
        try:
            keys = sort_by_zipdate(list(list_state_zips(s3, args.bucket, args.prefix, st)))
            for key in keys:
                cols, source = get_fields_from_zip(s3, args.bucket, key)
                if not cols: continue
                g = pick(cols, "General")
                p = pick(cols, "Primary")
                if (not first_general) and g:
                    first_general = {"zip": key, "columns": g[:50], "source": source}
                    log(f"[{st}] General_2024 in {key} ({source}) -> sample: {g[:3]}")
                if (not first_primary) and p:
                    first_primary = {"zip": key, "columns": p[:50], "source": source}
                    log(f"[{st}] Primary_2024 in {key} ({source}) -> sample: {p[:3]}")
                if first_general and first_primary:
                    break
        except Exception as e:
            log(f"[ERROR] {st}: {e}")

        rec = {"state": st, "general": first_general, "primary": first_primary, "ts": int(time.time())}
        with open(args.out, "a", buffering=1) as jf:
            jf.write(json.dumps(rec) + "\n")
            jf.flush(); os.fsync(jf.fileno())

        append_txt(args.txt_out, st, first_general, first_primary)
        log(f"[DONE] {st}")

    # Uploads at the end (optional)
    try:
        if args.s3_out_txt:
            upload_to_s3(s3, args.txt_out, args.s3_out_txt)
            log(f"[UPLOAD] TXT -> {args.s3_out_txt}")
        if args.s3_out_jsonl:
            upload_to_s3(s3, args.out, args.s3_out_jsonl)
            log(f"[UPLOAD] JSONL -> {args.s3_out_jsonl}")
    except Exception as e:
        log(f"[UPLOAD-ERROR] {e}")

    log("[ALL DONE]")

if __name__ == "__main__":
    main()
PY

chmod +x scan_votehistory_2024_allstates.py
```

Run the following that tells the script to run in tmux and upload both TXT + JSONL to S3 when done:
```
tmux new -s vfscan
export AWS_PROFILE=jingyao
./scan_votehistory_2024_allstates.py \
  --bucket datahub-redshift-ohio \
  --prefix l2_updates/ \
  --out vf_scan_2024.jsonl \
  --txt-out vf_scan_2024.txt \
  --s3-out-txt s3://datahub-redshift-ohio/reports/vf_scan_2024.txt \
  --s3-out-jsonl s3://datahub-redshift-ohio/reports/vf_scan_2024.jsonl
```

You can check progress / results while the script is running:
```
tail -n 20 vf_scan_2024.jsonl.log
tail -n 10 vf_scan_2024.txt
wc -l vf_scan_2024.txt
```
When finished, confirm uploads by:
```
aws s3 ls s3://datahub-redshift-ohio/reports/ --profile jingyao
aws s3 cp s3://datahub-redshift-ohio/reports/vf_scan_2024.txt - --profile jingyao | head
```
If you open the txt file, it should look like:
```
{"state": "AK", "general": {"zip": "l2_updates/VM2--AK--2024-12-24.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--AK--2024-12-24-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--AK--2024-10-02.zip", "columns": ["Primary_2024_08_20"], "source": "dictionary:VM2--AK--2024-10-02-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755068050}
{"state": "AL", "general": {"zip": "l2_updates/VM2--AL--2025-01-18.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--AL--2025-01-18-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--AL--2024-10-02.zip", "columns": ["Primary_2024_03_05"], "source": "dictionary:VM2--AL--2024-10-02-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755068161}
{"state": "AR", "general": {"zip": "l2_updates/VM2--AR--2025-02-06.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--AR--2025-02-06-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--AR--2024-10-10.zip", "columns": ["Primary_2024_03_05"], "source": "dictionary:VM2--AR--2024-10-10-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755068198}
{"state": "AZ", "general": {"zip": "l2_updates/VM2--AZ--2024-12-27.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--AZ--2024-12-27-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--AZ--2024-10-02.zip", "columns": ["Primary_2024_07_30"], "source": "dictionary:VM2--AZ--2024-10-02-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755068289}
{"state": "CA", "general": {"zip": "l2_updates/VM2--CA--2025-01-06.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--CA--2025-01-06-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--CA--2024-10-06.zip", "columns": ["Primary_2024_03_05"], "source": "dictionary:VM2--CA--2024-10-06-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755105420}
{"state": "CO", "general": {"zip": "l2_updates/VM2--CO--2025-01-15.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--CO--2025-01-15-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--CO--2024-10-05.zip", "columns": ["Primary_2024_06_25"], "source": "dictionary:VM2--CO--2024-10-05-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755105579}
{"state": "CT", "general": {"zip": "l2_updates/VM2--CT--2025-03-14.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--CT--2025-03-14-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--CT--2025-03-14.zip", "columns": ["Primary_2024_08_13"], "source": "dictionary:VM2--CT--2025-03-14-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755105712}
{"state": "DC", "general": {"zip": "l2_updates/VM2--DC--2025-01-29.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--DC--2025-01-29-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--DC--2024-10-03.zip", "columns": ["Primary_2024_06_04"], "source": "dictionary:VM2--DC--2024-10-03-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755105726}
{"state": "DE", "general": {"zip": "l2_updates/VM2--DE--2024-12-17.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--DE--2024-12-17-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--DE--2024-12-17.zip", "columns": ["Primary_2024_09_10"], "source": "dictionary:VM2--DE--2024-12-17-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755105745}
{"state": "FL", "general": {"zip": "l2_updates/VM2--FL--2025-02-11.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--FL--2025-02-11-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--FL--2024-10-18.zip", "columns": ["Primary_2024_08_20"], "source": "dictionary:VM2--FL--2024-10-18-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755106389}
{"state": "GA", "general": {"zip": "l2_updates/VM2--GA--2024-12-24.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--GA--2024-12-24-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--GA--2024-10-03.zip", "columns": ["Primary_2024_05_21"], "source": "dictionary:VM2--GA--2024-10-03-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755106548}
{"state": "HI", "general": {"zip": "l2_updates/VM2--HI--2025-01-29.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--HI--2025-01-29-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--HI--2024-10-02.zip", "columns": ["Primary_2024_08_10"], "source": "dictionary:VM2--HI--2024-10-02-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755106577}
{"state": "IA", "general": {"zip": "l2_updates/VM2--IA--2025-02-03.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--IA--2025-02-03-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--IA--2024-10-03.zip", "columns": ["Primary_2024_06_04"], "source": "dictionary:VM2--IA--2024-10-03-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755106670}
{"state": "ID", "general": {"zip": "l2_updates/VM2--ID--2025-03-26.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--ID--2025-03-26-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--ID--2025-03-27.zip", "columns": ["Primary_2024_05_21"], "source": "dictionary:VM2--ID--2025-03-27-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755106736}
{"state": "IL", "general": {"zip": "l2_updates/VM2--IL--2025-02-27.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--IL--2025-02-27-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--IL--2024-10-11.zip", "columns": ["Primary_2024_03_19"], "source": "dictionary:VM2--IL--2024-10-11-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755107137}
{"state": "IN", "general": {"zip": "l2_updates/VM2--IN--2025-04-07.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--IN--2025-04-07-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--IN--2024-10-17.zip", "columns": ["Primary_2024_05_07"], "source": "dictionary:VM2--IN--2024-10-17-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755107424}
{"state": "KS", "general": {"zip": "l2_updates/VM2--KS--2025-04-02.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--KS--2025-04-02-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--KS--2024-10-22.zip", "columns": ["Primary_2024_08_06"], "source": "dictionary:VM2--KS--2024-10-22-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755107536}
{"state": "KY", "general": {"zip": "l2_updates/VM2--KY--2025-04-22.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--KY--2025-04-22-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--KY--2025-02-25.zip", "columns": ["Primary_2024_05_21"], "source": "dictionary:VM2--KY--2025-02-25-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755107723}
{"state": "LA", "general": {"zip": "l2_updates/VM2--LA--2024-12-16.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--LA--2024-12-16-VOTEHISTORY_DataDictionary.csv"}, "primary": null, "ts": 1755108038}
{"state": "MA", "general": {"zip": "l2_updates/VM2--MA--2025-02-26.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--MA--2025-02-26-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--MA--2024-10-17.zip", "columns": ["Primary_2024_09_03"], "source": "dictionary:VM2--MA--2024-10-17-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755108326}
{"state": "MD", "general": {"zip": "l2_updates/VM2--MD--2025-01-21.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--MD--2025-01-21-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--MD--2024-10-04.zip", "columns": ["Primary_2024_05_14"], "source": "dictionary:VM2--MD--2024-10-04-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755108521}
{"state": "ME", "general": {"zip": "l2_updates/VM2--ME--2025-03-05.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--ME--2025-03-05-VOTEHISTORY_DataDictionary.csv"}, "primary": null, "ts": 1755108609}
{"state": "MI", "general": {"zip": "l2_updates/VM2--MI--2025-01-24.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--MI--2025-01-24-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--MI--2024-10-02.zip", "columns": ["Primary_2024_08_06"], "source": "dictionary:VM2--MI--2024-10-02-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755109000}
{"state": "MN", "general": {"zip": "l2_updates/VM2--MN--2025-03-04.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--MN--2025-03-04-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--MN--2024-01-10.zip", "columns": ["Primary_2024_08_13"], "source": "dictionary:VM2--MN--2024-01-10-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755109286}
{"state": "MO", "general": {"zip": "l2_updates/VM2--MO--2025-02-12.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--MO--2025-02-12-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--MO--2024-10-24.zip", "columns": ["Primary_2024_08_06"], "source": "dictionary:VM2--MO--2024-10-24-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755109546}
{"state": "MS", "general": {"zip": "l2_updates/VM2--MS--2025-01-24.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--MS--2025-01-24-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--MS--2024-10-03.zip", "columns": ["Primary_2024_03_12"], "source": "dictionary:VM2--MS--2024-10-03-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755109625}
{"state": "MT", "general": {"zip": "l2_updates/VM2--MT--2025-02-13.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--MT--2025-02-13-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--MT--2024-10-04.zip", "columns": ["Primary_2024_06_04"], "source": "dictionary:VM2--MT--2024-10-04-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755109683}
{"state": "NC", "general": {"zip": "l2_updates/VM2--NC--2024-12-31.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--NC--2024-12-31-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--NC--2024-10-06.zip", "columns": ["Primary_2024_03_05"], "source": "dictionary:VM2--NC--2024-10-06-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755109916}
{"state": "ND", "general": {"zip": "l2_updates/VM2--ND--2025-02-28.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--ND--2025-02-28-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--ND--2024-10-02.zip", "columns": ["Primary_2024_06_11"], "source": "dictionary:VM2--ND--2024-10-02-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755109942}
{"state": "NE", "general": {"zip": "l2_updates/VM2--NE--2024-12-17.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--NE--2024-12-17-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--NE--2024-10-04.zip", "columns": ["Primary_2024_05_14"], "source": "dictionary:VM2--NE--2024-10-04-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755109985}
{"state": "NH", "general": {"zip": "l2_updates/VM2--NH--2025-04-21.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--NH--2025-04-21-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--NH--2024-10-14.zip", "columns": ["Primary_2024_09_10"], "source": "dictionary:VM2--NH--2024-10-14-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755110047}
{"state": "NJ", "general": {"zip": "l2_updates/VM2--NJ--2025-01-18.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--NJ--2025-01-18-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--NJ--2024-10-15.zip", "columns": ["Primary_2024_06_04"], "source": "dictionary:VM2--NJ--2024-10-15-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755110840}
{"state": "NM", "general": {"zip": "l2_updates/VM2--NM--2025-04-09.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--NM--2025-04-09-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--NM--2024-10-04.zip", "columns": ["Primary_2024_06_04"], "source": "dictionary:VM2--NM--2024-10-04-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755110887}
{"state": "NV", "general": {"zip": "l2_updates/VM2--NV--2025-01-10.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--NV--2025-01-10-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--NV--2024-10-01.zip", "columns": ["Primary_2024_06_11"], "source": "dictionary:VM2--NV--2024-10-01-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755110948}
{"state": "NY", "general": {"zip": "l2_updates/VM2--NY--2025-03-26.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--NY--2025-03-26-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--NY--2024-10-15.zip", "columns": ["Primary_2024_06_25"], "source": "dictionary:VM2--NY--2024-10-15-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755113308}
{"state": "OH", "general": {"zip": "l2_updates/VM2--OH--2024-01-29.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--OH--2024-01-29-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--OH--2024-01-29.zip", "columns": ["Primary_2024_03_19"], "source": "dictionary:VM2--OH--2024-01-29-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755113365}
{"state": "OK", "general": {"zip": "l2_updates/VM2--OK--2025-01-15.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--OK--2025-01-15-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--OK--2024-10-04.zip", "columns": ["Primary_2024_06_18"], "source": "dictionary:VM2--OK--2024-10-04-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755113428}
{"state": "OR", "general": {"zip": "l2_updates/VM2--OR--2025-01-23.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--OR--2025-01-23-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--OR--2024-10-07.zip", "columns": ["Primary_2024_05_21"], "source": "dictionary:VM2--OR--2024-10-07-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755113564}
{"state": "PA", "general": {"zip": "l2_updates/VM2--PA--2025-02-27.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--PA--2025-02-27-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--PA--2024-10-15.zip", "columns": ["Primary_2024_04_23"], "source": "dictionary:VM2--PA--2024-10-15-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755113951}
{"state": "RI", "general": {"zip": "l2_updates/VM2--RI--2025-01-06.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--RI--2025-01-06-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--RI--2025-01-06.zip", "columns": ["Primary_2024_09_10"], "source": "dictionary:VM2--RI--2025-01-06-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755113964}
{"state": "SC", "general": {"zip": "l2_updates/VM2--SC--2025-02-26.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--SC--2025-02-26-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--SC--2024-10-04.zip", "columns": ["Primary_2024_06_11"], "source": "dictionary:VM2--SC--2024-10-04-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755114094}
{"state": "SD", "general": {"zip": "l2_updates/VM2--SD--2025-01-19.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--SD--2025-01-19-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--SD--2024-10-07.zip", "columns": ["Primary_2024_06_04"], "source": "dictionary:VM2--SD--2024-10-07-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755114112}
{"state": "TN", "general": {"zip": "l2_updates/VM2--TN--2025-02-25.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--TN--2025-02-25-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--TN--2025-02-25.zip", "columns": ["Primary_2024_08_01"], "source": "dictionary:VM2--TN--2025-02-25-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755114302}
{"state": "TX", "general": {"zip": "l2_updates/VM2--TX--2025-02-14.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--TX--2025-02-14-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--TX--2024-10-07.zip", "columns": ["Primary_2024_03_05"], "source": "dictionary:VM2--TX--2024-10-07-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755114828}
{"state": "UT", "general": {"zip": "l2_updates/VM2--UT--2025-02-11.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--UT--2025-02-11-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--UT--2024-10-04.zip", "columns": ["Primary_2024_06_25"], "source": "dictionary:VM2--UT--2024-10-04-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755114875}
{"state": "VA", "general": {"zip": "l2_updates/VM2--VA--2025-02-13.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--VA--2025-02-13-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--VA--2024-10-03.zip", "columns": ["Primary_2024_06_18"], "source": "dictionary:VM2--VA--2024-10-03-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755115070}
{"state": "VT", "general": {"zip": "l2_updates/VM2--VT--2025-01-28.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--VT--2025-01-28-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--VT--2024-10-05.zip", "columns": ["Primary_2024_08_13"], "source": "dictionary:VM2--VT--2024-10-05-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755115084}
{"state": "WA", "general": {"zip": "l2_updates/VM2--WA--2025-02-04.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--WA--2025-02-04-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--WA--2024-10-07.zip", "columns": ["Primary_2024_08_06"], "source": "dictionary:VM2--WA--2024-10-07-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755115396}
{"state": "WI", "general": {"zip": "l2_updates/VM2--WI--2025-02-14.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--WI--2025-02-14-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--WI--2024-10-02.zip", "columns": ["Primary_2024_08_13"], "source": "dictionary:VM2--WI--2024-10-02-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755115629}
{"state": "WV", "general": {"zip": "l2_updates/VM2--WV--2025-01-28.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--WV--2025-01-28-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--WV--2024-10-17.zip", "columns": ["Primary_2024_05_14"], "source": "dictionary:VM2--WV--2024-10-17-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755115678}
{"state": "WY", "general": {"zip": "l2_updates/VM2--WY--2025-03-06.zip", "columns": ["General_2024_11_05"], "source": "dictionary:VM2--WY--2025-03-06-VOTEHISTORY_DataDictionary.csv"}, "primary": {"zip": "l2_updates/VM2--WY--2025-03-06.zip", "columns": ["Primary_2024_08_20"], "source": "dictionary:VM2--WY--2025-03-06-VOTEHISTORY_DataDictionary.csv"}, "ts": 1755115691}
```
 {"zip": "l2_updates/VM2--AK--2024-10-02.zip", "columns": ["Primary_2024_08_20"] means that the first update that contains the Aug 20 primary election in Alaska is in the file l2_updates/VM2--AK--2024-10-02.zip
### Step 3: Process the datasets into a parquet format and store in s3 so that redshift can read from there:
Why are we using parquet:  
- A Parquet file is a columnar storage format designed for efficient data processing, especially in big data environments. Compared to CSV or JSON, Parquet:
- Stores data column-by-column rather than row-by-row.
- Compresses better because similar data is stored together (e.g., all values for a column are next to each other).
- Reads faster for queries that only need a few columns, because it doesn’t need to read the entire row.
- Keeps schema metadata (column names, types, etc.) inside the file.

Open a text editor in the EC2 console(I use nano):
```
nano process_demo_to_parquet.py
```
Paste the code, Press Ctrl+O → Enter (to save) and Press Ctrl+X (to exit).
Make it executable by 
```
chmod +x process_demo_to_parquet.py
```

##### Code for process_demo_to_parquet.py:  
```
#!/usr/bin/env python3
"""
Extract DEMOGRAPHIC (voter master) from an L2 ZIP on S3 and write Parquet to S3.

Example:
  export AWS_PROFILE=jingyao
  python process_demo_to_parquet.py \
    --bucket datahub-redshift-ohio \
    --key l2_updates/VM2--AK--2024-12-24.zip \
    --out-prefix parquet/demo

Output:
  s3://<bucket>/<out-prefix>/state=<STATE>/pull_date=<YYYY-MM-DD>/demo.parquet
"""
import os, io, re, sys, json, argparse, tempfile, zipfile, csv
from typing import Optional, Tuple, List

import boto3, pandas as pd
import pyarrow as pa, pyarrow.parquet as pq

# Accept .csv/.txt/.tsv/.tab/.psv and tolerate spaces/underscores; ignore DataDictionary
RE_DEMO_PRIORITIZED = [
    re.compile(r'(?i)(?:^|/)[^/]*?(?:DEMOGRAPHIC|DEMO|VOTER(?:FILE|_FILE|_MASTER)?)'
              r'[^/]*\.(csv|txt|tsv|tab|psv)$'),
]
RE_ANY_TEXT = re.compile(r'(?i)\.(csv|txt|tsv|tab|psv)$')
ZIP_DATE = re.compile(r'--([A-Z]{2})--(\d{4}-\d{2}-\d{2})\.zip$')

DEFAULT_NEEDED = [
    "lalvoterid","file_state","voters_calculatedregdate","voters_gender",
    "parties_description","EthnicGroups_EthnicGroup1Desc","ethnic_description",
    "languages_description","AbsenteeTypes_Description",
    "CommercialData_EstimatedHHIncomeAmount","county"
]

def s3c(profile: Optional[str]):
    return (boto3.Session(profile_name=profile) if profile else boto3.Session()).client("s3")

def parse_zip_key_state_date(key: str) -> Tuple[str,str]:
    m = ZIP_DATE.search(key)
    return (m.group(1) if m else "XX", m.group(2) if m else "unknown")

def choose_demo_member(zf: zipfile.ZipFile) -> Optional[zipfile.ZipInfo]:
    infos = [i for i in zf.infolist()
             if RE_ANY_TEXT.search(i.filename or "")
             and "dictionary" not in i.filename.lower()]
    # First pass: prioritized pattern
    pri = []
    for rx in RE_DEMO_PRIORITIZED:
        pri += [i for i in infos if rx.search(i.filename)]
    if pri:
        return max(pri, key=lambda x: x.file_size)
    # Fallback: largest text-like file that contains 'lalvoterid' in header
    best = None; best_size = -1
    for inf in infos:
        try:
            with zf.open(inf, "r") as fh:
                head = fh.read(128*1024).decode("utf-8","replace")
            cols = [c.strip() for c in head.splitlines()[0].split(",")]
            if any(c.lower()=="lalvoterid" for c in cols):
                if inf.file_size > best_size:
                    best, best_size = inf, inf.file_size
        except Exception:
            pass
    return best

def sniff_sep(member_name: str, head_sample: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(head_sample, delimiters=",\t|;")
        return dialect.delimiter
    except Exception:
        low = member_name.lower()
        if low.endswith((".tab",".tsv")): return "\t"
        if low.endswith(".psv"): return "|"
        return ","

def write_parquet_streaming(zf, member, local_out, needed: List[str], chunksize=400_000) -> int:
    with zf.open(member, "r") as fh_head:
        sample = fh_head.read(65536).decode("utf-8","replace")
    sep = sniff_sep(member.filename, sample)

    writer = None
    total = 0
    with zf.open(member, "r") as fh:
        text = io.TextIOWrapper(fh, encoding="utf-8", errors="replace", newline="")
        for chunk in pd.read_csv(text, dtype=str, chunksize=chunksize, low_memory=False, sep=sep):
            keep = [c for c in needed if c in chunk.columns]
            if keep: chunk = chunk[keep]
            if "file_state" in chunk.columns:
                chunk["file_state"] = chunk["file_state"].astype(str).str.upper().str.strip()
            if "voters_gender" in chunk.columns:
                chunk["voters_gender"] = chunk["voters_gender"].astype(str).str.strip()

            tbl = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(local_out, tbl.schema, compression="zstd")
            writer.write_table(tbl)
            total += len(chunk)
    if writer: writer.close()
    return total

def process_one_zip(s3, bucket, key, out_prefix, needed: List[str], overwrite=True) -> str:
    state, pull_date = parse_zip_key_state_date(key)
    dest_prefix = f"{out_prefix.rstrip('/')}/state={state}/pull_date={pull_date}"
    out_key = f"{dest_prefix}/demo.parquet"
    if not overwrite:
        try:
            s3.head_object(Bucket=bucket, Key=out_key)
            print(f"[SKIP] exists: s3://{bucket}/{out_key}")
            return out_key
        except Exception:
            pass

    print(f"==> DEMO {state} | {key} | pull_date={pull_date}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    zf = zipfile.ZipFile(io.BytesIO(obj["Body"].read()))
    member = choose_demo_member(zf)
    if not member:
        raise RuntimeError(f"No DEMOGRAPHIC file found in ZIP: s3://{bucket}/{key}")

    with tempfile.TemporaryDirectory() as td:
        local_out = os.path.join(td, f"{state}_{pull_date}_demo.parquet")
        rows = write_parquet_streaming(zf, member, local_out, needed)
        print(f"[LOCAL] {rows:,} rows -> {local_out}")
        s3.upload_file(local_out, bucket, out_key)
        print(f"[S3] uploaded -> s3://{bucket}/{out_key}")
    return out_key

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--key", required=True, help="ZIP key like l2_updates/VM2--AK--YYYY-MM-DD.zip")
    ap.add_argument("--out-prefix", default="parquet/demo")
    ap.add_argument("--profile", default=os.environ.get("AWS_PROFILE"))
    ap.add_argument("--needed-cols", default=",".join(DEFAULT_NEEDED))
    ap.add_argument("--no-overwrite", action="store_true")
    args = ap.parse_args()

    needed = [c.strip() for c in args.needed_cols.split(",") if c.strip()]
    s3 = s3c(args.profile)
    try:
        out = process_one_zip(s3, args.bucket, args.key, args.out_prefix, needed, overwrite=(not args.no_overwrite))
        print(json.dumps({"ok": True, "parquet_s3": f"s3://{args.bucket}/{out}"}))
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}))
        sys.exit(1)

if __name__ == "__main__":
    main()

```

#### Code for process_votehistory_to_parquet.py:  
```
#!/usr/bin/env python3
"""
Convert L2 VOTEHISTORY inside an S3 ZIP to Parquet on S3.

Usage (direct):
  export AWS_PROFILE=jingyao
  python process_votehistory_to_parquet.py \
    --bucket datahub-redshift-ohio \
    --key l2_updates/VM2--AK--2024-10-02.zip \
    --election primary \
    --flag Primary_2024_08_20 \
    --out-prefix parquet/votehistory

Usage (from finder JSONL; processes primary+general for chosen states):
  python process_votehistory_to_parquet.py \
    --bucket datahub-redshift-ohio \
    --out-prefix parquet/votehistory \
    --results vf_scan_2024.jsonl \
    --states AK,AL
"""
import os
import io
import re
import json
import csv
import argparse
import tempfile
import zipfile
from typing import Optional, Tuple, List

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# --- patterns & helpers ---
RE_VH = re.compile(r'(?i)(?:^|/)[^/]*vote[\s._-]*history[^/]*\.(csv|txt|tsv|tab|psv)$')
RE_ANY_TEXT = re.compile(r'(?i)\.(csv|txt|tsv|tab|psv)$')
ZIP_DATE = re.compile(r'--([A-Z]{2})--(\d{4}-\d{2}-\d{2})\.zip$')

def s3c(profile: Optional[str] = None):
    return (boto3.Session(profile_name=profile) if profile else boto3.Session()).client("s3")

def parse_zip_key_state_date(key: str) -> Tuple[str, str]:
    m = ZIP_DATE.search(key)
    return (m.group(1) if m else "XX", m.group(2) if m else "unknown")

def largest_votehistory_member(zf: zipfile.ZipFile) -> Optional[zipfile.ZipInfo]:
    # Prefer files matching VOTEHISTORY pattern (ignore any *DataDictionary*)
    cands = [inf for inf in zf.infolist()
             if RE_VH.search(inf.filename)
             and "dictionary" not in inf.filename.lower()]
    if cands:
        return max(cands, key=lambda x: x.file_size)
    # Fallback: largest text-like file excluding dictionaries
    txtish = [inf for inf in zf.infolist()
              if RE_ANY_TEXT.search(inf.filename)
              and "dictionary" not in inf.filename.lower()]
    return max(txtish, key=lambda x: x.file_size) if txtish else None

def sniff_sep(member_name: str, head_sample: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(head_sample, delimiters=",\t|;")
        return dialect.delimiter
    except Exception:
        low = member_name.lower()
        if low.endswith((".tab", ".tsv")): return "\t"
        if low.endswith(".psv"): return "|"
        return ","

# --- core streaming writer with schema freezing (all-string) ---
def write_parquet_streaming(
    zf: zipfile.ZipFile,
    member: zipfile.ZipInfo,
    local_out: str,
    election_flag: Optional[str],
    chunksize: int = 400_000
) -> Tuple[int, int]:
    # sniff delimiter
    with zf.open(member, "r") as fh_head:
        sample = fh_head.read(65536).decode("utf-8", "replace")
    sep = sniff_sep(member.filename, sample)

    writer: Optional[pq.ParquetWriter] = None
    total = 0
    ones = 0
    all_cols: Optional[List[str]] = None
    schema: Optional[pa.Schema] = None

    with zf.open(member, "r") as fh:
        text = io.TextIOWrapper(fh, encoding="utf-8", errors="replace", newline="")
        reader = pd.read_csv(text, dtype=str, chunksize=chunksize, low_memory=False, sep=sep)

        for chunk in reader:
            if all_cols is None:
                all_cols = list(chunk.columns)
                schema = pa.schema([pa.field(c, pa.string()) for c in all_cols])

            # align to first-chunk columns (add missing, drop extras), keep order
            missing = [c for c in all_cols if c not in chunk.columns]
            for c in missing:
                chunk[c] = pd.NA
            extras = [c for c in chunk.columns if c not in all_cols]
            if extras:
                chunk = chunk.drop(columns=extras)
            chunk = chunk[all_cols]

            # normalize a couple of common fields if present (optional)
            for col in ("state", "STATE"):
                if col in chunk.columns:
                    chunk[col] = chunk[col].astype("string").str.upper().str.strip()
            for col in ("county", "COUNTY"):
                if col in chunk.columns:
                    chunk[col] = chunk[col].astype("string").str.upper().str.strip()

            # count election flag (treat '1' or 'Y' as voted)
            if election_flag and election_flag in chunk.columns:
                ones += chunk[election_flag].fillna("0").astype("string").str.upper().isin(["1", "Y"]).sum()

            # cast everything to string so we never get null-vs-string schema flips
            chunk = chunk.astype("string")

            tbl = pa.Table.from_pandas(chunk, preserve_index=False, schema=schema, safe=False)
            if writer is None:
                writer = pq.ParquetWriter(local_out, schema, compression="zstd")
            writer.write_table(tbl)
            total += len(chunk)

    if writer:
        writer.close()
    return int(total), int(ones)

# --- one-zip processor ---
def process_one_zip(
    s3,
    bucket: str,
    key: str,
    out_prefix: str,
    election_type: str,
    election_flag: Optional[str],
    overwrite: bool = True
) -> Tuple[str, str]:
    state, pull_date = parse_zip_key_state_date(key)
    base = f"{out_prefix.rstrip('/')}/state={state}/pull_date={pull_date}/election={election_type}"
    out_parquet = f"{base}/votehistory.parquet"
    out_stats   = f"{base}/_stats.json"

    if not overwrite:
        try:
            s3.head_object(Bucket=bucket, Key=out_parquet)
            print(f"[SKIP] exists: s3://{bucket}/{out_parquet}")
            return out_parquet, out_stats
        except Exception:
            pass

    print(f"==> {state} {election_type} | {key} | pull_date={pull_date} | flag={election_flag or 'n/a'}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    zf = zipfile.ZipFile(io.BytesIO(obj["Body"].read()))
    member = largest_votehistory_member(zf)
    if not member:
        raise RuntimeError(f"No VOTEHISTORY file found in ZIP: s3://{bucket}/{key}")

    with tempfile.TemporaryDirectory() as td:
        local_out = os.path.join(td, f"{state}_{pull_date}_{election_type}.parquet")
        rows, ones = write_parquet_streaming(zf, member, local_out, election_flag=election_flag)
        print(f"[LOCAL] {rows:,} rows -> {local_out}")
        s3.upload_file(local_out, bucket, out_parquet)
        print(f"[S3] uploaded parquet -> s3://{bucket}/{out_parquet}")

    stats = {
        "state": state,
        "pull_date": pull_date,
        "election": election_type,
        "flag_field": election_flag,
        "rows": int(rows),
        "flag_ones": int(ones),
    }
    s3.put_object(Bucket=bucket, Key=out_stats, Body=json.dumps(stats).encode("utf-8"))
    print(f"[S3] uploaded stats -> s3://{bucket}/{out_stats}")
    return out_parquet, out_stats

# --- runner modes ---
def run_from_results(args):
    s3 = s3c(args.profile)
    wanted = set(s.strip().upper() for s in args.states.split(",")) if args.states else None
    with open(args.results) as f:
        for line in f:
            rec = json.loads(line)
            st = (rec.get("state") or "").upper()
            if not st or (wanted and st not in wanted):
                continue

            # primary
            p = rec.get("primary")
            if p and p.get("zip"):
                process_one_zip(
                    s3, args.bucket, p["zip"], args.out_prefix,
                    election_type="primary",
                    election_flag=(p.get("columns") or [None])[0],
                    overwrite=(not args.no_overwrite),
                )

            # general
            g = rec.get("general")
            if g and g.get("zip"):
                process_one_zip(
                    s3, args.bucket, g["zip"], args.out_prefix,
                    election_type="general",
                    election_flag=(g.get("columns") or [None])[0],
                    overwrite=(not args.no_overwrite),
                )
    print("[DONE] from results")

def run_direct(args):
    s3 = s3c(args.profile)
    assert args.key and args.election, "--key and --election are required in direct mode"
    process_one_zip(
        s3, args.bucket, args.key, args.out_prefix,
        election_type=args.election,
        election_flag=args.flag,
        overwrite=(not args.no_overwrite),
    )

# --- cli ---
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--out-prefix", default="parquet/votehistory")
    ap.add_argument("--profile", default=os.environ.get("AWS_PROFILE"))

    # Mode A: from results JSONL
    ap.add_argument("--results", help="finder JSONL (e.g., vf_scan_2024.jsonl)")
    ap.add_argument("--states", help="Comma-separated states to include (e.g., AK,AL)")

    # Mode B: direct
    ap.add_argument("--key", help="S3 key to a ZIP")
    ap.add_argument("--election", help="e.g., general|primary")
    ap.add_argument("--flag", help="Election flag column (e.g., General_2024_11_05)")

    ap.add_argument("--no-overwrite", action="store_true")
    args = ap.parse_args()

    if args.results:
        run_from_results(args)
    else:
        run_direct(args)

if __name__ == "__main__":
    main()

```

#### Test the code for 1 state Alaksa first
Convert Alaska ZIPs → Parquet on S3(Alaska):  
Voter file(demographics):
```
export AWS_PROFILE=jingyao
# DEMO from the Dec 24 pull (general)
./process_demo_to_parquet.py \
  --bucket datahub-redshift-ohio \
  --key l2_updates/VM2--AK--2024-12-24.zip \
  --out-prefix parquet/demo
# DEMO from the Oct 02 pull (primary)
./process_demo_to_parquet.py \
  --bucket datahub-redshift-ohio \
  --key l2_updates/VM2--AK--2024-10-02.zip \
  --out-prefix parquet/demo
```

which should yield:
```
s3://datahub-redshift-ohio/parquet/demo/state=AK/pull_date=2024-12-24/demo.parquet
s3://datahub-redshift-ohio/parquet/demo/state=AK/pull_date=2024-10-02/demo.parquet
```

VOTE HISTORY (primary + general):  
```
# This reads your vf_scan_2024.jsonl and writes both primary+general partitions for AK
./process_votehistory_to_parquet.py \
  --bucket datahub-redshift-ohio \
  --out-prefix parquet/votehistory \
  --results vf_scan_2024.jsonl \
  --states AK
```
which yields:
```
s3://datahub-redshift-ohio/parquet/votehistory/state=AK/pull_date=2024-12-24/election=general/votehistory.parquet
s3://datahub-redshift-ohio/parquet/votehistory/state=AK/pull_date=2024-10-02/election=primary/votehistory.parquet
```

### Step 4: Do some testing to make sure the voter file and vote history tables contain the columns we want  

Run this script, change path1, path2 to this year's table and previous year's, it will display all columns and diffs
```
# %% [markdown]
# # Parquet Schema Comparison
# Comparing schemas between two L2 voter file parquet files

# %%
import pyarrow.parquet as pq
import s3fs
import pandas as pd
from IPython.display import display, HTML


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


# %%
def get_schema_dict(parquet_path, s3_fs):
    """Get schema as a dictionary of field_name: field_type"""
    try:
        with s3_fs.open(parquet_path, 'rb') as f:
            parquet_file = pq.ParquetFile(f)
            schema = parquet_file.schema_arrow
        
        # Convert to dictionary for easy comparison
        schema_dict = {field.name: str(field.type) for field in schema}
        return schema_dict, None
    except Exception as e:
        return None, str(e)

# %%
# CONFIG - File paths
path1 = "s3://datahub-redshift-ohio/parquet/votehistory/state=AK/pull_date=2024-12-24/election=general/0000_part_00.parquet"
path2 = "s3://datahub-redshift-ohio/l2_files/ky_la_ms_nj_va_vf_and_vh_20240823/vh_spectrum_parquet/ky/0000_part_00.parquet"

print(" Parquet Schema Comparison")
print("="*80)
print(f"File 1 (2024): {path1}")
print(f"File 2 (2022): {path2}")
print("="*80)

# %%
# Read schemas
s3 = s3fs.S3FileSystem()
print("Reading schemas...")

schema1, error1 = get_schema_dict(path1, s3)
schema2, error2 = get_schema_dict(path2, s3)

# Convert File 1 field names to lowercase for comparison
if schema1:
    schema1 = {field_name.lower(): field_type for field_name, field_type in schema1.items()}
    print(" Converted File 1 field names to lowercase")

if error1:
    print(f" Error reading File 1: {error1}")
if error2:
    print(f" Error reading File 2: {error2}")

if schema1 and schema2:
    print(f" File 1 has {len(schema1)} fields")
    print(f" File 2 has {len(schema2)} fields")

# %%
# Create side-by-side comparison of all fields
def create_side_by_side_comparison(schema1, schema2, max_rows=None):
    """Create a side-by-side DataFrame comparison of schemas"""
    
    # Convert to lists and pad to same length
    fields1 = [(name, dtype) for name, dtype in schema1.items()]
    fields2 = [(name, dtype) for name, dtype in schema2.items()]
    
    # Sort both for easier comparison
    fields1.sort()
    fields2.sort()
    
    max_len = max(len(fields1), len(fields2))
    
    # Pad shorter list with empty tuples
    while len(fields1) < max_len:
        fields1.append(("", ""))
    while len(fields2) < max_len:
        fields2.append(("", ""))
    
    # Limit rows if specified
    if max_rows:
        fields1 = fields1[:max_rows]
        fields2 = fields2[:max_rows]
        max_len = max_rows
    
    # Create DataFrame
    comparison_df = pd.DataFrame({
        'File 1 (2024) - Field Name': [f[0] for f in fields1],
        'File 1 (2024) - Type': [f[1] for f in fields1],
        'File 2 (2022) - Field Name': [f[0] for f in fields2],
        'File 2 (2022) - Type': [f[1] for f in fields2],
    })
    
    return comparison_df

# Show first 50 fields side by side
print(" Side-by-Side Field Comparison")
comparison_df = create_side_by_side_comparison(schema1, schema2, max_rows=5000)
display(comparison_df)

# %%
# Analysis of differences
fields1 = set(schema1.keys())
fields2 = set(schema2.keys())

common_fields = fields1 & fields2
only_in_1 = fields1 - fields2
only_in_2 = fields2 - fields1

print(" SCHEMA ANALYSIS")
print("="*50)
print(f"Total fields in File 1: {len(fields1)}")
print(f"Total fields in File 2: {len(fields2)}")
print(f"Common fields: {len(common_fields)}")
print(f"Fields only in File 1: {len(only_in_1)}")
print(f"Fields only in File 2: {len(only_in_2)}")

# %%
# Show field patterns analysis
def analyze_field_patterns(schema_dict, file_name):
    """Analyze common patterns in field names"""
    fields = list(schema_dict.keys())
    
    print(f"\n Field Pattern Analysis for {file_name}")
    print("-" * 40)
    
    # Look for common prefixes
    prefixes = {}
    for field in fields:
        if '_' in field:
            prefix = field.split('_')[0]
            prefixes[prefix] = prefixes.get(prefix, 0) + 1
    
    print("Common prefixes (top 10):")
    for prefix, count in sorted(prefixes.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {prefix}_*: {count} fields")
    
    # Show first 20 fields
    print(f"\nFirst 20 fields:")
    for i, field in enumerate(fields[:20], 1):
        print(f"  {i:2d}. {field}")
    
    if len(fields) > 20:
        print(f"  ... and {len(fields) - 20} more fields")

analyze_field_patterns(schema1, "File 1 (2024)")
analyze_field_patterns(schema2, "File 2 (2022)")

# %%
# Create searchable comparison table for all fields
print(" Complete Field Comparison (All Fields)")
full_comparison_df = create_side_by_side_comparison(schema1, schema2)

# Style the DataFrame for better readability
styled_df = full_comparison_df.style.set_properties(**{
    'text-align': 'left',
    'font-size': '12px',
    'border': '1px solid black'
}).set_table_styles([
    {'selector': 'th', 'props': [('background-color', '#40466e'), ('color', 'white')]},
    {'selector': 'tr:nth-of-type(odd)', 'props': [('background-color', '#f2f2f2')]},
])

display(styled_df)

# %%
# Summary statistics
print("\n FINAL SUMMARY")
print("="*50)

if len(common_fields) == 0:
    print(" NO COMMON FIELDS - These files have completely different schemas!")
    print("   This suggests:")
    print("   • Different L2 data products")
    print("   • Different processing pipelines")
    print("   • Major schema changes between versions")
else:
    print(f" {len(common_fields)} fields in common")

print(f"\ Schema Comparison:")
print(f"   File 1 (2024): {len(schema1)} fields")
print(f"   File 2 (2022): {len(schema2)} fields")
print(f"   Overlap: {len(common_fields)} fields ({len(common_fields)/max(len(schema1), len(schema2))*100:.1f}%)")

# %%
```
# Aug 15 Update:
Process vote_history.py  
This code significantly reduces workload by filtering only necessary columns

```
#!/usr/bin/env python3
"""
Convert L2 VOTEHISTORY inside an S3 ZIP to Parquet on S3.

Usage (direct):
  export AWS_PROFILE=jingyao
  python process_votehistory_to_parquet.py \
    --bucket datahub-redshift-ohio \
    --key l2_updates/VM2--AK--2024-10-02.zip \
    --election primary \
    --flag Primary_2024_08_20 \
    --out-prefix parquet/votehistory

Usage (from finder JSONL; processes primary+general for chosen states):
  python process_votehistory_to_parquet.py \
    --bucket datahub-redshift-ohio \
    --out-prefix parquet/votehistory \
    --results vf_scan_2024.jsonl \
    --states AK,AL \
    --min-year 2024
"""
import os
import io
import re
import json
import csv
import argparse
import tempfile
import zipfile
from typing import Optional, Tuple, List

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# --- patterns & helpers ---
RE_VH = re.compile(r'(?i)(?:^|/)[^/]*vote[\s._-]*history[^/]*\.(csv|txt|tsv|tab|psv)$')
RE_ANY_TEXT = re.compile(r'(?i)\.(csv|txt|tsv|tab|psv)$')
ZIP_DATE = re.compile(r'--([A-Z]{2})--(\d{4}-\d{2}-\d{2})\.zip$')

# columns that look like ..._YYYY_MM_DD at the end
RE_DATE_SUFFIX = re.compile(r'_(\d{4})_\d{2}_\d{2}$')

def s3c(profile: Optional[str] = None):
    return (boto3.Session(profile_name=profile) if profile else boto3.Session()).client("s3")

def parse_zip_key_state_date(key: str) -> Tuple[str, str]:
    m = ZIP_DATE.search(key)
    return (m.group(1) if m else "XX", m.group(2) if m else "unknown")

def largest_votehistory_member(zf: zipfile.ZipFile) -> Optional[zipfile.ZipInfo]:
    # Prefer files matching VOTEHISTORY pattern (ignore any *DataDictionary*)
    cands = [inf for inf in zf.infolist()
             if RE_VH.search(inf.filename)
             and "dictionary" not in inf.filename.lower()]
    if cands:
        return max(cands, key=lambda x: x.file_size)
    # Fallback: largest text-like file excluding dictionaries
    txtish = [inf for inf in zf.infolist()
              if RE_ANY_TEXT.search(inf.filename)
              and "dictionary" not in inf.filename.lower()]
    return max(txtish, key=lambda x: x.file_size) if txtish else None

def sniff_sep(member_name: str, head_sample: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(head_sample, delimiters=",\t|;")
        return dialect.delimiter
    except Exception:
        low = member_name.lower()
        if low.endswith((".tab", ".tsv")): return "\t"
        if low.endswith(".psv"): return "|"
        return ","

# --- core streaming writer with schema freezing (all-string) ---
def write_parquet_streaming(
    zf: zipfile.ZipFile,
    member: zipfile.ZipInfo,
    local_out: str,
    election_flag: Optional[str],
    min_year: int = 2024,
    chunksize: int = 400_000
) -> Tuple[int, int]:
    # sniff delimiter
    with zf.open(member, "r") as fh_head:
        sample = fh_head.read(65536).decode("utf-8", "replace")
    sep = sniff_sep(member.filename, sample)

    writer: Optional[pq.ParquetWriter] = None
    total = 0
    ones = 0
    all_cols: Optional[List[str]] = None
    schema: Optional[pa.Schema] = None

    with zf.open(member, "r") as fh:
        text = io.TextIOWrapper(fh, encoding="utf-8", errors="replace", newline="")
        reader = pd.read_csv(text, dtype=str, chunksize=chunksize, low_memory=False, sep=sep)

        for chunk in reader:
            # --- Drop election/date columns older than min_year (keep flag even if older) ---
            drop_cols = []
            for c in chunk.columns:
                m = RE_DATE_SUFFIX.search(c)
                if m:
                    try:
                        yr = int(m.group(1))
                    except Exception:
                        continue
                    if yr < min_year and (not election_flag or c != election_flag):
                        drop_cols.append(c)
            if drop_cols:
                chunk = chunk.drop(columns=drop_cols)

            if all_cols is None:
                all_cols = list(chunk.columns)
                schema = pa.schema([pa.field(c, pa.string()) for c in all_cols])

            # align to first-chunk columns (add missing, drop extras), keep order
            missing = [c for c in all_cols if c not in chunk.columns]
            for c in missing:
                chunk[c] = pd.NA
            extras = [c for c in chunk.columns if c not in all_cols]
            if extras:
                chunk = chunk.drop(columns=extras)
            chunk = chunk[all_cols]

            # normalize a couple of common fields if present (optional)
            for col in ("state", "STATE"):
                if col in chunk.columns:
                    chunk[col] = chunk[col].astype("string").str.upper().str.strip()
            for col in ("county", "COUNTY"):
                if col in chunk.columns:
                    chunk[col] = chunk[col].astype("string").str.upper().str.strip()

            # count election flag (treat '1' or 'Y' as voted)
            if election_flag and election_flag in chunk.columns:
                ones += chunk[election_flag].fillna("0").astype("string").str.upper().isin(["1", "Y"]).sum()

            # cast everything to string so we never get null-vs-string schema flips
            chunk = chunk.astype("string")

            tbl = pa.Table.from_pandas(chunk, preserve_index=False, schema=schema, safe=False)
            if writer is None:
                writer = pq.ParquetWriter(local_out, schema, compression="zstd")
            writer.write_table(tbl)
            total += len(chunk)

    if writer:
        writer.close()
    return int(total), int(ones)

# --- one-zip processor ---
def process_one_zip(
    s3,
    bucket: str,
    key: str,
    out_prefix: str,
    election_type: str,
    election_flag: Optional[str],
    min_year: int,
    overwrite: bool = True
) -> Tuple[str, str]:
    state, pull_date = parse_zip_key_state_date(key)
    base = f"{out_prefix.rstrip('/')}/state={state}/pull_date={pull_date}/election={election_type}"
    out_parquet = f"{base}/votehistory.parquet"
    out_stats   = f"{base}/_stats.json"

    if not overwrite:
        try:
            s3.head_object(Bucket=bucket, Key=out_parquet)
            print(f"[SKIP] exists: s3://{bucket}/{out_parquet}")
            return out_parquet, out_stats
        except Exception:
            pass

    print(f"==> {state} {election_type} | {key} | pull_date={pull_date} | flag={election_flag or 'n/a'} | min_year={min_year}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    zf = zipfile.ZipFile(io.BytesIO(obj["Body"].read()))
    member = largest_votehistory_member(zf)
    if not member:
        raise RuntimeError(f"No VOTEHISTORY file found in ZIP: s3://{bucket}/{key}")

    with tempfile.TemporaryDirectory() as td:
        local_out = os.path.join(td, f"{state}_{pull_date}_{election_type}.parquet")
        rows, ones = write_parquet_streaming(
            zf, member, local_out, election_flag=election_flag, min_year=min_year
        )
        print(f"[LOCAL] {rows:,} rows -> {local_out}")
        s3.upload_file(local_out, bucket, out_parquet)
        print(f"[S3] uploaded parquet -> s3://{bucket}/{out_parquet}")

    stats = {
        "state": state,
        "pull_date": pull_date,
        "election": election_type,
        "flag_field": election_flag,
        "rows": int(rows),
        "flag_ones": int(ones),
        "min_year": int(min_year),
        "dropped_rule": "drop columns ending with _YYYY_MM_DD where YYYY < min_year (flag preserved)",
    }
    s3.put_object(Bucket=bucket, Key=out_stats, Body=json.dumps(stats).encode("utf-8"))
    print(f"[S3] uploaded stats -> s3://{bucket}/{out_stats}")
    return out_parquet, out_stats

# --- runner modes ---
def run_from_results(args):
    s3 = s3c(args.profile)
    wanted = set(s.strip().upper() for s in args.states.split(",")) if args.states else None
    with open(args.results) as f:
        for line in f:
            rec = json.loads(line)
            st = (rec.get("state") or "").upper()
            if not st or (wanted and st not in wanted):
                continue

            # primary
            p = rec.get("primary")
            if p and p.get("zip"):
                process_one_zip(
                    s3, args.bucket, p["zip"], args.out_prefix,
                    election_type="primary",
                    election_flag=(p.get("columns") or [None])[0],
                    min_year=args.min_year,
                    overwrite=(not args.no_overwrite),
                )

            # general
            g = rec.get("general")
            if g and g.get("zip"):
                process_one_zip(
                    s3, args.bucket, g["zip"], args.out_prefix,
                    election_type="general",
                    election_flag=(g.get("columns") or [None])[0],
                    min_year=args.min_year,
                    overwrite=(not args.no_overwrite),
                )
    print("[DONE] from results")

def run_direct(args):
    s3 = s3c(args.profile)
    assert args.key and args.election, "--key and --election are required in direct mode"
    process_one_zip(
        s3, args.bucket, args.key, args.out_prefix,
        election_type=args.election,
        election_flag=args.flag,
        min_year=args.min_year,
        overwrite=(not args.no_overwrite),
    )

# --- cli ---
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--out-prefix", default="parquet/votehistory")
    ap.add_argument("--profile", default=os.environ.get("AWS_PROFILE"))

    # Mode A: from results JSONL
    ap.add_argument("--results", help="finder JSONL (e.g., vf_scan_2024.jsonl)")
    ap.add_argument("--states", help="Comma-separated states to include (e.g., AK,AL)")

    # Mode B: direct
    ap.add_argument("--key", help="S3 key to a ZIP")
    ap.add_argument("--election", help="e.g., general|primary")
    ap.add_argument("--flag", help="Election flag column (e.g., General_2024_11_05)")

    # Filter setting
    ap.add_argument("--min-year", type=int, default=2024,
                    help="Drop columns whose names end with _YYYY_MM_DD when YYYY < this year. Default: 2024")

    ap.add_argument("--no-overwrite", action="store_true")
    args = ap.parse_args()

    if args.results:
        run_from_results(args)
    else:
        run_direct(args)

if __name__ == "__main__":
    main()

```

## Current processing code demo:
```
(.venv) ubuntu@ip-10-11-65-4:~$ cat process_demo_to_parquet.py
#!/usr/bin/env python3
"""
Extract DEMOGRAPHIC (voter master) from L2 ZIP(s) on S3 and write Parquet (ALL COLUMNS) to S3.

Direct example:
  export AWS_PROFILE=jingyao
  python process_demo_to_parquet.py \
    --bucket datahub-redshift-ohio \
    --key l2_updates/VM2--AK--2024-12-24.zip \
    --out-prefix parquet/demo

Batch example (mirrors votehistory usage; processes both primary & general zips listed):
  python process_demo_to_parquet.py \
    --bucket datahub-redshift-ohio \
    --out-prefix parquet/demo \
    --results vf_scan_2024.jsonl \
    --states AK,AL
"""
import os
import io
import re
import sys
import csv
import json
import argparse
import tempfile
import zipfile
from typing import Optional, Tuple, List, Set

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# --- patterns ---
RE_ANY_TEXT = re.compile(r'(?i)\.(csv|txt|tsv|tab|psv)$')
RE_DEMO_PRIORITIZED = re.compile(
    r'(?i)(?:^|/)[^/]*?(?:DEMOGRAPHIC|DEMO|VOTER(?:FILE|_FILE|_MASTER)?)'
    r'[^/]*\.(csv|txt|tsv|tab|psv)$'
)
ZIP_STATE_DATE = re.compile(r'--([A-Z]{2})--(\d{4}-\d{2}-\d{2})\.zip$')

def s3c(profile: Optional[str] = None):
    return (boto3.Session(profile_name=profile) if profile else boto3.Session()).client("s3")

def parse_state_date_from_key(key: str) -> Tuple[str, str]:
    m = ZIP_STATE_DATE.search(key)
    return (m.group(1) if m else "XX", m.group(2) if m else "unknown")

def sniff_sep(member_name: str, head_sample: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(head_sample, delimiters=",\t|;")
        return dialect.delimiter
    except Exception:
        low = member_name.lower()
        if low.endswith((".tab", ".tsv")): return "\t"
        if low.endswith(".psv"): return "|"
        return ","

def choose_demo_member(zf: zipfile.ZipFile) -> Optional[zipfile.ZipInfo]:
    """Pick DEMOGRAPHIC-like text file, ignoring dictionaries; fallback by header containing lalvoterid."""
    # Filter text-like & not a dictionary
    candidates = [i for i in zf.infolist()
                  if RE_ANY_TEXT.search(i.filename or "")
                  and "dictionary" not in (i.filename or "").lower()]

    # Prefer names that look like DEMOGRAPHIC/DEMO/VOTERFILE
    pri = [i for i in candidates if RE_DEMO_PRIORITIZED.search(i.filename or "")]
    if pri:
        return max(pri, key=lambda x: x.file_size)

    # Fallback: largest text file whose first line (header) contains lalvoterid
    best = None
    best_size = -1
    for inf in candidates:
        try:
            with zf.open(inf, "r") as fh:
                head = fh.read(128 * 1024).decode("utf-8", "replace")
            first = (head.splitlines() or [""])[0]
            header_fields = re.split(r"[,|\t;]", first)
            if any(c.strip().lower() == "lalvoterid" for c in header_fields):
                if inf.file_size > best_size:
                    best, best_size = inf, inf.file_size
        except Exception:
            pass
    return best

def write_parquet_streaming_allcols(
    zf: zipfile.ZipFile,
    member: zipfile.ZipInfo,
    local_out: str,
    chunksize: int = 400_000
) -> Tuple[int, int]:
    """Read ALL columns, freeze first-chunk schema to all-string, stream to Parquet."""
    # Sniff delimiter
    with zf.open(member, "r") as fh_head:
        sample = fh_head.read(65536).decode("utf-8", "replace")
    sep = sniff_sep(member.filename, sample)

    writer: Optional[pq.ParquetWriter] = None
    total_rows = 0
    ncols = 0
    all_cols: Optional[List[str]] = None
    schema: Optional[pa.Schema] = None

    with zf.open(member, "r") as fh:
        text = io.TextIOWrapper(fh, encoding="utf-8", errors="replace", newline="")
        reader = pd.read_csv(text, dtype=str, chunksize=chunksize, low_memory=False, sep=sep)

        for chunk in reader:
            if all_cols is None:
                all_cols = list(chunk.columns)
                ncols = len(all_cols)
                schema = pa.schema([pa.field(c, pa.string()) for c in all_cols])

            # Align to first-chunk columns
            missing = [c for c in all_cols if c not in chunk.columns]
            for c in missing:
                chunk[c] = pd.NA
            extras = [c for c in chunk.columns if c not in all_cols]
            if extras:
                chunk = chunk.drop(columns=extras)
            chunk = chunk[all_cols]

            # Force all string to stabilize schema
            chunk = chunk.astype("string")

            tbl = pa.Table.from_pandas(chunk, preserve_index=False, schema=schema, safe=False)
            if writer is None:
                writer = pq.ParquetWriter(local_out, schema, compression="zstd")
            writer.write_table(tbl)
            total_rows += len(chunk)

    if writer:
        writer.close()
    return int(total_rows), int(ncols)

def process_one_zip(s3, bucket: str, key: str, out_prefix: str, overwrite: bool = True) -> Tuple[str, str]:
    state, pull_date = parse_state_date_from_key(key)
    dest_prefix = f"{out_prefix.rstrip('/')}/state={state}/pull_date={pull_date}"
    out_parquet = f"{dest_prefix}/demo.parquet"
    out_stats   = f"{dest_prefix}/_stats.json"

    if not overwrite:
        try:
            s3.head_object(Bucket=bucket, Key=out_parquet)
            print(f"[SKIP] exists: s3://{bucket}/{out_parquet}")
            return out_parquet, out_stats
        except Exception:
            pass

    print(f"==> DEMO {state} | {key} | pull_date={pull_date}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    zf = zipfile.ZipFile(io.BytesIO(obj["Body"].read()))

    member = choose_demo_member(zf)
    if not member:
        raise RuntimeError(f"No DEMOGRAPHIC file found in ZIP: s3://{bucket}/{key}")

    with tempfile.TemporaryDirectory() as td:
        local_out = os.path.join(td, f"{state}_{pull_date}_demo.parquet")
        rows, ncols = write_parquet_streaming_allcols(zf, member, local_out)
        print(f"[LOCAL] {rows:,} rows, {ncols} columns -> {local_out}")
        s3.upload_file(local_out, bucket, out_parquet)
        print(f"[S3] uploaded -> s3://{bucket}/{out_parquet}")

    # small stats doc
    stats = {"state": state, "pull_date": pull_date, "rows": int(rows), "columns": int(ncols)}
    s3.put_object(Bucket=bucket, Key=out_stats, Body=json.dumps(stats).encode("utf-8"))
    print(f"[S3] uploaded stats -> s3://{bucket}/{out_stats}")

    return out_parquet, out_stats

# -------- runners --------
def run_direct(args):
    s3 = s3c(args.profile)
    return process_one_zip(s3, args.bucket, args.key, args.out_prefix, overwrite=(not args.no_overwrite))

def run_from_results(args):
    s3 = s3c(args.profile)
    wanted: Optional[Set[str]] = set(x.strip().upper() for x in args.states.split(",")) if args.states else None

    # Build a set of ZIP keys to avoid duplicates when primary/general share the same ZIP
    to_process: Set[str] = set()
    with open(args.results) as f:
        for line in f:
            rec = json.loads(line)
            st = (rec.get("state") or "").upper()
            if not st or (wanted and st not in wanted):
                continue
            for k in ("primary", "general"):
                node = rec.get(k) or {}
                z = node.get("zip")
                if z:
                    to_process.add(z)

    if not to_process:
        print("[INFO] nothing to process from results (check --states and JSONL).")
        return

    for key in sorted(to_process):
        process_one_zip(s3, args.bucket, key, args.out_prefix, overwrite=(not args.no_overwrite))

    print("[DONE] demo from results")

# -------- cli --------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--out-prefix", default="parquet/demo")
    ap.add_argument("--profile", default=os.environ.get("AWS_PROFILE"))
    ap.add_argument("--no-overwrite", action="store_true")

    # Mode A: batch from finder JSONL
    ap.add_argument("--results", help="finder JSONL (e.g., vf_scan_2024.jsonl)")
    ap.add_argument("--states", help="Comma-separated states to include (e.g., AK,AL)")

    # Mode B: direct single ZIP
    ap.add_argument("--key", help="ZIP key like l2_updates/VM2--AK--YYYY-MM-DD.zip")

    args = ap.parse_args()

    if args.results:
        run_from_results(args)
    elif args.key:
        run_direct(args)
    else:
        print("Provide either --results (batch) or --key (direct).", file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
    main()
```

### Step 5: Create external tables in redshift
