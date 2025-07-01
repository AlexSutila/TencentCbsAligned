from bs4 import BeautifulSoup
import re
import os
import csv
import time
import requests

BASE_URL = "https://ftp.pdl.cmu.edu/pub/datasets/twemcacheWorkload/cacheDatasets/tencentBlock/stat/"
CSV_FILE = "tencent_stats.csv"
ERROR_LOG = "errors.log"
MAX_RETRIES = 3

FIELDS = [
    "device_id",
    "n_original_req",
    "n_req",
    "n_obj",
    "n_byte",
    "n_uniq_byte",
    "n_read",
    "n_write",
    "start_ts",
    "end_ts",
    "duration"
]

def parse_stat_content(text):
    stats = {}
    for field in FIELDS[1:]:  # skip device_id for now
        match = re.search(rf'^\s*{field}:\s*([\d]+)', text, re.MULTILINE)
        if match:
            stats[field] = int(match.group(1))
        else:
            stats[field] = None
    return stats

def fetch_with_retry(url):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.text
        except requests.exceptions.HTTPError as e:
            if response.status_code == 503:
                print(f"Retry {attempt}/{MAX_RETRIES} for {url} (503)")
                time.sleep(2 ** attempt)
            else:
                raise
        except Exception as e:
            raise
    raise Exception(f"Failed after {MAX_RETRIES} retries: {url}")

def load_processed_ids():
    if not os.path.exists(CSV_FILE):
        return set()
    with open(CSV_FILE, newline="") as f:
        return {row["device_id"] for row in csv.DictReader(f)}

def main():
    response = requests.get(BASE_URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    links = soup.find_all("a", href=re.compile(r'\d+\.oracleGeneral\.stat'))

    processed_ids = load_processed_ids()

    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            writer.writeheader()

    for link in links:
        href = link['href']
        device_id = href.split('.')[0]

        if device_id in processed_ids:
            continue

        stat_url = BASE_URL + href

        try:
            text = fetch_with_retry(stat_url)
            stats = parse_stat_content(text)
            stats["device_id"] = device_id

            if stats["n_req"] is not None:
                with open(CSV_FILE, "a", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=FIELDS)
                    writer.writerow(stats)
                print(f"✓ {device_id}: n_req = {stats['n_req']}")
            else:
                print(f"Skipped {device_id} (n_req not found)")
                with open(ERROR_LOG, "a") as logf:
                    logf.write(f"{device_id},{stat_url},NO_N_REQ\n")

        except Exception as e:
            print(f"Error fetching {stat_url}: {e}")
            with open(ERROR_LOG, "a") as logf:
                logf.write(f"{device_id},{stat_url},{str(e)}\n")

    print(f"\nDone. Data written to {CSV_FILE}")

if __name__ == "__main__":
    main()

