"""
Download public insurance datasets from FEMA, CAS, and other sources.
Stores raw files in the landing zone for Bronze layer ingestion.

TRAINEE GUIDE — Why download public data?
Real insurance data is highly sensitive (personal details, claim amounts).
This platform supplements synthetic data with REAL publicly available datasets:

1. FEMA NFIP (National Flood Insurance Program):
   - Source: https://www.fema.gov/flood-insurance/work-with-nfip/data-visualizations
   - What: Real flood insurance policies and claims data for the entire US
   - Scale: 2.7M claims, 72M policies (one of the largest public insurance datasets)
   - Format: JSON API (paginated) + CSV bulk download
   - Why useful: Gives realistic flood zone distributions, claim amounts, and
     geographic patterns that synthetic data cannot easily replicate

2. CAS (Casualty Actuarial Society) Schedule P datasets:
   - Source: https://www.casact.org/publications-research/research/research-resources
   - What: Actuarial loss development triangles for Auto, Workers' Comp, GL
   - Format: CSV
   - Why useful: Real industry loss development patterns used by actuaries.
     Loss triangles show how claims evolve over time — crucial for reserving.

Data flow:
  FEMA/CAS API or URL → requests.get() → raw file → landing zone path
  Landing zone → Bronze ingestion (ingest_structured.py or ingest_semi_structured.py)

Usage (Databricks notebook):
    from src.ingestion.download_public_data import PublicDataDownloader
    downloader = PublicDataDownloader(spark, config)
    downloader.download_all()
"""

import requests
import yaml
from pyspark.sql import SparkSession


class PublicDataDownloader:
    """Downloads public insurance datasets to raw landing zone.

    TRAINEE NOTE — Why a class with a config dict?
    The URLs and paths for public data sources can change (APIs are versioned,
    bulk files move). By putting all source definitions in a config dictionary
    (loaded from YAML or defaulting to hard-coded values), we can update source
    locations without changing any logic code.

    Config structure:
      storage.raw_landing_zone : Where to write downloaded files
      data_sources             : Dictionary of source_name → {url, format, lob}
    """

    def __init__(self, spark: SparkSession, config_path: str = None):
        self.spark = spark
        # Load config from YAML file if provided, otherwise use defaults.
        # This allows the same class to be used with different environment configs
        # (dev, staging, prod) by passing different config file paths.
        if config_path:
            with open(config_path, "r") as f:
                self.config = yaml.safe_load(f)
        else:
            self.config = self._default_config()
        self.landing_zone = self.config["storage"]["raw_landing_zone"]

    def _default_config(self):
        """Return hardcoded default config when no config file is provided.

        TRAINEE NOTE — When is this used?
        In Databricks notebooks the config YAML may not be easily accessible
        without additional setup. The default config provides sensible
        defaults so the downloader works out-of-the-box.

        Source breakdown:
          fema_nfip_claims/policies : FEMA OpenFEMA API (JSON, paginated)
          cas_workers_comp          : CAS WC loss triangle (CSV)
          cas_private_auto          : CAS personal auto loss triangle (CSV)
          cas_commercial_auto       : CAS commercial auto loss triangle (CSV)
          cas_other_liability       : CAS other liability = GL (CSV)
          cas_product_liability     : CAS product liability = GL (CSV)
        """
        return {
            "storage": {"raw_landing_zone": "/Volumes/insurance_catalog/bronze/raw_data"},
            "data_sources": {
                # ---- FEMA NFIP API sources (JSON format) ----
                "fema_nfip_claims": {
                    "url": "https://www.fema.gov/api/open/v2/FimaNfipClaims",
                    "format": "json",
                    "lob": "property",
                },
                "fema_nfip_policies": {
                    "url": "https://www.fema.gov/api/open/v2/FimaNfipPolicies",
                    "format": "json",
                    "lob": "property",
                },
                # ---- CAS Schedule P actuarial datasets (CSV format) ----
                # CAS loss triangles contain cumulative paid losses by accident year
                # and development year — used to estimate ultimate losses (IBNR reserving)
                "cas_workers_comp": {
                    "url": "https://www.casact.org/sites/default/files/2021-04/wkcomp_pos.csv",
                    "format": "csv",
                    "lob": "workers_comp",
                },
                "cas_private_auto": {
                    "url": "https://www.casact.org/sites/default/files/2021-04/ppauto_pos.csv",
                    "format": "csv",
                    "lob": "auto",
                },
                "cas_commercial_auto": {
                    "url": "https://www.casact.org/sites/default/files/2021-04/comauto_pos.csv",
                    "format": "csv",
                    "lob": "auto",
                },
                "cas_other_liability": {
                    "url": "https://www.casact.org/sites/default/files/2021-04/othliab_pos.csv",
                    "format": "csv",
                    "lob": "general_liability",
                },
                "cas_product_liability": {
                    "url": "https://www.casact.org/sites/default/files/2021-04/prodliab_pos.csv",
                    "format": "csv",
                    "lob": "general_liability",
                },
            },
        }

    def download_all(self):
        """Download all configured data sources.

        TRAINEE NOTE — Graceful degradation:
        Each source is downloaded in a try/except block. If one source fails
        (network timeout, API rate limit, file moved), the error is logged and
        the loop continues to the next source. This prevents one bad download
        from cancelling all remaining downloads.

        In production pipelines, failed downloads would trigger a monitoring
        alert so the data engineering team can investigate.
        """
        print("=" * 60)
        print("Starting Public Data Download")
        print("=" * 60)

        for source_name, source_config in self.config["data_sources"].items():
            try:
                print(f"\nDownloading: {source_name}")
                self._download_source(source_name, source_config)
                print(f"  -> Completed: {source_name}")
            except Exception as e:
                print(f"  -> FAILED: {source_name} — {str(e)}")

        print("\n" + "=" * 60)
        print("Download Complete")
        print("=" * 60)

    def _download_source(self, source_name: str, source_config: dict):
        """Route a single source to the appropriate download method based on format.

        TRAINEE NOTE:
        The landing zone path follows the pattern:
          /mnt/insurance-data/raw/{lob}/{source_name}/
          e.g. /mnt/insurance-data/raw/property/fema_nfip_claims/

        This matches the path prefixes expected by BronzeStructuredIngestion and
        BronzeSemiStructuredIngestion in src/bronze/. If you change the path
        here, you must also update the ingestion_map in those classes.
        """
        url = source_config["url"]
        fmt = source_config["format"]
        lob = source_config["lob"]
        output_dir = f"{self.landing_zone}/{lob}/{source_name}"

        if fmt == "csv":
            self._download_csv(url, output_dir, source_name)
        elif fmt == "json":
            self._download_fema_api(url, output_dir, source_name)

    def _download_csv(self, url: str, output_dir: str, source_name: str):
        """Download CSV file and save to landing zone.

        TRAINEE NOTE — Two download strategies (primary and fallback):

        Strategy 1 (Primary): Spark reads directly from URL
          self.spark.read.csv(url) — Spark can read directly from HTTP URLs on
          Databricks clusters that have outbound internet access. This is fast
          because Spark downloads in parallel across workers.

        Strategy 2 (Fallback): requests.get() + local write
          If Spark can't reach the URL (e.g. firewall, corporate proxy), we fall
          back to the requests library. This downloads the file to the driver node's
          local /tmp/ directory first, then reads it from there with Spark.
          Note: /tmp/ is temporary and not shared across workers, which is why we
          re-read it with Spark and write to the shared cloud storage path.

        The try/except around Strategy 1 automatically falls back to Strategy 2
        without any manual intervention.

        stream=True in requests.get():
          Downloads the response body in chunks instead of loading the entire
          file into memory at once. Crucial for large files (GB-scale CSVs).
          chunk_size=8192 = 8KB per chunk (a good balance for network efficiency).
        """
        # Strategy 1: Spark direct URL read (works on Databricks with internet access)
        try:
            df = self.spark.read.option("header", "true").csv(url)
            df.write.mode("overwrite").option("header", "true").csv(output_dir)
            print(f"  -> Rows: {df.count()}")
        except Exception:
            # Strategy 2: Download via requests library to local /tmp/, then load with Spark
            response = requests.get(url, stream=True, timeout=120)
            response.raise_for_status()  # Raises HTTPError for 4xx/5xx status codes
            local_path = f"/tmp/{source_name}.csv"
            with open(local_path, "wb") as f:
                # iter_content streams the response in 8KB chunks to avoid OOM
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            # Read from local file and write to cloud storage
            df = self.spark.read.option("header", "true").csv(local_path)
            df.write.mode("overwrite").option("header", "true").csv(output_dir)
            print(f"  -> Rows: {df.count()}")

    def _download_fema_api(self, base_url: str, output_dir: str, source_name: str, batch_size: int = 10000):
        """
        Download data from FEMA OpenAPI with pagination.
        The API returns JSON with a max of 10,000 records per request.

        TRAINEE NOTE — API Pagination:
        Most REST APIs limit how many records they return in a single response
        (here: 10,000 records max). To get all records, we must make MULTIPLE
        requests, each fetching the next "page" of results.

        FEMA uses OData-style pagination parameters:
          $skip  : How many records to skip (i.e. which page we're on)
          $top   : How many records to return in this request (page size)
          $format: Response format (we request JSON)

        Example: To get records 20,000–30,000:
          URL = base_url + "?$skip=20000&$top=10000&$format=json"

        Termination conditions (when to stop looping):
          1. dataset_key is missing or the result array is empty → no more data
          2. len(records) < batch_size → the API returned a partial page,
             meaning we've reached the end of the dataset
          3. skip >= max_records → safety cap to avoid infinite loops
             (Full FEMA dataset has 2.7M+ claims; we cap at 100K for initial load)

        FEMA API response structure:
          {
            "metadata": { "skip": 0, "top": 10000, "count": 2700000 },
            "FimaNfipClaims": [ {...}, {...}, ... ]
          }
        The "metadata" key is excluded from the dataset_key search so we find
        the actual data key ("FimaNfipClaims" or "FimaNfipPolicies") dynamically.
        """
        all_records = []
        skip = 0
        # Safety cap: 100K records for initial download.
        # Use download_fema_bulk_csv() for the full multi-million record dataset.
        max_records = 100000

        while skip < max_records:
            url = f"{base_url}?$skip={skip}&$top={batch_size}&$format=json"
            print(f"  -> Fetching records {skip} to {skip + batch_size}...")

            try:
                response = requests.get(url, timeout=120)
                response.raise_for_status()
                data = response.json()

                # Dynamically detect the dataset key (not "metadata")
                dataset_key = None
                for key in data.keys():
                    if key not in ("metadata",):
                        dataset_key = key
                        break

                # No data key found or empty result → end of dataset
                if dataset_key is None or len(data.get(dataset_key, [])) == 0:
                    break

                records = data[dataset_key]
                all_records.extend(records)   # Accumulate all pages in memory
                skip += batch_size

                # Partial page → we've reached the last page of results
                if len(records) < batch_size:
                    break

            except Exception as e:
                print(f"  -> API error at offset {skip}: {e}")
                break

        if all_records:
            # Convert the list of dicts to a Spark DataFrame and write as JSON
            # spark.createDataFrame(list_of_dicts) infers the schema from the data
            df = self.spark.createDataFrame(all_records)
            df.write.mode("overwrite").json(output_dir)
            print(f"  -> Total records downloaded: {len(all_records)}")
        else:
            print(f"  -> No records downloaded for {source_name}")

    def download_fema_bulk_csv(self, lob: str = "property"):
        """
        Download FEMA bulk CSV files (larger datasets).
        These are pre-packaged full extracts available for download.
        Run this on Databricks for the full-size files.

        TRAINEE NOTE — Bulk CSV vs. API pagination:
        The paginated API method above fetches up to 100K records (enough for
        development and testing). For the FULL FEMA dataset (2.7M+ claims,
        72M+ policies), FEMA provides pre-packaged bulk CSV exports at a
        special endpoint with ?$format=csv appended.

        When to use which:
          _download_fema_api()    : Quick download for dev/testing (100K records)
          download_fema_bulk_csv(): Full production dataset (2.7M+ records)

        The bulk download may take 10-30 minutes and requires a stable network
        connection. Run on a Databricks cluster with high network bandwidth.

        Note: The bulk CSV URL reads the data directly into Spark (no local temp
        file), relying on the cluster's network access to the FEMA endpoint.
        """
        bulk_sources = {
            "fema_nfip_claims_bulk": {
                "url": "https://www.fema.gov/api/open/v2/FimaNfipClaims?$format=csv",
                "output": f"{self.landing_zone}/property/fema_nfip_claims_bulk",
            },
            "fema_nfip_policies_bulk": {
                "url": "https://www.fema.gov/api/open/v2/FimaNfipPolicies?$format=csv",
                "output": f"{self.landing_zone}/property/fema_nfip_policies_bulk",
            },
        }

        for name, info in bulk_sources.items():
            print(f"Downloading bulk: {name}")
            try:
                # Spark reads the CSV directly from the FEMA URL
                df = self.spark.read.option("header", "true").csv(info["url"])
                df.write.mode("overwrite").option("header", "true").csv(info["output"])
                print(f"  -> Rows: {df.count()}")
            except Exception as e:
                print(f"  -> FAILED: {name} — {str(e)}")
