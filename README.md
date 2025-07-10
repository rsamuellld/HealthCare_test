# For CMS Hospitals Data Downloader by Raymond Samuel 

This Python project downloads datasets related to the the hospitals via API call. This  processes their CSV files by converting column headers to the snake_case, and saves the cleaned CSVs locally.

---

## Features

- Filters datasets by theme "Hospitals".
- Downloads only datasets modified since the last run.
- Converts CSV headers to snake_case (e.g., "Patients’ rating of the facility" → "patients_rating_of_the_facility").
- Downloads and processes files in parallel.
- Runs on any standard Linux or Windows environment with Python 3.
- Easily scheduled to run daily with Linux cron or GitHub Actions.

---

## Setup & Usage

1. Clone the repository:

   ```bash
   git clone https://github.com/rsamuellld/HealthCare_test
   cd cms_hospitals_downloader
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Run the downloader:

   ```bash
   python cms_hospitals_downloader.py
   ```

Processed CSV files will be saved in the `cms_hospitals_data/` directory.

---

## Scheduling

### Linux Cron Job

Edit your crontab:

```bash
crontab -e
```

Add this line to run daily at 2 AM:

```cron
0 2 * * * /usr/bin/python3 /path/to/cms_hospitals_downloader.py >> /path/to/cms_hospitals_downloader.log 2>&1
```

---

## GitHub Actions

A GitHub Actions workflow is included to run the script daily and upload the processed CSV files as build artifacts.

- Located at `.github/workflows/daily_run.yml`
- Scheduled for 2:00 AM UTC daily.

---

## Files

- `cms_hospitals_downloader.py`: Main downloader script.
- `requirements.txt`: Python dependencies.
- `.gitignore`: Files and folders excluded from Git.
- `.github/workflows/daily_run.yml`: GitHub Actions workflow config.

---

## Contact

For questions or support, please contact Raymond Samuel 
