# Validation-Scripts
REDSHIFT validation scripts to check for consistency between L5 and L3 parameters.
## Install libraries
```
pip install -r requirements.txt
```
## Run the validation python script
Make changes in *Validation.py* file by specifying the database host, port, username and password. Also specify for which SQL script you want to validate. Also specify the month of measurement/attribution. Then run the script.
```
python Validation.py
```
## Output
The script outputs a CSV file mentioning the KPIs, Plan names, L5 Schema numbers, L3 Schema numbers, and L2 schema numbers (if available).
