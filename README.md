# Validation Scripts

* Checks for data consistency
* Applied b/w L3 and L3 schemas
* Data gets pushed onto dashboards
* And results are shared with the customer for internal auditing.

---

***Categories***
| Dashboards | Descriptions |
|:---------- |:------------ |
| Attribution | Enrollments/Disenrollments, and Patient Leakages |
| Cost | Customer spend for inpatient, ED, and SNF admits |
| Risk | Risk and associated costs for patients (e.g., BP) |
| Medication | Expenditure on medications and alternative medications|
| Provider Performance | Ranks physicians based on quality, readmissions, and risk recapture rates |
| Quality | Computes quality score per measure, as a metric for **value-based care**|

---

***Execute***
* `pip install requirements.txt`
* `L3_L5_Validation.py` edit DB urls and month/year.
* ```python L3_L5_Validation.py```
