# Validation Scripts

* Checks for data consistency
* Applied b/w L3 and L3 schemas
* Data gets pushed onto dashboards
* And results are shared with the customer for internal auditing.

---

***Categories:-***
| Dashboards | Descriptions |
|:---------- |:------------ |
| Attribution | Tracks Enrollments / Disenrollments, deaths, and patient leakage across organizations |
| Cost | Tracks customer spend for Inpatient/Emergency/SNF admits |
| Medication | Tracks customer spend on drugs and suggests generic alternatives |
| Provider Performance | Ranks physicians based on quality of care, readmits and pre-emptive diagnoses |
| Quality | Measure-wise realization of *value based services* provided to the target demographics |
| Risk | Tracks the risk for patients (eg. diabetic, high bp, etc.) and estimates associated cost |

---

***How to Execute:-***

* Install packages from requirements.txt
* Edit the DB connections
* Edit the month/year for which the metrics have to be calcualated
* Run the *L3_L5_Validation.py* file
