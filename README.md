# Validation Scripts

* Checks for data consistency
* Applied b/w L3 and L3 schemas
* Data gets pushed onto dashboards
* And results are shared with the customer for internal auditing.

---

***Categories:-***
| Dashboards | Descriptions |
|:---------- |:------------ |
| Attribution | Tracks Enrollments/Disenrollments, and Patient Leakages |
| Cost | Tracks customer spend for inpatient, ED, and SNF admits |
| Risk | Tracks the risk and associated costs for patients (e.g., BP) |
| Medication | Tracks expenditure on medications and suggests alternatives |
| Provider Performance | Ranks physicians based on quality, readmissions, and risk recapture rates |
| Quality | Measure-wise realization of *value based services* provided to the target demographics |

---

***How to Execute:-***

* Install packages from requirements.txt
* Edit the DB connections
* Edit the month/year for which the metrics have to be calculated
* Run the *L3_L5_Validation.py* file
