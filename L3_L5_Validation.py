import psycopg2
import boto3
import numpy
import requests
import pandas as pd
from io import StringIO

date = '2023-06-01' ## Month of attribution

class Runner(object):
    @staticmethod
    def runner(file_object):
        kpi  = {}
        temp = None
        sql_text = ''
        for line in file_object.readlines():
            if '--' in line:
                sql_text = sql_text.replace('DATE', date)
                kpi[temp]= sql_text.strip()
                sql_text = ''
                temp = line[3:].strip()
            else:
                sql_text = sql_text + ' ' + line
            if not line:
                break

        sql_text = sql_text.replace('DATE', date)
        kpi[temp]= sql_text.strip()

        host = 'arkansashealth-prod-redshift.cg8mcnhmisj5.us-east-1.redshift.amazonaws.com' ## hostname
        port = '5439' ## port
        user = 'batman' ## username
        database = 'arkansashealth_prod' ## db name
        password = 'Test#123' ## credentials

        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            database=database,
            password=password
        )
        cur  = conn.cursor()
        Lx_result_array = []

        for t in kpi.keys():
            if t is None:
                cur.execute(kpi[t])
                continue
            cur.execute(kpi[t])
            i = cur.fetchall()
            if len(i[0]) == 4:
                for j in i:
                    Lx_result_array.append([t,j[0],str(j[1]),str(j[2]),str(j[3])])
            else:
                for j in i:
                    Lx_result_array.append([t,j[0],str(j[1]),str(j[2]),"N/A"])

        Lx_result = pd.DataFrame(numpy.asarray(Lx_result_array), 
            index = None , 
            columns = [
            'KPI',
            'Plans',
            'L5_Numbers',
            'L3_Numbers',
            'L2_Numbers'])

        csv_buffer = StringIO()
        Lx_result.to_csv(csv_buffer, index = False)
        host = "dap-configuration-management"
        port = "80"
        resx = requests.get(url=f"http://{host}:{port}/collection/credential", 
            params = {
            "datastore_name": "invcr_prod", 
            "user_id": "dap_user"})
        credentials = resx.json()["data"]
        output_file = "cost_validation_report/report/"
        file_upload = f"Cost_report-{date}.csv"
        session = boto3.Session(aws_access_key_id = credentials['awsaccesskey'], 
            aws_secret_access_key = credentials['awssecretkey'])

        s3 = session.resource('s3') # source s3 bucket
        s3_bucket = s3.Bucket(credentials['bucketname'])
        s3_bucket.put_object(Body = csv_buffer.getvalue(), 
            Key = output_file+file_upload)
        yield str(Lx_result)