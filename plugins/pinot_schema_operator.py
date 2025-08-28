import glob
from typing import Any
import json

import requests
from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.utils.decorators import apply_defaults


'''
find all definded schemas in the schema folder, send to pinot;
used for DAG schema_dag

'''

class PinotSchemaSubmitOperator(BaseOperator):
    @apply_defaults
    def __init__(self, folder_path, pinot_url, *args, **kwargs):
        super(PinotSchemaSubmitOperator, self).__init__(*args, **kwargs)
        self.folder_path = folder_path
        self.pinot_url = pinot_url

    def execute(self, context: Context) -> Any:
        
        schema_files = glob.glob(self.folder_path + '/*.json')
        if not schema_files:
            raise RuntimeError(f"No schema files under {self.folder_path}")
        
        for schema_file in schema_files:
            with open(schema_file, 'r', encoding='utf-8') as f:
                body = f.read()
                # schema_data = file.read()

                # #define the headers and submit the post request to pinot, schema is in json format, 
                # #send to pinot server
                # headers = {'Content-Type': 'application/json'}
                # response = requests.post(self.pinot_url, headers=headers, data=schema_data)

                # if response.status_code == 200:
                #     self.log.info(f'Schema successfully submitted to Apache Pinot! {schema_file}')
                # else:
                #     self.log.error(f'Failed to submit schema: {response.status_code} - {response.text}')
                #     raise Exception(f'Schema submission failed with status code {response.status_code}')
            try:
                payload = json.loads(body)
            except Exception as e:
                raise RuntimeError(f"Invalid JSON in {schema_file}: {e}")
            
            schema_name = payload.get("schemaName")
            if not schema_name:
                raise RuntimeError(f"{schema_file} missing 'schemaName'")
            
            # submit schema to pinot
            self.log.info(f"POST {self.pinot_url} < {schema_file}")
            r = requests.post(
                self.pinot_url,
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=15
            )
            self.log.info(f"-> {r.status_code} {r.text[:300]}")
            r.raise_for_status()

            # double check
            get_url = f"{self.pinot_url}/{schema_name}"
            self.log.info(f"GET {get_url}")
            v = requests.get(get_url, timeout=10)
            self.log.info(f"-> {v.status_code} {v.text[:300]}")
            v.raise_for_status()



        