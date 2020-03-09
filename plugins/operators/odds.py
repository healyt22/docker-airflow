import os
import requests
import yaml
import json

from tempfile import NamedTemporaryFile

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

AIRFLOW_HOME = os.environ['AIRFLOW_HOME']


class OddsApiOperator(BaseOperator):
    template_fields = ('endpoint',)
    ui_color = '#A3E4D7'

    @apply_defaults
    def __init__(self, endpoint, out_filepath=None, *args, **kwargs):
        super(OddsApiOperator, self).__init__(*args, **kwargs)
        self.endpoint = endpoint

        if not out_filepath:
            out_filepath = NamedTemporaryFile(delete=False).name
        self.out_filepath = out_filepath
        print(self.out_filepath)

        keys_path = os.path.join(AIRFLOW_HOME, 'plugins', 'keys.yaml')
        with open(keys_path, 'r') as f:
            self.api_key = yaml.safe_load(f)

    def execute(self, context):
        url = 'https://api.the-odds-api.com'
        version = 'v3'
        url_with_endpoint = os.path.join(url, version, self.endpoint)

        response = requests.get(url_with_endpoint,
                                params = {'api_key': self.api_key})
        response_json = json.loads(response.text)
        with open(self.out_filepath, "w") as f:
            json.dump(response_json, f, indent=4)

        ti = context['task_instance']
        ti.xcom_push(key='out_filepath', value=self.out_filepath)
