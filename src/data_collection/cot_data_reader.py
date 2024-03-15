
import requests
import os
from io import StringIO
import pandas as pd


class COT_Data_Reader():
    def __init__(self, dataset_name: str = "short-term-rentals-registration") -> None:
        self.dataset_name = dataset_name
        self.base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
        self.package_endpoint = os.path.join(
            self.base_url, "api/3/action/package_show")
        self.package_endpoint_params = {"id": dataset_name}
        self.resource_prefix = os.path.join(self.base_url, "datastore/dump/")

    def __call__(self):
        package = requests.get(self.package_endpoint,
                               params=self.package_endpoint_params).json()

        resource_info = [
            resource for resource in package['result']['resources']
            if (resource["datastore_active"]) and (resource['format'] == 'CSV')
        ][0]

        data_last_updated = resource_info['datastore_cache_last_update']
        data_url = resource_info['url']

        raw_data = StringIO(requests.get(data_url).text)

        raw_data_df = pd.read_csv(raw_data)

        return raw_data_df, data_last_updated