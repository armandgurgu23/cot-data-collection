import typer
from src.data_collection.cot_data_reader import COT_Data_Reader
import pendulum
import os
import pandas as pd
from pprint import pprint

DATETIME_FORMAT = "YYYY-MM-DD_HH:mm:ss"

def get_current_datetime():
    return pendulum.now().format(DATETIME_FORMAT)

def log_results(filtered_data:pd.DataFrame):
    print('-' * 100 + '\n')
    print(f'\n\nI FOUND {filtered_data.shape[0]} RESULTS: \n\n')
    for index, curr_result in enumerate(filtered_data.to_dict('records')):
        print('#' * 100 + f'RESULT {index}\n')
        pprint(curr_result)
    print('\n' + '-' * 100)
    return
   
def main(dataset_name: str, output_path:str, fetch_new_data:bool, cached_dataset_path:str):

    if fetch_new_data:
        print(f'Querying new data dump for dataset: {dataset_name}')
        data_fetcher = COT_Data_Reader(dataset_name=dataset_name)
        raw_data, last_updated = data_fetcher()
        last_updated = pendulum.parse(last_updated).format(DATETIME_FORMAT)
        execution_time = get_current_datetime()
        full_output_path = os.path.join(output_path, f"{dataset_name}_execution_at_{execution_time}_last_updated_at_{last_updated}.parquet")
        raw_data.to_parquet(
            full_output_path
        )
        print(f'Finished fetching data from City of Toronto! Dataset stored at: {full_output_path}')
    else:
        print('Fetching existing data dump from cache!')
        assert os.path.isfile(cached_dataset_path), 'Cached dataset isnt a valid path!'
        raw_data = pd.read_parquet(cached_dataset_path)
    
    # Request postal code / address and unit information from user.
    postal_code = input("Enter the first 3 digits of the postal code: ").upper()
    address = input('Enter the address to be searched: ').lower()
    unit_nr = input('Enter the unit number: ')

    # Apply first filtering using postal code.
    filtered_data = raw_data[raw_data['postal_code'] == postal_code]
    address_match = [curr_address for curr_address in filtered_data.address.unique() if address in curr_address.lower()]
    assert len(address_match) == 1, f'Searching for an address lead to {address_match} results!'

    if unit_nr:
        filtered_data = filtered_data[(filtered_data['address'] == address_match[0]) & (filtered_data['unit'] == unit_nr)]
    else:
        filtered_data = filtered_data[(filtered_data['address'] == address_match[0])]

    log_results(filtered_data)


if __name__ == "__main__":
    typer.run(main)
