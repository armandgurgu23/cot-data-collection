import typer
from src.data_collection.cot_data_reader import COT_Data_Reader


def main(dataset_name: str):

    data_fetcher = COT_Data_Reader(dataset_name=dataset_name)
    data_fetcher()


if __name__ == "__main__":
    typer.run(main)
