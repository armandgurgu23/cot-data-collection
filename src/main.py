import typer


def main(dataset_name: str):
    print(f"Hello {dataset_name}")


if __name__ == "__main__":
    typer.run(main)
