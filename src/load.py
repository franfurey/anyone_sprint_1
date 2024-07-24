from typing import Dict
from pandas import DataFrame
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

from src.extract import extract
from src.config import DATASET_ROOT_PATH, PUBLIC_HOLIDAYS_URL, SQLITE_BD_ABSOLUTE_PATH, get_csv_to_table_mapping

def load(data_frames: Dict[str, DataFrame], database: Engine):
    """Load the dataframes into the sqlite database.

    Args:
        data_frames (Dict[str, DataFrame]): A dictionary with keys as the table names
        and values as the dataframes.
        database (Engine): A SQLAlchemy Engine instance connected to the SQLite database.
    """
    for table_name, df in data_frames.items():
        df.to_sql(name=table_name, con=database, if_exists='replace', index=False)

    print("All dataframes have been loaded into the database.")

def main():
    # Configurar la base de datos
    database_url = f"sqlite:///{SQLITE_BD_ABSOLUTE_PATH}"
    engine = create_engine(database_url)

    # Extraer los datos
    csv_folder = DATASET_ROOT_PATH
    csv_table_mapping = get_csv_to_table_mapping()
    data_frames = extract(
        csv_folder=csv_folder,
        csv_table_mapping=csv_table_mapping,
        public_holidays_url=PUBLIC_HOLIDAYS_URL
    )

    # Cargar los datos en la base de datos
    load(data_frames=data_frames, database=engine)

if __name__ == "__main__":
    main()
