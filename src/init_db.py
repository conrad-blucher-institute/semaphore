import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))
from dotenv import load_dotenv
from SeriesStorage.SeriesStorage.SQLAlchemyORM import SQLAlchemyORM

def main():
    load_dotenv()
    sqlorm = SQLAlchemyORM()
    sqlorm.drop_DB()
    sqlorm.create_DB()


if __name__ == "__main__":
    main()