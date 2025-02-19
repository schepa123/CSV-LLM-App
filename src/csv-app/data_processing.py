import sqlite3
import pandas as pd
from dotenv import load_dotenv
import os
import utils
from io import BytesIO


def get_connection() -> sqlite3.Connection:
    """
    Get connection to the SQLite database.

    Parameters
    ----------
    None

    Returns
    -------
    conn: sqlite3.Connection
        Connection object to the SQLite database
    """
    load_dotenv()
    db_path = os.getenv("DATBASE")
    conn = sqlite3.connect(db_path)
    return conn


def create_table(uploaded_csv: BytesIO) -> None:
    """
    Takes a CSV file and creates a table in the SQLite database
    with the same name as the file.

    Parameters
    ----------
    uploaded_csv: BytesIO
        The uploaded CSV file

    Returns
    -------
    None
    """
    file_name = utils.remove_invalid_characters(uploaded_csv.name.split(".")[0])
    df = pd.read_csv(uploaded_csv)
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = [
        utils.remove_invalid_characters(col) for col in df.columns
    ]
    conn = get_connection()
    cursor = conn.cursor()

    columns = [
        f"{col} {utils.map_dtype_to_sql(df[col].dtype)}" for col in df.columns
    ]
    cursor.execute(f"DROP TABLE IF EXISTS {file_name}")
    print(f"CREATE TABLE {file_name} ({(", ").join(columns)})")
    cursor.execute(f"CREATE TABLE {file_name} ({(", ").join(columns)})")
    fill_table(table_name=file_name, df=df)

    conn.commit()
    conn.close()


def fill_table(table_name: str, df: pd.DataFrame) -> None:
    """
    Takes a DataFrame and inserts it into the SQLite database.

    Parameters
    ----------
    table_name: str
        The name of the table
    df: pd.DataFrame
        The DataFrame to insert

    Returns
    -------
    None
    """
    conn = get_connection()
    df.to_sql(table_name, conn, if_exists="append", index=False)
    conn.close()


def return_erroneous_data(table_name: str) -> pd.DataFrame:
    """
    Returns the rows where at least one value is missing.

    Parameters
    ----------
    table_name: str
        The name of the table

    Returns
    -------
    df: pd.DataFrame
        The DataFrame with the erroneous rows
    """
    conn = get_connection()
    where_clause = " OR ".join(
        [f"{col} IS NULL" for col in utils.get_all_columns(table_name)]
    )
    query = f"SELECT * FROM {table_name} uploaded_file WHERE 1=1 AND {where_clause}"
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def map_dtype_to_where(
    dtype: pd.api.types.CategoricalDtype, col: str, value: str
) -> str:
    """
    Maps the dtype to a SQL WHERE clause. Adds single quotes
    around the value if it is not a number or bool.

    Parameters
    ----------
    dtype: pd.api.types.CategoricalDtype
        The dtype of the column
    col: str
        The name of the column
    value: str
        The value to map

    Returns
    -------
    str
        The mapped value
    """
    if (
        pd.api.types.is_float_dtype(dtype)
        or pd.api.types.is_integer_dtype(dtype)
        or pd.api.types.is_bool_dtype(dtype)
    ):
        return f"{col}={value}"
    else:
        return f"{col}='{value}'"


def save_corrections(corrections_dict, df, table_name) -> None:
    """
    Saves the corrections to the SQLite database.

    Parameters
    ----------
    corrections_dict: dict
        A dictionary with the corrections
    df: pd.DataFrame
        The DataFrame with the erroneous rows
    table_name: str
        The name of the table

    Returns
    -------
    None
    """
    conn = get_connection()
    cursor = conn.cursor()

    for key, value in corrections_dict.items():
        temp = df.iloc[[key]]
        columns = [col for col in temp.columns if not pd.isnull(temp[col].values[0])]
        where_clause = " AND ".join(
            [
                f"{map_dtype_to_where(df[col].dtype, col, temp[col].values[0])}"
                for col in columns
            ]
        )
        update_queries = []
        for key_corr, val_corr in value.items():
            set_value = map_dtype_to_where(df[key_corr].dtype, key_corr, val_corr)
            update_queries.append(
                f"UPDATE {table_name} SET {set_value} WHERE {where_clause}"
            )
        for query in update_queries:
            cursor.execute(query)

    conn.commit()
    conn.close()
