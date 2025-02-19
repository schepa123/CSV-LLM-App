import pandas as pd
import data_processing
import streamlit as st
import numpy


def map_dtype_to_sql(dtype) -> str:
    """
    Takes a pandas dtype and maps it to a SQLite data type.

    Parameters
    ----------
    dtype: numpy.dtype
        The dtype of a pandas Series

    Returns
    -------
    str
        The SQLite data type that corresponds to the input dtype
    """
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "REAL"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    else:
        return "TEXT"


def get_all_columns(table_name) -> list:
    """
    Get all columns from a table in the SQLite database.

    Parameters
    ----------
    table_name: str
        The name of the table

    Returns
    -------
    columns: list
        A list of column names
    """
    conn = data_processing.get_connection()
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [col[1] for col in cursor.fetchall()]
    conn.close()
    return columns


def get_all_tables() -> list:
    """
    Get all tables from the SQLite database.

    Parameters
    ----------
    None

    Returns
    -------
    tables: list
        A list of table names
    """
    conn = data_processing.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [table[0] for table in cursor.fetchall()]
    conn.close()
    return tables


def drop_all_tables() -> None:
    """
    Drops all tables from the SQLite database.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    conn = data_processing.get_connection()
    cursor = conn.cursor()
    cursor.execute("PRAGMA writable_schema = 1;")
    cursor.execute(
        """
        delete from sqlite_master
        where type in ('table', 'index', 'trigger');
    """
    )
    cursor.execute("PRAGMA writable_schema = 0;")
    cursor.execute("CREATE TABLE IF NOT EXISTS Choose (id INTEGER);")
    conn.commit()
    conn.close()


def create_empty_table() -> None:
    """
    Create an empty table in the SQLite database.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    conn = data_processing.get_connection()
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS Choose (id INTEGER);")
    conn.commit()
    conn.close()

def remove_invalid_characters(table_name) -> str:
    """
    Remove invalid characters from a table name.

    Parameters
    ----------
    table_name: str
        The name of the table

    Returns
    -------
    str
        The table name without invalid characters
    """
    return (
        table_name.replace(" ", "_")
        .replace("-", "_")
        .replace("!", "")
        .replace("%", "")
        .replace("^", "")
        .replace("&", "")
        .replace("(", "")
        .replace(")", "")
        .replace("{", "")
        .replace("}", "")
        .replace("'", "")
        .replace(".", "")
        .replace("\\", "")
        .replace("`", "")
        .replace("/", "")
    )


def return_proper_selection(
    dtype: numpy.dtypes.ObjectDType, key: str
) -> int | float | str:
    """
    Returns the proper selection widget based on the dtype of the column.

    Parameters
    ----------
    dtype: numpy.dtypes.ObjectDType
        The dtype of the column
    key: str
        The key of the widget

    Returns
    -------
    int | float | str
        The value selected by the user
    """
    key = f"{key}_input"
    if pd.api.types.is_integer_dtype(dtype):
        return st.number_input(
            label="Your own Input:",
            step=1,
            key=key
        )
    elif pd.api.types.is_float_dtype(dtype):
        return st.number_input(
            label="Your own Input:",
            key=key
        )
    elif pd.api.types.is_bool_dtype(dtype):
        return st.pills(
            label="Your own Input",
            options=["True", "False"],
            key=key
        )
    else:
        return st.text_input(label="Your own Input:", key=key)


def display_error(df: pd.DataFrame, index: int, llm_suggestions: dict):
    """
    Display and handle errors in DataFrame rows with state persistence.
    Returns a dictionary of corrections for the current row.
    """
    row_key = f"row_{index}_state"
    if row_key not in st.session_state:
        st.session_state[row_key] = {"decisions": {}, "custom_values": {}}

    updated_values = {index: {}}
    df_temp = df.iloc[[index]]
    st.write(f"### Row {index + 1} contains an error:")

    for col in df_temp.columns:
        if pd.isnull(df_temp[col].values[0]):
            col_key = f"{index}_{col}"

            if col not in st.session_state[row_key]["decisions"]:
                st.session_state[row_key]["decisions"][col] = "Yes"
                st.session_state[row_key]["custom_values"][col] = None

            suggestion = llm_suggestions[index][col]
            st.write(f"❗ Column '{col}' contains a NULL value.")

            def on_radio_change():
                st.session_state[row_key]["decisions"][col] = (
                    st.session_state[f"{col_key}_radio"]
                )

            agree = st.radio(
                f"We suggest the value {suggestion}. Do you accept this suggestion?",
                options=["Yes", "No, I want to pick my own value"],
                key=f"{col_key}_radio",
                horizontal=True,
                on_change=on_radio_change,
                index=0 if st.session_state[row_key]["decisions"][col] == "Yes" else 1,
            )

            if agree == "Yes":
                updated_values[index][col] = suggestion
            else:
                if f"{col_key}_input" not in st.session_state:
                    st.session_state[f"{col_key}_input"] = (
                        st.session_state[row_key]["custom_values"][col]
                    )

                custom_value = return_proper_selection(
                    df_temp[col].dtype, col_key
                )

                if custom_value:
                    st.session_state[row_key]["custom_values"][col] = (
                        custom_value
                    )
                    updated_values[index][col] = custom_value
                elif st.session_state[row_key]["custom_values"][col] is not None:
                    updated_values[index][col] = st.session_state[row_key][
                        "custom_values"
                    ][col]

    return updated_values


def convert_df(table_name) -> pd.DataFrame:
    """
    Load a table from the SQLite database and return it as a DataFrame.

    Parameters
    ----------
    table_name: str
        The name of the table

    Returns
    -------
    pd.DataFrame
        The DataFrame representation of the table
    """
    conn = data_processing.get_connection()
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df.to_csv().encode("utf-8")
