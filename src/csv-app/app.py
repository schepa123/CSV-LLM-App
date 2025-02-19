import streamlit as st
import pandas as pd
import os
import data_processing
import llm
import utils
import asyncio

agent = llm.LLMAgent()

if "started" not in st.session_state:
    st.session_state.started = True
if "previous_file_name" not in st.session_state:
    st.session_state.previous_file_name = None
if "llm_suggestions" not in st.session_state:
    st.session_state.llm_suggestions = None
if "current_table" not in st.session_state:
    st.session_state.current_table = None
if "data_corrected" not in st.session_state:
    st.session_state.data_corrected = False

if st.session_state.started:
    utils.drop_all_tables()
    utils.create_empty_table()
    st.session_state.started = False

st.title("CSV Error Handling App")
uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

conn = data_processing.get_connection()

if uploaded_file is not None:
    current_file_name = uploaded_file.name
    if current_file_name != st.session_state.previous_file_name:
        data_processing.create_table(uploaded_file)
        st.session_state.previous_file_name = current_file_name
        st.success(f"File '{current_file_name}' successfully processed!")

db_path = os.getenv("DB_PATH")
select_table = st.selectbox(
    label="Select a table",
    options=utils.get_all_tables()
)
if select_table is None:
    st.warning("No tables found in the database. Please upload a CSV file!")
else:
    if select_table != st.session_state.current_table:
        st.session_state.current_table = select_table
        query = f"SELECT * FROM {select_table}"
        conn = data_processing.get_connection()
        df_original = pd.read_sql_query(query, conn)
        df_error = data_processing.return_erroneous_data(select_table)

        suggestions = asyncio.run(agent.send_missing_values_to_llm(
            df_original=df_original,
            df_error=df_error
        ))
        st.session_state.llm_suggestions = agent.gather_respones(suggestions)
    else:
        query = f"SELECT * FROM {select_table}"
        conn = data_processing.get_connection()
        df_original = pd.read_sql_query(query, conn)
        df_error = data_processing.return_erroneous_data(select_table)

    st.write(f"**Selected table**: {select_table}")
    st.write(df_original)
    st.write("**Erroneous data**")
    st.write(df_error)

    corrections_list = []
    for index, row in df_error.iterrows():
        corrected_values = utils.display_error(
            df=df_error,
            index=index,
            llm_suggestions=st.session_state.llm_suggestions
        )
        corrections_list.append(corrected_values)

    corrections_dict = {
        key: value for corr_dict in corrections_list
        for key, value in corr_dict.items()
    }

    if st.button("Save corrections"):
        data_processing.save_corrections(
            corrections_dict,
            df_error,
            select_table,
        )
        st.session_state.current_table = None
        st.session_state.data_corrected = True

    if st.session_state.data_corrected:
        corrected_data = utils.convert_df(select_table)
        st.write("Data corrected and saved!")
        st.write("Please download the corrected data!")
        st.download_button(
            label="Download CSV",
            data=corrected_data,
            file_name=f"{select_table}_corrected.csv",
            mime="text/csv"
        )
