from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("FirstApp").getOrCreate()

import streamlit as st
st.set_page_config(page_title="Data Comparator",
                   page_icon=":bar_chart:",
                   layout="wide"
                   )

st.header("Data Comparator")
container = st.container()
sidebar = st.sidebar.header("Input Form")
col1 = sidebar.columns(1)
with sidebar:
    with st.form("input form"):
        source_path = st.text_input("Source Path", placeholder="Enter source data path")
        destination_path = st.text_input("Destination Path", placeholder="Enter destination data path" )
        selected_parameter = st.selectbox('Select parameter:',
                                    ('Missing records in destination', 'count of columns',))
        st.sidebar.write('You selected:', selected_parameter)
        submit = st.form_submit_button("SHOW")
        if submit:
            source_df = spark.read.csv(source_path, header=True)
            count_source_rows = source_df.count()
            print(count_source_rows)
            destination_df = spark.read.csv(destination_path, header=True)
            count_destination_rows = destination_df.count()
            print(count_destination_rows)
            difference = abs(count_destination_rows - count_source_rows)
            if difference==0:
                container.write("Match")
            else:
                container.write(f"{difference} records are not matching")
                container.header("Missing records in destination")
                container.write("_"*30)
                result_df = source_df.subtract(destination_df)
                container.dataframe(result_df.toPandas())
