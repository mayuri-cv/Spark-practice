import pandas as pd
#import plotly.express
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
                                    ('count of rows', 'count of columns',))
        st.sidebar.write('You selected:', selected_parameter)
        submit = st.form_submit_button("Compare")
        if submit:
            #source_df = pd.read_csv("resources/annual_survey.csv")
            try:
                #source_df = pd.read_csv(source_path)
                source_df = spark.read.csv(source_path,header=True)
                count_source_rows= source_df.count()
                print(count_source_rows)
            except:
                st.error("Invalid source path")
            else:
                try:
                    #destination_df = pd.read_csv(destination_path)
                    destination_df = spark.read.csv(destination_path)
                    count_destination_rows = destination_df.count()
                    print(count_destination_rows)
                except:
                    st.error("Invalid destination path")
                else:
                    if count_destination_rows==count_destination_rows:
                        container.write("Match")
                    else:
                        container.write("No match")
                    #new_df = source_df.toPandas()
                   # container.dataframe(new_df)
#Collapse










