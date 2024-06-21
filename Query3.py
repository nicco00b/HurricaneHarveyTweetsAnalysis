import streamlit as st
import plotly.express as px

def show(spark):
    st.markdown("## Tweet con :red[media]")
    st.markdown("##### I due grafici riportano, per ogni categoria, la percentuale dei tweet a cui è allegato un contenuto multimediale \
                (come foto, video, gif, ecc...).")
    with st.spinner("Disegnando i grafici..."):
        result = spark.query.media_for_category()
        st.dataframe(result, height=500)
        st.divider()
        st.bar_chart(result, x="AIDRLabel", y="percentuale %")
        
    
