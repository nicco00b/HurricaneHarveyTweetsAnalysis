import plotly.express as px
import streamlit as st
from pyspark.sql.functions import desc

#from Backend import SparkBuilder

def show(spark):
    st.markdown("## Numero totale di tweet per ogni :red[categoria] ")
    st.markdown("##### Il grafico a torta rappresenta, in percentuale, le categorie e il loro numero totale di tweet ")
    bottone1 = st.button(
            label="Disegna grafico"
        )
    if bottone1:
        with st.spinner("Disegnando il grafico"):
            result = spark.query.number_of_tweet_for_category()
            ds_pandas = result.toPandas()
            fig = px.pie(ds_pandas, values=ds_pandas["Numero di tweet"], names=ds_pandas["AIDRLabel"], title='Categorie e numero di tweet')
            st.plotly_chart(fig)
            nome = result.orderBy(desc("Numero di tweet")).first()["AIDRLabel"]
            num_tweet = result.orderBy(desc("Numero di tweet")).first()["Numero di tweet"]
            st.markdown("##### La categoria con il maggior numero di tweet è :red["+nome+"] con :red["+str(num_tweet)+"] tweet")

    st.divider()
    col1, col2  =st.columns(2)
    
    with col1:
        categoria = st.selectbox(
            label= "###### Scegli la categoria da visualizzare",
            options= ("Tutte", "injured_or_dead_people", "relevant_information", "caution_and_advice","displaced_and_evacuations",
                                "sympathy_and_support","response_efforts","infrastructure_and_utilities_damage", "personal",
                                "affected_individual","not_related_or_irrelevant","missing_and_found_people","donation_and_volunteering")
        )

        bottone = st.button(
            label="Esegui query"
        )
    if categoria.__contains__( "Tutte"):
        categoria = None
    with col2:
        st.markdown("#### :red[-] Si visualizza la distribuzione dei tweet per ogni :red[giorno] (è possibile specificare la categoria) ")
        if bottone:
            with st.spinner("Disegnando il grafico"):
                result = spark.query.tweets_distribution_for_category(categoria)
                st.line_chart(result, x="Giorno", y= "Tweet per giorno")
