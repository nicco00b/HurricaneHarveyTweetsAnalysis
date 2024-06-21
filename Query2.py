import streamlit as st
import plotly.express as px

def show(spark):
    st.markdown("## :red[Hashtag più utilizzati] nei tweet")
    st.markdown("##### La tabella riporta i top hashtag più utilizzati nei tweet e, scegliendo le categorie, la distribuzione \
                di occorrenze degli hashtag più utilizzati per categoria")
    col1, col2 = st.columns(2)
    with col1:
        with st.markdown("###### Scegli le categorie da visualizzare"):
            scelta = st.multiselect(
            label="###### Scegli le categorie",
            placeholder="Scegli",
            options=("Tutte", "injured_or_dead_people", "relevant_information", "caution_and_advice","displaced_and_evacuations",
                                            "sympathy_and_support","response_efforts","infrastructure_and_utilities_damage", "personal",
                                            "affected_individual","not_related_or_irrelevant","missing_and_found_people","donation_and_volunteering")   
            )
    if scelta == []:
            flag = True
    else:
            flag = False
    bottone = st.button(
            label="Esegui query",
            disabled= flag
        )  

    if scelta.__contains__("Tutte"):
        scelta = [ "injured_or_dead_people", "relevant_information", "caution_and_advice","displaced_and_evacuations",
                                            "sympathy_and_support","response_efforts","infrastructure_and_utilities_damage", "personal",
                                            "affected_individual","not_related_or_irrelevant","missing_and_found_people","donation_and_volunteering"]
        
    with col2:
        s = st.radio(label="###### Scegli il numero di hashtag da visualizzare",
                options=("5", "10", "20", "50")
                )
        n = int(s)

    if bottone :
        with st.spinner("Disegnando il grafico..."):
            result = spark.query.top_n_hashtag(scelta, n)
            st.dataframe(result)
        
