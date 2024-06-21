import streamlit as st

def show(spark):
    st.markdown("## Top utenti per categoria")
    st.markdown("##### La tabella riporta gli utenti che hanno twittato di più sull'uragano Harvey. Specificando le categorie vengono \
                mostrati gli utenti con il maggior numero di tweet per quelle categorie e il numero di tweet in percentuale per \
                ciascuna categoria")
    col1, col2 = st.columns(2)
    with col1:
        s = st.radio(
            label="###### Scegli il numero di utenti da visualizzare",
            options=("5", "10", "20")
        )
        n = int(s)
    with col2:
        scelta = st.multiselect(
            label="###### Scegli le categorie",
            options=("Tutte", "injured_or_dead_people", "relevant_information", "caution_and_advice","displaced_and_evacuations",
                                            "sympathy_and_support","response_efforts","infrastructure_and_utilities_damage", "personal",
                                            "affected_individual","not_related_or_irrelevant","missing_and_found_people","donation_and_volunteering"),
            placeholder="Nessuna",
            default=[]
        )
    if scelta == []:
        flag = True
    else:
        flag = False
    bottone1 = st.button(
            label="Esegui query",
            disabled= flag
        )  

    if scelta.__contains__("Tutte"):
        scelta = [ "injured_or_dead_people", "relevant_information", "caution_and_advice","displaced_and_evacuations",
                                            "sympathy_and_support","response_efforts","infrastructure_and_utilities_damage", "personal",
                                            "affected_individual","not_related_or_irrelevant","missing_and_found_people","donation_and_volunteering"]
        
    if bottone1:
        with st.spinner("Disegnando il grafico..."):
            result = spark.query.top_n_user(scelta, n)
            st.dataframe(result)

