import streamlit as st

def show(spark):
    st.markdown("## Analisi dei :red[termini più utilizzati]")
    st.markdown("##### La tabella riporta i termini più utilizzati nei tweet per ogni categoria e il loro numero di occorrenze.")
    bottone1 = st.button(
            label="Riporta tabella"
        )
    if bottone1:  
        with st.spinner("Disegnando il grafico..."):
            result = spark.query.max_word_for_category()
            st.dataframe(result, height=500)

    st.divider()

    st.markdown("##### Questa tabella, oltre a mostrare le parole maggiormente utilizzate nei tweet indipendentemente dalla categoria\
                 e le loro realtive occorrenze, riporta come e in che modo le singole categorie si adattano all'andamento generale. Si può notare come ogni \
                categoria abbia la propria distribuzione di occorrenze fra i diversi termini seppur conservando alcune similarità \
                con le altre.")
    col1, col2 = st.columns(2)
    with col1:
        scelta = st.multiselect(
                        label="###### Scegli le categorie da visualizzare",
                        options= ("Tutte", "injured_or_dead_people", "relevant_information", "caution_and_advice",
                                  "displaced_and_evacuations", "sympathy_and_support","response_efforts",
                                  "infrastructure_and_utilities_damage", "personal", "affected_individual","not_related_or_irrelevant",
                                  "missing_and_found_people","donation_and_volunteering"),
                        placeholder="Scegli"
                )
        if scelta == []:
            flag = True
        else:
                flag = False
        bottone = st.button(
                label="Esegui query",
                disabled= flag
            )  
        
    with col2:
        s = st.radio(
            label="###### Scegli quante parole visualizzare (ordinate per numero di occorrenze totali)",
            options= ("5", "10", "20")
        )
        n = int(s)  

    if scelta.__contains__( "Tutte"):
            scelta = ["injured_or_dead_people", "relevant_information", "caution_and_advice","displaced_and_evacuations",
                                    "sympathy_and_support","response_efforts","infrastructure_and_utilities_damage", "personal",
                                    "affected_individual","not_related_or_irrelevant","missing_and_found_people","donation_and_volunteering"]
    
    if bottone:
        with st.spinner("Disegnando il grafico..."):
            ds = spark.query.process_text_data(scelta, n)
            st.dataframe(ds)