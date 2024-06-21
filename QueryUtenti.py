import streamlit as st

def show(spark):
    st.markdown("## Analisi degli utenti maggiormente :red[citati] e degli utenti più :red[influenti]")
    col1, col2= st.columns(2)
    #scelta = "Tutte"
    with col1:
        scelta = st.multiselect(
                label= "###### Scegli le categorie",
                placeholder="Scegli",
                options= ("Tutte", "injured_or_dead_people", "relevant_information", "caution_and_advice","displaced_and_evacuations",
                                            "sympathy_and_support","response_efforts","infrastructure_and_utilities_damage", "personal",
                                            "affected_individual","not_related_or_irrelevant","missing_and_found_people","donation_and_volunteering")
            )
        if scelta == []:
            flag = True
        else:
            flag = False
        bottone1 = st.button(
                label="Esegui query",
                key=1,
                disabled= flag
            )  
    if scelta.__contains__("Tutte"):
            scelta = ["injured_or_dead_people", "relevant_information", "caution_and_advice","displaced_and_evacuations",
                                        "sympathy_and_support","response_efforts","infrastructure_and_utilities_damage", "personal",
                                        "affected_individual","not_related_or_irrelevant","missing_and_found_people","donation_and_volunteering"]
    with col2:
        s = st.radio(
            label= "###### Scegli il numero di utenti",
            options= ("5", "10", "20", "50")
        )
    n = int(s) 
   
    
    st.markdown("### Top "+s+" utenti :red[più citati] nei tweet")
    st.markdown("##### In questo grafico si visualizzano gli utenti di Twitter maggiormente menzionati (taggati) in altri tweet. \
                Per ogni utente nel grafico, si riporta il nome e il numero di menzioni  ottenute.")
    if bottone1:
        with st.spinner("Disegnando il grafico..."):
            result1 = spark.query.top_mentioned_for_categories(scelta, n)
            st.bar_chart(result1, x="Nome", y="Occorrenze" )
            nome1 = result1.first()["Nome"]
            num_menz = result1.first()["Occorrenze"]
            st.markdown("##### L'utente più citato è :red["+nome1+"] con :red["+str(num_menz)+"] menzioni")
    
    st.divider()
    st.markdown("### Top "+s+" utenti :red[più influenti]")
    st.markdown("##### In questo grafico si visualizzano gli utenti più influenti (ovvero con il maggior numero di \
                followers) che hanno twittato almeno una volta per le categorie selezionate. Per ogni utente, si riporta il nome, il numero di \
                followers e il numero totale di tweet postati.")
    if scelta == []:
            flag = True
    else:
            flag = False
    bottone2 = st.button(
         label="Esegui query",
         key=2,
         disabled= flag
    )
    if bottone2:
        with st.spinner("Disegnando il grafico..."):
           
            result2 = spark.query.most_influents_users(scelta, n)
            st.dataframe(result2)
            nome = result2.first()["Nome"]
            num_foll = result2.first()["Followers"]
            st.markdown("##### L'utente più influente che ha twittato almeno una volta sull'uragano Harvey è :red["+nome+"] con :red["+str(num_foll)+"] followers")
