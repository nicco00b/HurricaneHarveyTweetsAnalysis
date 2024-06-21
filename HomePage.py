import streamlit as st
#import plotly.express as px
from Backend import SparkBuilder
import Query1, Query2, Query3, Query4, QueryUtenti, QueryUtenti2, EsecuzioneModello, RisultatiModello

st.set_page_config(
    page_title="Analisi tweet uragano Harvey",
    page_icon="🌪️",
    layout="wide",
    initial_sidebar_state="expanded",
)

@st.cache_resource
def getSpark():
    return SparkBuilder("appName")

with st.spinner("Caricando la pagina..."):
    spark = getSpark()

# HomePage
def HomePage():
    st.markdown("## Analisi dei tweet sull'uragano :blue[Harvey] 🌪️")
    st.markdown("#### :red[Uragano Harvey]")
    st.markdown("##### L'uragano Harvey è stato uno dei più devastanti uragani ad aver colpito\
                 gli Stati Uniti negli ultimi decenni, causando danni estesi, inondazioni catastrofiche e perdite umane. Harvey si \
                forma il 17 agosto 2017 come una depressione tropicale nell'Atlantico orientale, inizialmente indebolito, si \
                riorganizza nel Golfo del Messico e inizia a intensificarsi rapidamente. Il 25 agosto, diventa un uragano di categoria \
                4 sulla scala Saffir-Simpson, con venti sostenuti fino a 215 km/h. Tocca terra vicino a Rockport, Texas, il 25 \
                agosto, portando con sé venti forti e piogge torrenziali. Staziona sul Texas per diversi giorni, scaricando quantità \
                eccezionali di pioggia, soprattutto nell'area di Houston e nelle contee circostanti. Alcune aree hanno registrato \
                oltre 1000 mm di pioggia, un record per il Texas. Dopo circa 4 giorni l'uragano comincia a perdere potenza fino ad \
                esaurirsi completamente facendo segnare un bilancio di almeno 68 morti e una stima complessiva di circa 125 miliardi \
                di dollari di danni")
    
    st.markdown("#### :red[Dataset]")
    st.markdown("##### Il dataset analizzato si compone dei tweet relativi all'uragano Harvey postati tra il 4 Settembre 2017 ed il XX \
                Settembre 2017. Oltre alle colonne caratteristiche dell'oggetto tweet, il dataset possiede una colonna chiamata \
                 *AIDRLabel*  che classifica il tweet in una delle seguenti categorie : ")
    st.markdown(""" 
                - ##### *injured_or_dead_people* : se il tweet parla di persone ferite o rimaste uccise
                - ##### *relevant_information* : se il tweet contiene informazioni rilevanti
                - ##### *caution_and_advice* : se il tweet contiene avvertimenti o avvisi riguardanti il disastro che possono essere utili
                - ##### *displaced_and_evacuations* : se il tweet contiene informazioni su evacuazioni o persone sfollate
                - ##### *sympathy_and_support* : se il tweet mostra supporto per le vittime
                - ##### *response_efforts* : se il tweet è correlato alle risposte di aiuto
                - ##### *infrastructure_and_utilities_damage* : se il tweet riporta danni ad infrastrutture
                - ##### *personal* : se il tweet riporta aggiornamenti personali, soprattutto della situazione e della salute
                - ##### *affected_individual* : se il tweet è scritto da persone colpite dalla catasrtofe 
                - ##### *not_related_or_irrelevant* : se il tweet è poco o per niente rilevante con l'uragano
                - ##### *missing_and_found_people* : se il tweet riporta di persone scomparse o ritrovate
                - ##### *donation_and_volunteering* : se il tweet contiene richieste o invii di donazioni e volontariato
            """)
    

    dataset = spark.dataset_completo
    col1, col2 = st.columns(2)

    with st.spinner("Calcolando il numero di tweet"), col1:
        st.subheader("Numero totale di tweet : :red["+str(dataset.count())+"]")
        
    with col2:
        st.subheader("Numero totale di attributi : :red["+str(len(dataset.columns))+"]")

    st.markdown("#### :red[Queries]")
    st.markdown("##### Le queries effettuate forniscono un'analisi di carattere generale sul numero di tweet e sulla loro distribuzione \
                nel tempo, concentrandosi anche su tre diversi componenti del tweet quali gli utenti, gli hashtags e il corpo del tweet. \
                Nella barra laterale è possibile selezionare una pagina in cui verranno mostrati grafici e tabelle di una o \
                più queries dello stesso topic.")
    


# Crea un menu di navigazione nella barra laterale
pagina_selezionata = st.sidebar.selectbox("Seleziona una pagina", ["HomePage", "Distribuzione dei tweet",\
                                                                    "Top hashtag utilizzati", "Tweet con media",\
                                                                    "Analisi dei termini più utilizzati", "Utenti più menzionati",
                                                                    "Top utenti per categoria","Risultati del modello di classificazione", "Esecuzione Modello"])

# Mostra la pagina selezionata

if pagina_selezionata == "HomePage":
    HomePage()
elif pagina_selezionata == "Distribuzione dei tweet":
    Query1.show(spark)
elif pagina_selezionata == "Top hashtag utilizzati":
    Query2.show(spark)
elif pagina_selezionata == "Tweet con media":
    Query3.show(spark)
elif pagina_selezionata == "Analisi dei termini più utilizzati":
    Query4.show(spark)
elif pagina_selezionata == "Utenti più menzionati":
    QueryUtenti.show(spark)
elif pagina_selezionata == "Top utenti per categoria":
    QueryUtenti2.show(spark)
elif pagina_selezionata == "Esecuzione Modello":
    EsecuzioneModello.show(spark)
elif pagina_selezionata == "Risultati del modello di classificazione":
    RisultatiModello.show(spark)
