import streamlit as st
import Classification as cl

def show(spark):
    st.markdown("## Esecuzione del modello di machine learning")
    st.markdown("##### Seleziona un modello di machine learning tra quelli elencati e visualizza i risultati di predizione")
    col1, col2 = st.columns(2)
    with col1:
        scelta = st.radio(
            label="###### Scegli il modello : ",
            options=("Nessuno","Logistic Regression", "Naive Bayes", "Random Forest")
        )
        flag = True
        modello = -1
        if scelta == "Logistic Regression":
            modello = 0
            flag = False
        elif scelta == "Naive Bayes":
            modello = 1
            flag = False
        elif scelta == "Random Forest":
            modello = 2
            flag = False
        
        bottone = st.button(
            label="Esegui il modello",
            disabled= flag
        )

    if bottone : 
        with col2:
            with st.spinner("Eseguendo il modello..."):
                classification = cl.Classification(spark.dataset_completo, ["relevant_information", "not_related_or_irrelevant"], modello)
                accuracy,precision,recall,f1,auc, predictions = classification.main()
                st.markdown("##### :red[Accuracy] : "+str(accuracy))
                st.markdown("##### :red[Precision] : "+str(precision))
                st.markdown("##### :red[Recall] : "+str(recall))
                st.markdown("##### :red[F1] : "+str(f1))
                st.markdown("##### :red[Auc] : "+str(auc))
        with st.spinner("Disegnando la tabella..."):
            st.markdown("#### :red[Campione del test set]")
            st.table(predictions.select("text", "label", "prediction").limit(20))
