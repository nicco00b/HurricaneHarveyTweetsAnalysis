import streamlit as st

def show(spark):
    st.markdown("## Risultati dei modelli di classificazione")
    #st.markdown("##### Si mostrano i risultati di 3 modelli di classificazione diversi")
    st.markdown("#### Modello di apprendimento Logistic Regression")
    st.markdown("""
                - :red[Accuracy] : 0.9832815256657985
                - :red[Precision] : 0.9832294139131377
                - :red[Recall] : 0.9832815256657985
                - :red[F1] : 0.9832164501909502
                - :red[AUC] : 0.996134615802398
                """)
    st.divider()
    st.markdown("#### Modello di apprendimento Naive Bayes")
    st.markdown("""
                - :red[Accuracy] : 0.8739009130875888 
                - :red[Precision] : 0.8951250361753855
                - :red[Recall] : 0.8739009130875888
                - :red[F1] : 0.8791614179795341
                - :red[AUC] : 0.442633282732909
                """)
    st.divider()
    st.markdown("#### Modello di apprendimento Random Forest")
    st.markdown("""
                - :red[Accuracy] : 0.8270629015894487
                - :red[Precision] : 0.8591178909237592
                - :red[Recall] : 0.8270629015894487
                - :red[F1] : 0.7871770133856701
                - :red[AUC] : 0.986390891414076
                """)