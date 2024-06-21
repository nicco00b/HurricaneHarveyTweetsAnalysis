import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType,LongType,TimestampType, DoubleType
from pyspark.sql.functions import count,explode,desc,col,when,lower,split,regexp_replace, to_date,unix_timestamp, from_unixtime, date_format, asc, round
from pyspark.sql import functions as F
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover

class SparkBuilder:

    def __init__(self, appname: str):
        path1 = "C:\\Users\\Asus\\Desktop\\MAGISTRALE ESAMI FATTI\\BIG DATA\\ProgettoWS\\dataset\\170826213907_hurricane_harvey_2017_20170903_vol-4.json"
        path2 = "C:\\Users\\Asus\\Desktop\\MAGISTRALE ESAMI FATTI\\BIG DATA\\ProgettoWS\\dataset\\170826213907_hurricane_harvey_2017_20170904_vol-5.json"
        #path3 = "C:\\Users\\39331\\Desktop\\dataset_estratto\\170826213907_hurricane_harvey_2017_20170904_vol-6.json\\170826213907_hurricane_harvey_2017_20170904_vol-6.json"
        #path4 = "C:\\Users\\39331\\Desktop\\dataset_estratto\\170826213907_hurricane_harvey_2017_20170905_vol-7.json\\170826213907_hurricane_harvey_2017_20170905_vol-7.json"
        #path5 = "C:\\Users\\39331\\Desktop\\dataset_estratto\\170826213907_hurricane_harvey_2017_20170905_vol-8.json\\170826213907_hurricane_harvey_2017_20170905_vol-8.json"
        #path6 = "C:\\Users\\39331\\Desktop\\dataset_estratto\\170826213907_hurricane_harvey_2017_20170906_vol-9.json\\170826213907_hurricane_harvey_2017_20170906_vol-9.json"
        #path7 = "C:\\Users\\39331\\Desktop\\dataset_estratto\\170826213907_hurricane_harvey_2017_20170906_vol-10.json\\170826213907_hurricane_harvey_2017_20170906_vol-10.json"
        #path8 ="C:\\Users\\39331\\Desktop\\dataset_estratto\\170826213907_hurricane_harvey_2017_20170906_vol-11.json\\170826213907_hurricane_harvey_2017_20170906_vol-11.json"
        #path9 ="C:\\Users\\39331\\Desktop\\dataset_estratto\\170826213907_hurricane_harvey_2017_20170907_vol-12.json\\170826213907_hurricane_harvey_2017_20170907_vol-12.json"
        #path10 ="C:\\Users\\39331\\Desktop\\dataset_estratto\\170826213907_hurricane_harvey_2017_20170907_vol-13.json\\170826213907_hurricane_harvey_2017_20170907_vol-13.json"
        #path11 ="C:\\Users\\39331\\Desktop\\dataset_estratto\\170826213907_hurricane_harvey_2017_20170908_vol-14.json\\170826213907_hurricane_harvey_2017_20170908_vol-14.json"

        path_classificazione = "C:\\Users\\Asus\\Desktop\\MAGISTRALE ESAMI FATTI\\BIG DATA\\ProgettoWS\\dataset\\harvey_data_2017_aidr_classification.txt"

        spark = SparkSession.builder.master("local[*]")\
            .appName("Disastri Naturali")\
            .config("spark.sql.debug.maxToStringFields", 1000) \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
            .getOrCreate()
        spark.sparkContext.setLogLevel("OFF")

        #Creazione dei dataset dal volume 4 al volume 14 
        #df1 = spark.read.option("multiline","false").option("inferSchema", "true").json(path2).drop('quoted_status').drop("retweeted_status")
        df1 = spark.read.option("multiline","false").option("inferSchema", "true").json(path1).drop('quoted_status').drop("retweeted_status")
        df2 = spark.read.option("multiline","false").option("inferSchema", "true").json(path2).drop('quoted_status').drop("retweeted_status")
        #df3 = spark.read.option("multiline","false").option("inferSchema", "true").json(path3).drop('quoted_status').drop("retweeted_status")
        #df4 = spark.read.option("multiline","false").option("inferSchema", "true").json(path4).drop('quoted_status').drop("retweeted_status")
        #df5 = spark.read.option("multiline","false").option("inferSchema", "true").json(path5).drop('quoted_status').drop("retweeted_status")
        #df6 = spark.read.option("multiline","false").option("inferSchema", "true").json(path6).drop('quoted_status').drop("retweeted_status")
        #df7 = spark.read.option("multiline","false").option("inferSchema", "true").json(path7).drop('quoted_status').drop("retweeted_status")
        #df8 = spark.read.option("multiline","false").option("inferSchema", "true").json(path8).drop('quoted_status').drop("retweeted_status")
        #df9 = spark.read.option("multiline","false").option("inferSchema", "true").json(path9).drop('quoted_status').drop("retweeted_status")
        #df10 = spark.read.option("multiline","false").option("inferSchema", "true").json(path10).drop('quoted_status').drop("retweeted_status")
        #df11 = spark.read.option("multiline","false").option("inferSchema", "true").json(path11).drop('quoted_status').drop("retweeted_status")

        df = df1\
            .union(df2)\
            #.union(df3)\
            #.union(df4)\
            #.union(df5)\
            #.union(df6)\
            #.union(df7)\
            #.union(df8)\
            #.union(df9)\
            #.union(df10)\
            #.union(df11)
        dataset = df.drop("aidr").drop("contributors").drop("display_text_range").drop("filter_level").drop("in_reply_to_screen_name")\
                .drop("in_reply_to_status_id").drop("in_reply_to_status_id_str").drop("in_reply_to_user_id").drop("in_reply_to_user_id_str")\
                .drop("possibly_sensitive").drop("withheld_in_countries").drop("source").drop("retweeted").drop("place")\
                .drop("favorited")

        dataset = dataset.withColumn("id", df["id"].cast(LongType()))
        #dataset.printSchema()
        #dataset.show(1,vertical = True)
        #print(dataset.count())
        #print(len(dataset.columns))

        schema_classificazione = StructType([
            StructField("TweetID", LongType(),nullable=False),
            StructField("Date", TimestampType(),nullable=True),
            StructField("AIDRLabel", StringType(),nullable=True),
            StructField("AIDRConfidenze", DoubleType(),nullable=True),
        ])

        self.aidr_label_classification = ["injured_or_dead_people", "relevant_information", "caution_and_advice","displaced_and_evacuations",
                             "sympathy_and_support","response_efforts","infrastructure_and_utilities_damage", "personal",
                             "affected_individual","not_related_or_irrelevant","missing_and_found_people","donation_and_volunteering"]


        dataset_classificazione = spark.read.csv(path_classificazione, schema=schema_classificazione, header=True, sep='\t').drop("Date")
        #print(dataset_classificazione.show())

        self.dataset_completo = dataset.join(dataset_classificazione, dataset["id"] == dataset_classificazione["TweetID"], "inner").dropDuplicates()

        self.dataset_completo.cache()
        self.query = GestoreQuery(self)

    def closeConnection(self):
        self.spark.stop()
        print("Connessione Chiusa")


class GestoreQuery:
    def __init__(self, spark: SparkBuilder):
        self.spark = spark
        self.dataset = self.spark.dataset_completo


    #Metodo preliminare che rimuove i tag degli utenti, gli hashtag, i link, caratteri speciali,punteggiatura e stop word.
    def pre_processing_text(self):
    #Parole che iniziano con @ (nomi utente),parole che iniziano con # (hashtag), URL (che iniziano con http:// o https://).
    #Simboli, caratteri di controllo, nuove linee e punteggiatura,la stringa RT (indicativo di un retweet),
    #caratteri non ASCII (come emoji e caratteri di altre lingue).
        regex = r"\B@\w+\b|\B#\w+\b|https?://\S+|[\p{So}\p{Cntrl}\n\p{Punct}]+|RT|[^\x00-\x7F]+"
        #Sostituiamo tutte le occorrenze che rispettano la regex con uno spazio e rendiamo tutte le parola lower case
        cleaned = self.dataset.withColumn("clean_text", regexp_replace(col("text"), regex, " "))
        cleaned_text_df = cleaned.withColumn("clean_text_lower", lower(col("clean_text")))

        #crea, per ogni tupla, una lista di parole, il cui separatore è lo spazio
        tokenizer = RegexTokenizer(inputCol="clean_text", outputCol="words", pattern="\\W+")
        dataset_tokenized = tokenizer.transform(cleaned_text_df)

        #rimuove le stop words dalla colonna tokenizzata ( articoli, congiunzioni ecc..)
        remover = StopWordsRemover(inputCol="words", outputCol="words_filtered")
        result = remover.transform(dataset_tokenized)
        return result
    
    #QUERY 1
    # Selezionare per ogni categoria, il numero di tweet associati a quella categoria e riportare un diagramma dove sulle ascisse
    #abbiamo la categoria e sulle ordinate i valori, per vedere quali sono le categorie più frequenti nei tweet
    #Eventuali miglioramenti potrebbero essere fare questo lavoro per giorno
    def number_of_tweet_for_category(self):
        tweets_for_category = self.dataset.groupBy("AIDRLabel")\
                                        .agg(count("text").alias("Numero di tweet"))
        return tweets_for_category
    

    #QUERY 2
    #Selezionare i top n hashtag più usati. 
    #TODO
    #Eventuali miglioramenti: restituire i top n hashtags per giorno o per categoria
    def top_n_hashtag(self, categories, n=10):
        hashtag = self.dataset.selectExpr("AIDRLabel", "transform(entities.hashtags,x -> upper(x.text)) as vettore_hashtags")
        #hashtag_exploded = hashtag.select(explode(col("vettore_hashtags")))
        #ds_tot = hashtag_exploded.groupBy("col").agg(count("col")).orderBy(desc(count("col")))
        #ds_tot = ds_tot.withColumnRenamed("col","hashtags").withColumnRenamed("count(col)","Occorrenze")

        result = hashtag.select(col("AIDRLabel"),explode(col("vettore_hashtags")).alias("Hashtag"))\
                .groupBy("Hashtag")\
                .agg(count("Hashtag")\
                .alias("Occorrenze"),*[count(when(col("AIDRLabel") == label,1)).alias(label) for label in categories])\
                .orderBy(desc("Occorrenze"))\
                .limit(n)

        return  result
    
    def num_tot_hashtag(self):
        hashtag = self.dataset.select("entities").selectExpr("transform(entities.hashtags,x -> upper(x.text)) as vettore_hashtags")

        hashtag_exploded = hashtag.select(explode(col("vettore_hashtags")))

        result = hashtag_exploded.groupBy("col").agg(count("col")).orderBy(desc(count("col")))
        result = result.withColumnRenamed("col","hashtags").withColumnRenamed("count(col)","Occorrenze")
        return  result

    #QUERY 3
    #Le parola più ricorrente per ogni categoria
    def max_word_for_category(self):
        #new_dataset = self.dataset.withColumn("text",when(col("truncated") == True,"extended_tweet.text").otherwise(col("text")))
        dataset_processato = self.pre_processing_text()
        
        #dal dataset processato prendiamo la colonna AIDRLabel e la colonna words_filtered esplosa, rinominata come words
        text_with_label = dataset_processato\
                        .select(col("AIDRLabel"), explode(col("words_filtered"))\
                        .alias("Parola")) #\
        
        #per ogni coppia label-parola, contiamo quante volte occorre la coppia e inseriamo il valore in una nuova colonna words_count                            
        count_words = text_with_label\
                        .groupBy(col("AIDRLabel"),col("Parola"))\
                        .agg(count("*")\
                        .alias("Occorrenze"))
        
        #per ogni label prendiamo il massimo della colonna words count
        max_word = count_words\
                    .groupBy(col("AIDRLabel")\
                    .alias("labels"))\
                    .agg(F.max("Occorrenze")\
                    .alias("max_counted"))
    
        #facciamo la join con la tabella precedente per prendere la parola associata alla coppia label-words_count
        #eliminiamo eventuali parole con conteggio uguale, nella stessa label, e ordiniamo in ordine decrescente
        result = max_word\
                .join(count_words,(max_word.labels == count_words.AIDRLabel) & (max_word.max_counted == count_words.Occorrenze) , "inner")\
                .drop_duplicates(["labels"])\
                .orderBy(desc("max_counted"))
        
        #rimuoviamo le colonne supeflue
        result = result.drop("labels").drop("max_counted")
        
        return result
    

    #QUERY 4
    #vediamo il trend totale delle parole nei tweet e come una generica categoria si adatta a questo trend
    def process_text_data(self, labels, n=10):
        dataset_processato = self.pre_processing_text()

        prova = dataset_processato.select(col("AIDRLabel"), explode(col("words_filtered")).alias("Parola"))\
                .groupBy("Parola")\
                .agg(count("Parola")\
                .alias("Occorrenze parola"),*[count(when(col("AIDRLabel") == label,1)).alias(label) for label in labels])\
                .orderBy(desc("Occorrenze parola"))\
                .limit(n)
        
        return prova
    
    
    #QUERY 5
    def top_mentioned_for_categories(self, categories, n=5):
        #per ogni tupla, prendere il campo entities.user_mention (che restituisce un array di account menzionati) 
        #scandire l'array ed estrarne il nome, infine contare il numero di volte che appare ogni nome
        ds_esploso = self.dataset.select("AIDRLabel", explode(col("entities.user_mentions")).alias("user_mentions"))\
                                .filter(col("AIDRLabel").isin(*categories)) #ogni riga possiede la categoria e un utente mezionato
        ds_users = ds_esploso.select("AIDRLabel", "user_mentions.name")
        risultato = ds_users.groupBy("AIDRLabel", "name").agg(count("*").alias("Occorrenze")).orderBy(desc("Occorrenze")).limit(n)
        return risultato.withColumnRenamed("name", "Nome")
    
    #QUERY 6  --> categoria, giorno, numero di tweet 
    def tweets_distribution_for_category(self, category):
        ds_data_convertita = self.dataset.withColumn("timestamp", from_unixtime(unix_timestamp(col("created_at"), "EEE MMM dd HH:mm:ss ZZZ yyyy")))
        ds_data_AMG = ds_data_convertita.withColumn("Giorno", date_format(col("timestamp"), "yyyy-MM-dd"))
        if (category != None):
            risultato = ds_data_AMG.groupBy("AIDRLabel", "Giorno").agg(count("*").alias("Tweet per giorno"))\
                                    .filter(col("AIDRLabel") == category)
        else:
            risultato = ds_data_AMG.groupBy("AIDRLabel", "Giorno").agg(count("*").alias("Tweet per giorno"))
        return risultato
    
    #QUERY 7
    #tra tutti gli utenti che hanno tweettato almeno una volta nelle date comprese nel nostro dataset
    #prendiamo gli utenti più influenti in base al numero di follower, il numero totale di tweet e se il profilo è verificato
    def most_influents_users(self, categories, n=10):
        filtered_df = self.dataset.filter(col("AIDRLabel").isin(*categories))
        dataset_user = filtered_df.select("user","AIDRLabel").filter(col("user.verified") == True)
        tmp1= dataset_user.select("user.name","user.id","user.followers_count","user.statuses_count")\
                    .dropDuplicates(["id"])\
                    
        tmp2 = dataset_user.select("user.id").groupBy("id")\
            .agg(count("id").alias("Tweet sull'uragano"))
        tmp2 = tmp2\
            .select(col("id")\
            .alias("userID"), col("Tweet sull'uragano"))
        result = tmp1.join(tmp2, tmp1["id"] == tmp2["userID"], "inner")\
            .drop("userID")\
            .dropDuplicates(["name"])\
            .orderBy(desc("followers_count"))\
            .limit(n)
        return result.withColumnRenamed("name", "Nome").withColumnRenamed("followers_count", "Followers").withColumnRenamed("statuses_count", "Tweet totali")
    
    #QUERY 8 
    def media_for_category(self):
        dataset_tot = self.dataset.groupBy("AIDRLabel").agg(count("*").alias("Occorrenze totali"))
        dataset_filtrato = self.dataset.filter(col("entities.media").isNotNull()).groupBy("AIDRLabel").agg(count("*").alias("Occorrenze con media"))
        dataset_combinato = dataset_tot.join(dataset_filtrato, dataset_filtrato.AIDRLabel == dataset_tot.AIDRLabel, "left")
        risultato = dataset_combinato.withColumn("percentuale %",  round((col("Occorrenze con media") / col("Occorrenze totali")) * 100, 2)).na.fill(0, "Occorrenze con media").na.fill(0, "percentuale %")
        return risultato.select(dataset_tot["AIDRLabel"], "Occorrenze con media", "Occorrenze totali", "percentuale %")
    
    #QUERY 9
    def top_n_user(self, categories, n=10):
        if categories == []:
            return self.dataset.groupBy("user.id", "user.name").agg(count("*").alias("Numero di tweet")).orderBy(desc("Numero di tweet")).limit(n)
        else:
            filtered_df = self.dataset.filter(col("AIDRLabel").isin(categories))
    
        tmp_user = filtered_df.select(col("user.id").alias("user_id"), col("user.name").alias("name"), col("AIDRLabel"))
    
        tmp2_user = tmp_user.groupBy("user_id", "name")\
            .agg(count("user_id").alias("Total"))\
            .orderBy(desc("Total")).limit(n)
    
        tmp_categories = tmp_user.groupBy("user_id")\
            .agg(*[
                count(when(col("AIDRLabel") == label, 1)).alias(label)
                for label in categories
            ])
    
        tmp_percentage = tmp_categories.join(tmp2_user, "user_id")
        result = tmp_percentage.select(
            "user_id","name","Total",
            *[round((col(label) / col("Total"))*100, 2).alias(f"{label}_percentage") for label in categories]
        ).orderBy(desc("Total"))
   
        return result
