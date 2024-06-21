import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType,LongType,TimestampType, DoubleType
from pyspark.sql.functions import col,when,regexp_replace,lower
from pyspark.sql import functions as F
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF,IDF
from pyspark.ml.classification import LogisticRegression,NaiveBayes,RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator,MulticlassClassificationEvaluator


class Classification:

    def __init__(self,dataset,categories,model):
        self.model = model
        self.dataset = dataset
        self.categories = categories

    def select_column(self):
        dataset = self.dataset
        categories = self.categories
        new_dataset = dataset.withColumn("text",when(col("truncated") == True,"extended_tweet.text").otherwise(col("text")))
        df_filtered = new_dataset.select("text","AIDRLabel").filter(col("AIDRLabel").isin(categories))
        #avendo una colonna label non ho il bisogno di comunicare quale è la colonna target quando definisco il modello
        result = df_filtered.withColumn("label", when(col("AIDRLabel")== categories[0],1).otherwise(0))
        return result

    def get_model(self):
        if self.model == 0:
            return LogisticRegression(maxIter=10, regParam=0.001)
        elif self.model == 1:
            return NaiveBayes(smoothing=1.0,modelType="multinomial")
        return  RandomForestClassifier(numTrees=100, maxDepth=10, seed=42)


    def get_pipeline(self,model):

        dataset = self.dataset
        regex = r"\B@\w+\b|\B#\w+\b|https?://\S+|[\p{So}\p{Cntrl}\n\p{Punct}]+|RT|[^\x00-\x7F]+"
        cleaned = dataset.withColumn("text", regexp_replace(col("text"), regex, " "))
        cleaned_text_df = cleaned.withColumn("text", lower(col("text")))

        tokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="\\W+")

        remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")

        hashingTF = HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=4000)

        idf = IDF(inputCol="raw_features", outputCol="features")

        model = self.get_model()

        pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, model])

        return pipeline


    def train_model(self,pipeline, training_set):
        model = pipeline.fit(training_set)
        return model
    
    def evaluate_model(self,model,test_set):
        predictions = model.transform(test_set)
        binary_evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction")
        accuracy_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
        precision_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision")
        recall_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall")
        f1_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")

        auc = binary_evaluator.evaluate(predictions, {binary_evaluator.metricName: "areaUnderROC"})
        accuracy = accuracy_evaluator.evaluate(predictions)
        precision = precision_evaluator.evaluate(predictions)
        recall = recall_evaluator.evaluate(predictions)
        f1 = f1_evaluator.evaluate(predictions)
        return accuracy,precision,recall,f1,auc, predictions\
    
    
    
        
    
    def main(self):
        dataset = self.select_column()
        
        (trainingData, testData) = dataset.randomSplit([0.8, 0.2], seed=1234)
        pipeline = self.get_pipeline(self.model)
        model = self.train_model(pipeline, trainingData)
        accuracy,precision,recall,f1,auc, predictions = self.evaluate_model(model, testData)
        if self.model == 0:
            print("MODELLO DI APPRENDIMENTO LOGISTIC REGRESSION")
            print(f"Accuracy: {accuracy}")
            print(f"Precision: {precision}")
            print(f"Recall: {recall}")
            print(f"F1: {f1}")
            print(f"AUC: {auc}")
            predictions.select("text", "label", "prediction").limit(10).show()
        elif self.model == 1:
            print("MODELLO DI APPRENDIMENTO NAIVE BAYES")
            print(f"Accuracy: {accuracy}")
            print(f"Precision: {precision}")
            print(f"Recall: {recall}")
            print(f"F1: {f1}")
            print(f"AUC: {auc}")
            predictions.select("text", "label", "prediction").limit(10).show()
        else:
            print("MODELLO DI APPRENDIMENTO RANDOM FOREST")
            print(f"Accuracy: {accuracy}")
            print(f"Precision: {precision}")
            print(f"Recall: {recall}")
            print(f"F1: {f1}")
            print(f"AUC: {auc}")
            predictions.select("text", "label", "prediction").limit(10).show()
            
        return accuracy,precision,recall,f1,auc, predictions

        
        






