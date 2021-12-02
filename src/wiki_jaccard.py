# libraries for gathering data & spark
import findspark
import numpy as np
import pandas as pd
import pyspark
import pyspark.sql.functions as f
import virtualenv
import pyspark_dist_explore

from datetime import datetime 
from pyspark import SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, MinHashLSH
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import * 
from pyspark.sql.functions import *
from pyspark_dist_explore import hist

# Spark building session
spark = SparkSession.builder.appName('Project').getOrCreate()

# Functions for converting pandas dataframe 
def equivalent_type(f):
    if f == 'datetime64[ns]': return TimestampType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return FloatType()
    else: return StringType()

def define_structure(string, format_type):
    try: typo = equivalent_type(format_type)
    except: typo = StringType()
    return StructField(string, typo)

# Given pandas dataframe, it will return a sparks dataframe.
def pandas_to_spark(pandas_df):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types): 
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return spark.createDataFrame(pandas_df, p_schema)

# Function for Jaccard Distance
def pipeline(df): 
    model = Pipeline(stages = [ 
        HashingTF(inputCol = "text", outputCol = "vectors"), 
        MinHashLSH(inputCol = "vectors", outputCol = "lsh", numHashTables = 10) # is chosen to create 10 independent minhash values from each record
    ]).fit(df)

    df_hashed = model.transform(df)
    df_matches = model.stages[-1].approxSimilarityJoin(df_hashed, df_hashed, 0.9)

    return df_matches


# Data gathering from CSV file
master_df = pd.read_csv(r'C:\Users\richa\OneDrive\Bureaublad\DAS_project\data\master_df.csv') # change path to open the csv
master_df

# Convert pandas_df to spark_df and change string to array
spark_df = pandas_to_spark(master_df)
spark_df = spark_df.withColumn("text", split(col("text"), ",\s").cast("array<int>").alias("ev"))

# Apply jaccard distance on spark_df and calculate the Jaccard similarity coefficient (jacCol)
spark_df = pipeline(spark_df)

# Show datasetA and datasetB
spark_df.show()

# Show non-duplicate matches
jaccard = spark_df.select(f.col('datasetA.revid').alias('revid_A'), f.col('datasetB.revid').alias('revid_B'), f.col('distCol')).filter('revid_A < revid_B')
jaccard.show()

distcol = jaccard.select('distCol')
dist_pandas = distcol.toPandas()

import matplotlib.pyplot as plt

plt.hist(dist_pandas)
plt.xlabel("Jaccard distance")
plt.ylabel("Sets of pages")
plt.show()

jaccard_1 = jaccard.filter((jaccard.distCol > 0.5))
jaccard_1.show()
jaccard_1.count()

jaccard_2 = jaccard.filter((jaccard.distCol == 0.5))
jaccard_2.show()
jaccard_2.count()

jaccard_3 = jaccard.filter((jaccard.distCol < 0.5))
jaccard_3.show()
jaccard_3.count()

# Export to CSV
jaccard.toPandas().to_csv('jaccard.csv')