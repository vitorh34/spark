from pyspark.sql import *
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType
from pyspark.sql.functions import to_timestamp, regexp_replace, col, udf
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

#define schema para DataFrame
schema = StructType()\
    .add("DATA INICIAL", StringType(), True)\
    .add("DATA FINAL", StringType(), True) \
    .add("REGIÃO", StringType(), True) \
    .add("ESTADO", StringType(), True) \
    .add("MUNICÍPIO", StringType(), True) \
    .add("PRODUTO", StringType(), True) \
    .add("NÚMERO DE POSTOS PESQUISADOS", IntegerType(), True) \
    .add("UNIDADE DE MEDIDA", StringType(), True) \
    .add("PREÇO MÉDIO REVENDA", StringType(), True) \
    .add("DESVIO PADRÃO REVENDA", StringType(), True) \
    .add("PREÇO MÍNIMO REVENDA", StringType(), True) \
    .add("PREÇO MÁXIMO REVENDA", StringType(), True) \
    .add("MARGEM MÉDIA REVENDA", StringType(), True) \
    .add("COEF DE VARIAÇÃO REVENDA", StringType(), True) \
    .add("PREÇO MÉDIO DISTRIBUIÇÃO", StringType(), True) \
    .add("DESVIO PADRÃO DISTRIBUIÇÃO", StringType(), True) \
    .add("PREÇO MÍNIMO DISTRIBUIÇÃO", StringType(), True) \
    .add("PREÇO MÁXIMO DISTRIBUIÇÃO", StringType(), True) \
    .add("COEF DE VARIAÇÃO DISTRIBUIÇÃO", StringType(), True) \


#importa arquivo csv "SEMANAL_MUNICIPIOS-2019.csv" para um DataFrame com schema definido
df = spark.read.format("csv") \
    .option("header", True) \
    .schema(schema) \
    .load("SEMANAL_MUNICIPIOS-2019.csv")

# This function converts the string cell into a date:
func =  udf (lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())
df = df.withColumn('data_final', func(col('DATA  FINAL')))

# df = spark.read.csv("SEMANAL_MUNICIPIOS-2019.csv", inferSchema = True, header = True)

df = df.withColumn("DATA FINAL", to_timestamp("DATA FINAL", "d/m/yyyy").cast(DateType))
# .withColumn("DATA INICIAL", to_timestamp("DATA INICIAL", "d/m/yyyy").cast('timestamp'))\
# .withColumn("DATA FINAL", to_timestamp("DATA FINAL", "d/m/yyyy").cast(DataType)) \
# .withColumn("PREÇO MÉDIO REVENDA", regexp_replace("PREÇO MÉDIO REVENDA", ",", "."))\
# .withColumn("DESVIO PADRÃO REVENDA", regexp_replace("DESVIO PADRÃO REVENDA", ",", "."))\
# .withColumn("MARGEM MÉDIA REVENDA", regexp_replace("MARGEM MÉDIA REVENDA", ",", "."))\

# df.select(to_timestamp(df["DATA FINAL"], 'd/m/yyyy').alias('data_final'))\

df.printSchema()
df.show()
# #transforma DataFrame em Tabela para execuçao de select no padrao SQL
# df.createOrReplaceTempView("table")
# df1 = spark.sql("""SELECT `DATA INICIAL` as data_inicial FROM table""")
# df1.show()
# df1.printSchema()
# df.filter(df.MUNICÍPIO=='ABAETETUBA').show(50)

# df_grouped = df.groupBy("DATA_INICIAL").groupBy("MUNICÍPIO").count().show()

# sqlContext.sql('select * from df_table').show()
