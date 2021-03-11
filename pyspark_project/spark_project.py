from pyspark.sql import *
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DataType, DoubleType, IntegerType, StringType
from pyspark.sql.functions import to_timestamp

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
    .add("DESVIO PADRÃO REVENDA", DoubleType(), True) \
    .add("PREÇO MÍNIMO REVENDA", DoubleType(), True) \
    .add("PREÇO MÁXIMO REVENDA", DoubleType(), True) \
    .add("MARGEM MÉDIA REVENDA", StringType(), True) \
    .add("COEF DE VARIAÇÃO REVENDA", DoubleType(), True) \
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

# df = spark.read.csv("SEMANAL_MUNICIPIOS-2019.csv", inferSchema = True, header = True)
df.printSchema()
df.show()

# df.withColumn("`data_inicial`", to_timestamp("`DATA INICIAL`", "DD/MM/YYYY"))
# df.show()
# #transforma DataFrame em Tabela para execuçao de select no padrao SQL
# df.createOrReplaceTempView("table")
# df1 = spark.sql("""SELECT to_timestamp(`DATA INICIAL`, 'DD/MM/YYYY') as data_inicial FROM table""")
# df1.show()

# df.filter(df.MUNICÍPIO=='ABAETETUBA').show(50)

# df_grouped = df.groupBy("DATA_INICIAL").groupBy("MUNICÍPIO").count().show()

# sqlContext.sql('select * from df_table').show()
