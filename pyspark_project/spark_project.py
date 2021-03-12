from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

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
# df = spark.read.csv("SEMANAL_MUNICIPIOS-2019.csv", inferSchema = True, header = True)



#Converte o tipo de dados das colunas de string para Data, Inteiro e Double
df = df.withColumn('DATA INICIAL',to_date(df["DATA INICIAL"], 'd/M/yyyy').cast(DateType()))\
       .withColumn("PREÇO MÉDIO REVENDA", regexp_replace("PREÇO MÉDIO REVENDA", ",", ".").cast(DoubleType()))\
       # .withColumn("MES FINAL", month(col("DATA FINAL")))\
       # .withColumn("ANO FINAL", year(col("DATA FINAL")))
       # .withColumn("data_final", to_timestamp("DATA FINAL", "d/m/yyyy"))\
       # .withColumn("DATA FINAL", to_timestamp("DATA FINAL", "d/m/yyyy").cast(DataType())) \
       # .withColumn("DESVIO PADRÃO REVENDA", regexp_replace("DESVIO PADRÃO REVENDA", ",", "."))\
       # .withColumn("MARGEM MÉDIA REVENDA", regexp_replace("MARGEM MÉDIA REVENDA", ",", "."))\
df.printSchema()
df.show(1000)

# #transforma DataFrame em Tabela para execuçao de select no padrao SQL
df.createOrReplaceTempView("table")

# # a) Estes valores estão distribuídos em dados semanais, agrupe eles por mês e calcule
# # as médias de valores de cada combustível por cidade.
# df1 = spark.sql("""SELECT month(`DATA INICIAL`) as mi, year(`DATA INICIAL`) as ai, `MUNICÍPIO` as m, PRODUTO as p,
#                           round(avg(`PREÇO MÉDIO REVENDA`), 2) as pm
#                    FROM table
#                    GROUP BY ai, mi, m, p
#                    ORDER BY m, ai, mi
#                 """)
# df1.show(2000)
# df1.printSchema()

# # b) Calcule a média de valor do combustível por estado e região.
# df2 = spark.sql("""SELECT ESTADO as e, PRODUTO as p, round(avg(`PREÇO MÉDIO REVENDA`), 2) as pm
#                    FROM table
#                    GROUP BY e, p
#                    ORDER BY e, p
#                 """).show(100)
#
# df3 = spark.sql("""SELECT `REGIÃO` as r, PRODUTO as p, round(avg(`PREÇO MÉDIO REVENDA`), 2) as pm
#                    FROM table
#                    GROUP BY r, p
#                    ORDER BY r, p
#                 """).show(20)

# c) Calcule a variância e a variação absoluta do máximo, mínimo de cada cidade, mês a mês.

#d) Quais são as 5 cidades que possuem a maior diferença entre o combustível mais barato e o mais caro.
df4 = spark.sql("""SELECT `MUNICIPIO`, max(p), min(p) 
                   FROM (  
                     SELECT `MUNICÍPIO` as m, PRODUTO as p, 
                             round(avg(`PREÇO MÉDIO REVENDA`), 2) as pm
                     FROM table 
                     GROUP BY m, p
                     ORDER BY m, p
                   )
                """).show(100)

# round(max(`PREÇO MÉDIO REVENDA`)) as pmax,
#                           round(max(`PREÇO MÉDIO REVENDA`)) as pmix,

#Filtra informaçoes no DataFrame
# df.filter(df.MUNICÍPIO=='ABAETETUBA').show(50)

#Agrupar informaçoes no DataFrame
# df_grouped = df.groupBy("DATA_INICIAL").groupBy("MUNICÍPIO").count().show()

