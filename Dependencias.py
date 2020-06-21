import geopandas as gpd
from pyspark.sql.functions import col, udf, isnan
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, IntegerType, StringType, FloatType, BooleanType, DateType, TimestampType, StructField, StructType

from pyspark import SparkContext
sc = SparkContext.getOrCreate()
from pyspark.sql import SQLContext
sqlC = SQLContext(sc)

from  pyspark.sql.functions import abs as fabs
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import sum as fsum


def SPandas(objgpd, especial_trafico = False):
  """
  objgpd : objeto geopandas/pandas NO vacío
  especial_trafico : para tratar el archivo de intensidades medias. En este caso,
                     en vez de tratar la columna geometry como strings se crean
                     dos columnas: una con la coordenada x de geometry y otra
                     con la y (el archivo de intensidades medias solo tienes dos
                     coordenadas en la columna geometry)
  """

  esquema = dict(objgpd.dtypes)
  # Si existe la columna geometry, la convertimos a strings
  if 'geometry' in esquema.keys():
    # Si es el archivo de intensidades medias...
    if especial_trafico:
      objgpd['coord_x'] = objgpd['geometry'].apply(lambda pto : pto.x)
      objgpd['coord_y'] = objgpd['geometry'].apply(lambda pto : pto.y)

      # Transformamos POINT Z a POINT y mantenemos la columna 'geometry' por si acaso
      objgpd['geometry'] = objgpd['geometry'].apply(lambda x : x.wkt.replace("POINT Z", "POINT").replace(" 0)", ")"))
    else:
      objgpd['geometry'] = objgpd['geometry'].apply(lambda x : x.wkt)

  # https://spark.apache.org/docs/2.1.2/api/python/_modules/pyspark/sql/types.html
  # https://pbpython.com/pandas_dtypes.html
  equivalencias = {'int64' : IntegerType, 'object' : StringType, 'float64' : FloatType, 
                    'bool' : BooleanType, 'datetime64' : DateType,
                    'timedelta' : TimestampType, 
                    # La columna geometry la trataremos con Strings, como hemos dicho antes
                    'geometry' : StringType}

  # Volvemos a coger el esquema porque a lo mejor hemos modificado el DF original
  esquema = dict(objgpd.dtypes)

  for clave, valor in esquema.items():
    try:
      # Comprobamos si existe algún tipo que coincida con el de la columna
      esquema[clave] = equivalencias[str(valor)]
    except KeyError:
      # Si no existe, lo pasamos a String directamente
      esquema[clave] = StringType

  # Definimos el esquema del archivo
  esquema_inferido = StructType([ StructField(v, esquema[v](), True) for v in esquema.keys() ])
  # Creamos un DF con este esquema
  datos = sqlC.createDataFrame(objgpd, schema = esquema_inferido)
  datos.esquema = esquema

  return datos

trafico_medio = SPandas(gpd.read_file('IMDs_2018.shp'), True)
trafico_medio = trafico_medio["IMD", "coord_x", "coord_y", "geometry"]
# Formateamos la columna IMD, eliminando el carácter de nueva línea
# y pasando a Int
trafico_medio = trafico_medio.withColumn("IMD", regexp_replace(col("IMD"), "[\n\r]", "").cast(IntegerType()))

def comparacion(cx, cy):
  # Definimos el radio
  r = 0.01
  # Cogemos los puntos de tráfico dentro de un radio r
  imds = trafico_medio.filter(fabs(trafico_medio["coord_x"] - cx) + fabs(trafico_medio["coord_y"] - cy) < r)
  
  # Devolvemos la media aritmética de las intensidades medias
  return imds.select(fsum("IMD")).collect()[0][0]/imds.count()













