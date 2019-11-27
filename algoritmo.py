

import time

# start_time=time.time()
# time.sleep(5)
# end_time=time.time()
# time_in_minutes = float(end_time-start_time)

# print("Tiempo de ejecuccion: ", time_in_minutes, " minutos")


for i in range(1,10801):
	print("Iteración: ", i)
	print("Algoritmo en ejecución")
	print("Ajustando Datos")
	time.sleep(1)






# Probar el codigo en CLOUDERA:
# Texto original

from pyspark.sql import HiveContext 
from pyspark.sql.functions import *
import ConfigParser
from pyspark import SparkContext, SparkConf



conf=SparkConf().setAppName("Prueba_1")
sc=SparkContext.getOrCreate(conf=conf)
sqlContext=HiveContext(sc)


df1=sqlContext.sql("SELECT * FROM dwhprod.cen_identificacion LIMIT 100")
df1.show()

df2=sqlContext.table("dwhprod.cen_identificacion")
df2.show()

# Guargadado de tablas
df1.write.mode("overwrite").saveAsTable("dwhprod.base_nueva")

sc.stop()





# JOINS EN IMPALA

# --SELECT * FROM dwhprod.cenpersona LIMIT 100

#--SELECT * FROM dwhprod.cenactividad LIMIT 100

#--fiactividad

# SELECT * FROM cenpersona, cenactividad WHERE (cenpersona.fiactividad=cenactividad.fiactividad AND cenactividad.fiactividad>0) LIMIT 1000