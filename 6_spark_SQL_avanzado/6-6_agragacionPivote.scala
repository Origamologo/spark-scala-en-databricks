// Agregación con pivote
/*.pivot es una forma de resumir los datos especificando una de las 
columnas categóricas y luego realizando agregaciones en otras columnas, 
de modo que los valores categóricos se transporten de las filas a las columnas*/

val df = spark.read.parquet("/FileStore/section8/estudiantes.parquet")

df.show

import org.apache.spark.sql.functions.{avg, col}

df.groupBy("graduacion").pivot("sexo").agg(avg(col("peso"))).show//Agrupa por año de graduación y da el peso medio medio por sexo

import org.apache.spark.sql.functions.{min, max}

df.groupBy("graduacion").pivot("sexo").agg(
  avg(col("peso")),
  min(col("peso")),
  max(col("peso"))
).show

df.groupBy("graduacion").pivot("sexo", Array("M")).agg( //A pivot le podemos decir qué valores de la columna que vamos a pivotar queremos que aparezcan, se indica dentro de un array
  avg(col("peso")),
  min(col("peso")),
  max(col("peso"))
).show
