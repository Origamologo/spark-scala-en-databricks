// Funciones filter y where
/*filter filtra las filas que no cumplen con a condición dada. Devuelve sólo las filas que cumplen con la condición especificada.
where es como filter, pero un poco más relacional*/

val df = spark.read.parquet("/FileStore/section7/datos.parquet")

// Filter

import org.apache.spark.sql.functions.col

display(df)

display(df.filter(col("video_id") === "2kyS6SvSYSE"))

display(df.where(col("trending_date") =!= "17.14.11"))

display(df.where(col("likes") > 5000))

display(
  df.filter(col("trending_date") =!= "17.14.11" && col("likes") > 5000)
)

display(
  df.filter(col("trending_date") =!= "17.14.11").filter(col("likes") > 5000)
)
