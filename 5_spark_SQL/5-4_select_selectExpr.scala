// Funciones select y selectExpr
/*select selecciona todas o alguans columnas que le indiquemos.
selectExpr es una variante de select que acepta una o más expresiones SQL en un string para ejecutarse en lugar de columnas*/

val df = spark.read.parquet("/FileStore/section7/datos.parquet")

df.printSchema

import org.apache.spark.sql.functions.col

df.select(col("video_id")).show()

df.select("video_id", "title").show()

// Esta via da error

df.select(
  "likes",
  "dislikes",
  "likes" - "dislikes"
).show()

//La forma correcta

df.select(
  col("likes"),
  col("dislikes"),
  (col("likes") - col("dislikes")).as("aceptacion")
).show()

// selectExpr

df.selectExpr("likes", "dislikes", "(likes - dislikes) as aceptacion").show()

df.selectExpr("count(distinct(video_id)) as videos").show()
