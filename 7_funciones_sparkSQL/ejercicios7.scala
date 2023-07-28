/*Cada línea del archivo movies.csv representa a un actor que actúa en una película. 
Si una película tiene diez actores, habrá diez filas para esa película en particular.*/

val movies = spark.read.option("header", "true").option("inferSchema", "true").option("sep", "|").csv("/FileStore/Lectura82/movies.csv")
val movies = spark.read.option("header", "true").option("inferSchema", "true").option("sep", "|").csv("/FileStore/Lectura82/movie_ratings.csv")

println("Movies")
movies.printSchema

println("="*100)

println("Ratings")
ratings.printSchema

/*Calcule la cantidad de películas en las que participó cada actor. 
La salida debe tener dos columnas: actor y conteo. 
La salida debe ordenarse por el conteo en orden descendente.*/
import org.apache.spark.sql.functions.{col, desc}

movies.groupBy("actor").count.orderBy(desc("count")).show

/*Calcule la cantidad de películas producidas cada año. 
La salida debe tener tres columnas: año, siglo al que pertenece el año y conteo. 
La salida debe ordenarse por el conteo en orden descendente.*/
import org.apache.spark.sql.functions.{when, lit, count_distinc}

movies.grouopBy("año").agg(
	when(col("año") >= 1900, lit("XX")).otherwise(lit("XXI")).as("siglo"),
	count_distinc(col("pelicula")).as("conteo")
).orderBy(desc("conteo"))
.show(false)

/*Obtenga la película con la calificación más alta por año. 
La salida debe tener solo una película por año y debe contener 
tres columnas: año, título de la película y valoración.*/
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number

val windowSpec = Window.partitionBy("año").orderBy("valoracion")

ratings.withColumn("row_number", row_number.over(windowSpec))
	.filter(col("row_number") === 1)
	.drop("row_number")
	.show(false)
