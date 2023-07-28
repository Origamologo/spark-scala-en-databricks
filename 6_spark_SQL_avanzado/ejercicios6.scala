//Lectura de datos

val jugadores = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/Lectura72/players.csv")
val apariciones = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/Lectura72/appearances.csv")
val competiciones = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/Lectura72/competitions.csv")
val juegos = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/Lectura72/games.csv")

//Schemas

println("Jugadores")
jugadores.printSchema

println("="*100)
println("Apariciones")
apariciones.printSchema

println("="*100)
println("Competiciones")
competiciones.printSchema

println("="*100)
println("Juegos")
juegos.printSchema


/*Determine los tres países­ses con mayor número de jugadores(jugadores nacidos en ese país). 
El resultado debe estar ordenado de forma descendente.*/
import org.apache.spark.sql.functions.{col, desc}

jugadores.groupBy("country_of_birth").count.orderBy(desc("count")).filter(col("country_of_birth").isNotNull).show(3, false)


/*Obtenga la lista de jugadores con tarjeta roja. La salida debe contener dos columnas, 
el nombre de pila del jugador y la cantidad de tarjetas rojas que tiene.*/

jugadores.join(apariciones, Seq("player_id"), "left").filter(col("red_cards") > 0).select(col("pretty_name"), col("red_cards")).show


/*¿Cuántos partidos se jugaron en la Premier League? La salida debe contener dos columnas, 
el nombre de la liga y la cantidad de juegos que se jugaron en ella.*/
display(competiciones)

juegos.join(competiciones, col("competition_code") === col("competition_id"), "left").filter(col("name") === "premier-league").groupBy("name").count.show


/*Obtenga las tres ligas con mayor número de asistencia de público teniendo en cuenta 
todos los juegos que se jugaron en ellas. El resultado debe estar ordenado de forma descendente 
y tener dos columnas, el nombre de la liga y la asistencia total.*/
import org.apache.spark.sql.functions.sum

juegos.join(competiciones, col("competition_code") === col("competition_id"), "left").groupBy("name").agg(sum(col("attendance")).as("asistencia_total")).orderBy(desc("asistencia_total")).show(3, false)
