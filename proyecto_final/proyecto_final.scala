/*Los datos para el proyecto están disponibles en la plataforma Kaggle 
para todo usuario que esté registrado con una cuenta gratuita. El nombre 
de la base de datos empleada es Internet Prices around 200+ countries in 2022.

La base de datos hace referencia a los precios y las velocidades de 
conexión a Internet en más de 200 países en 2022.*/



/*Cree tres DataFrames a partir de los datos proporcionados y verifique que todos 
los nombres de columnas de los tres DataFrames cumplen el siguiente formato:

-Todas la letras en minúscula.
-Las palabras deben estar separadas por el carácter _
-Los nombres de columna no deben tener espacios en blanco al principio, final o en medio.
-Los nombres de columnas no deben contener caracteres como puntos, paréntesis, o guiones medios.

Un ejemplo de como debe quedar el nombre de las columnas es el siguiente: 
	
	average_price_of_1gb_usd_at_the_start_of_2021*/

val usersRaw = spark.read.option("header", "true")
                         .option("inferSchema","true")
                         .csv("/FileStore/Lectura84/users.csv")

val pricesRaw = spark.read.option("header", "true")
                          .option("inferSchema", "true")
                          .csv("/FileStore/Lectura84/prices_2022.csv")

val speedRaw = spark.read.option("header", "true")
                         .option("inferSchema", "true")
                         .csv("/FileStore/Lectura84/avg_speed.csv")

//SCHEMAS

println("Schema dataframe users")
println("="*100)
println("")
usersRaw.printSchema

println("")
println("="*100)
println("Schema dataframe prices")
println("="*100)
println("")
pricesRaw.printSchema

println("")
println("="*100)
println("Schema dataframe speed")
println("="*100)
println("")
speedRaw.printSchema

//PROCESAMIENTO DE LOS NOMBRES DE LAS COLUMNAS

val usersColumn = usersRaw.columns.map(_.toLowerCase.replaceAll(" ", "_"))

val pricesColumn = pricesRaw.columns.map(_.trim.toLowerCase.replaceAll("\\.|\\(|\\)|\\–", ""))
                                    .map(_.replaceAll(" ", "_"))
                                    .map(_.replaceAll("__", "_"))

val speedColumn = speedRaw.columns.map(_.trim.toLowerCase)

//CREAR NUEVOS DATA FRAMES
/*A la función toDF le pasábamos los nombres de columnas separados por coma, pero en este momento
usersColumn, pricesColumn y speedColumn son de tipo string. Hay que desempaquetar el array para que
reciba todos esos nombres de columnas en formato string y lo conseguimos con _* que desempaqueta 
los elementos del array.*/

val users = usersRaw.toDF(usersColumn:_*)

val prices = pricesRaw.toDF(pricesColumn:_*)

val speed = speedRaw.toDF(speedColumn:_*)

//SCHEMAS DE LOS NUEVOS DFs

println("Schema dataframe users")
println("="*100)
println("")
users.printSchema

println("")
println("="*100)
println("Schema dataframe prices")
println("="*100)
println("")
prices.printSchema

println("")
println("="*100)
println("Schema dataframe speed")
println("="*100)
println("")
speed.printSchema

display(users)
display(prices)
display(speed)


/*Determine los cinco países con mayor número de usuarios de Internet en la región de América. 
La salida debe contener el nombre del país, la región, la subregión y la cantidad de usuarios de Internet.*/

//CONVERTIR LOS NÚMEROS CON COMAS TIPO STRING EN TIPO INTEGER Y DECIMAL
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.types.{IntegerType, DecimalType}

val usersDF = users.withColumn("internet_users", regexp_replace(col("internet_users"), ",", "").cast(IntegerType))//cast camnia el tipo de dato
                   .withColumn("population", regexp_replace(col("population"), ",", "").cast(IntegerType)

val pricesDF = prices.withColumn("average_price_of_1gb_usd_at_the_start_of_2021", 
	regexp_replace(col("average_price_of_1gb_usd_at_the_start_of_2021"), "\\$", "").cast(DecimalType(3,2))))


import org.apache.spark.sql.functions.desc

usersDF.filter(col("region") === "Americas")
.orderBy(desc("internet_users"))
.select(
  col("country_or_area"),
  col("region"),
  col("subregion"),
  col("internet_users")
)
.limit(5).show


/*Obtenga el top tres de las regiones con más usuarios de internet.*/

import org.apache.spark.sql.functions.sum

usersDF
.groupBy("region")
.agg(
  sum(col("internet_users")).as("cantidad_usuarios")
)
.orderBy(desc("cantidad_usuarios"))
.limit(3)
.show


/*Obtenga el país con más usuarios de Internet por región y subregión. 
Por ejemplo, el resultado para la región de las Américas y la subregión Norte América 
deberá­a ser Estados Unidos. La salida debe contener el nombre del país con más usuarios 
de Internet, la región, la subregión y la cantidad de usuarios de Internet. 
Además, la salida debe estar ordenada de mayor a menor atendiendo a la cantidad 
de usuarios de Internet de cada país.*/

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number

val windowSpec = Window.partitionBy("region","subregion").orderBy(desc("internet_users"))

val usersInternet = usersDF.withColumn("row_number", row_number().over(windowSpec))
.filter(col("row_number") === 1)
.select(
  col("country_or_area"),
  col("region"),
  col("subregion"),
  col("internet_users")
)
.orderBy(desc("internet_users"))

usersInternet.show(false)


/*Escriba el DataFrame obtenido en el ejercicio anterior teniendo en cuenta las siguientes cuestiones:

-El DataFrame debe tener tres particiones.
-La escritura del DataFrame debe quedar particionada por la regiÃ³n.
-El modo de escritura empleado para la escritura debe ser overwrite.
-El formato de escritura debe ser AVRO.
-El DataFrame debe guardarse en la ruta /FileStore/ProyectoFinal/salida*/

usersInternet.rdd.getNumPartitions

usersInternet.repartition(3).write.format("avro").partitionBy("region").mode("overwrite").save("/FileStore/ProyectoFinal/salida")

dbutils.fs.ls("/FileStore/ProyectoFinal/salida/")//Comprobamos que el DF se ha escrito correctamente
dbutils.fs.ls("/FileStore/ProyectoFinal/salida/region=Europe")//Comprobamos se los archivos son .avro


/*Determine los 10 países con la mayor velocidad promedio de internet según el test de Ookla. 
Es de interés conocer la región, subregión, población y usuarios de internet de cada país, 
por lo tanto los países a los que no se les pueda recuperar estos datos
deben ser excluidos de la salida resultante.*/

val usersWithSpeed = speed.join(usersDF, col("country") === col("country_or_area"), "left")

usersWithSpeed.count

usersWithSpeed.filter(col("country_or_area").isNull).show

usersWithSpeed.filter(col("country_or_area").isNotNull)
.orderBy(desc("avg"))
.select(
  col("country"),
  col("region"),
  col("subregion"),
  col("population"),
  col("internet_users"),
  col("avg").as("promedio_velocidad_internet")
)
.limit(10)
.show


/*Determine el promedio del costo de 1GB en usd a principios del aÃ±o 2021 por región. 
Aquellas ubicaciones a las que no pueda obtenerle la región no deben ser consideradas en el cálculo. 
La salida debe tener tres columnas: region, costo_prom_1_gb y grupo_region. 
Además, debe mostrar las regiones ordenadas de menor a mayor por su costo promedio 
de un 1GB en usd a principios del aÃ±o 2021. 
La columna grupo_region debe ser etiquetada de acuerdo a la siguiente regla:

-Si la región comienza con la letra A la etiqueta debe ser region_a.
-Si la región comienza con la letra E la etiqueta debe ser region_e.
-En los demás casos la etiqueta debe ser region_por_defecto.*/

val avgCost1G = pricesDF.join(usersDF, col("country_or_area") === col("name"), "left")

avgCost1G.filter(col("region").isNull).count

avgCost1G.count

import org.apache.spark.sql.functions.{avg, when, lit}

avgCost1G.filter(col("region").isNotNull)
.groupBy("region")
.agg(
  avg(col("average_price_of_1gb_usd_at_the_start_of_2021")).as("costo_prom_1_gb")
)
.orderBy("costo_prom_1_gb")
.withColumn("grupo_region", when(col("region").startsWith("A"), lit("region_a"))
                           .when(col("region").startsWith("E"), lit("region_e"))
                           .otherwise(lit("region_por_defecto"))
           )
.show
