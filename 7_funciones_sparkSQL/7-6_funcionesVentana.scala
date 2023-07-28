// Funciones de ventana
/*Las funciones de ventana operan con un número de filas y 
devuelven un valor único para cada fila de entrada*/

val df = spark.read.parquet("/FileStore/section9/funciones_ventana.parquet")

df.show(false)

import org.apache.spark.sql.expressions.Window/*Para realizar una operación en un grupo,
primero necesitamos particionar los datos utilizando Window.partitionBy*/

import org.apache.spark.sql.functions.{desc, row_number, rank, dense_rank, col}/*Para usar funciones
como row_number, rank o dense_rank hay que ordenar los datos de la partición con orderBy*/

val windowSpec = Window.partitionBy("departamento").orderBy(desc("puntos"))/*Con Window.partitionBy
se va a dividir el DF por los valores diferente de la columna "departamento"*/

df.withColumn("row_number", row_number().over(windowSpec)).filter(col("row_number").isin(1,2)).show(false)/*row_number es una función 
de ventana que se usa para dar el número de filas secuencial, desde uno hasta el resultado de
cada partición de ventana. Siempre se aplica sobre una partición de ventana que se especifica
con .over(<window_partition>)*/

df1.withColumn("rank", rank().over(windowSpec)).show/*rank se usa para proporcionar un rango
al resultado dentro de una partición de ventana. Deja huecos en el rango cuando hay empates.
Siempre se aplica sobre una partición de ventana que se especificacon .over(<window_partition>)*/

df1.withColumn("dense_rank", dense_rank().over(windowSpec)).show/*dense_run hace lo mismo que
run, pero sin dejar hueco cuando en el rango hay empates*/

// Agregación con funciones de ventana
/*Cuando trabajamos con funciones agregadas no es necesario aplicar orderBy*/

val windowSpecAgg = Window.partitionBy("departamento")

import org.apache.spark.sql.functions.{col, min, max, avg}
//Nos va a dar max, min y avg para cada una de las particiones
df.withColumn("min", min(col("puntos")).over(windowSpecAgg))
  .withColumn("max", max(col("puntos")).over(windowSpecAgg))
  .withColumn("avg", avg(col("puntos")).over(windowSpecAgg))
  .withColumn("row_number", row_number().over(windowSpec))/*Al añadir row_number tenemos que aplicarlo sobre la primera 
partición de ventana, que sí que tiene orderBy*/
.show
