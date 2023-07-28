// Funciones para trabajo con colecciones

/* /FileStore/section9/formato_array.parquet
   /FileStore/section9/formato_json.parquet */

val dfArray = spark.read.parquet("/FileStore/section9/formato_array.parquet")

dfArray.printSchema

dfArray.show(false)

// Obtener el tamaño del arreglo, ordenarlo y verificar si existe un valor en el arreglo

import org.apache.spark.sql.functions.{col, size, sort_array, array_contains, explode}

dfArray.select(
  size(col("tareas")).as("longitud"),//Nos da la longitud de un array
  sort_array(col("tareas")).as("arr_ordenado"),//Ordena el array
  array_contains(col("tareas"), "buscar agua").as("buscar_agua")//Nos dice si contiene o no lo que le digamos que busque, devuelve un booleano
).show(false)

// La función explode creará una nueva fila para cada elemento del array

dfArray.select(
  col("dia"),
  explode(col("tareas")).as("tareas")//Crea una nueva fila para cada elemento del array
).show(false)

// Trabajo con JSON

val dfJson = spark.read.parquet("/FileStore/section9/formato_json.parquet")

dfJson.printSchema

dfJson.show(false)

import org.apache.spark.sql.types.{StructType, StructField, StringType, ArrayType}

//Para transformar un json en un formato spark, definimos la estructura que tendrán los datos, creamos su esquema
val schemaJson = StructType(Array(
  StructField("dia", StringType, true),
  StructField("tareas", ArrayType(StringType), true)//Los true aquí indican que puede aceptar valores null
))

// Usar from_json para convertir el string JSON

import org.apache.spark.sql.functions.{from_json, to_json}

val dfJSON = dfJson.select(
  from_json(col("tareas_str"), schemaJson).as("por_hacer")//Convierte el string json en DF aplicándole el esquema que hemos creado
)

dfJSON.printSchema

// getItem para obtener elementos

dfJSON.select(
  col("por_hacer").getItem("dia"),//Obtenemos el elemento día
  col("por_hacer").getItem("tareas").getItem(0).as("primera_tarea"),//Obtenemos el primer elemento del array
  col("por_hacer").getItem("tareas").getItem(1).as("segunda_tarea")//Obtenemos el segundo elemento del array
).show(false)

// to_json para convertir a un string json

dfJSON.select(
  to_json(col("por_hacer"))//Pasamos el DF a un string tipo json pasándole la columna tipo Struq
).show(false)

val dfAux = dfJSON.select(to_json(col("por_hacer")))
dfAux.printSquema//Comprobamos que el esquema ahora es un string
