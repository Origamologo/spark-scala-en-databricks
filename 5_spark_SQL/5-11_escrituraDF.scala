// Escritura de DataFrames
/*La clase DataFrameWriter es responsable de la lógica y la complegidad de escribir los datos de un DF en un sistema de almacenamiento externo.
 Una instancia de la clase DataFrameWriter está disponible como variable de escritura en la clase DataFrame a través de df.write.
 El patrón común para interactuar con DataFrameWriter es:

df.write.format(...).mode(...).partitionBy(...).sortBy(...).save(<path>)

El formato predeterminado en .parquet

Modos de guardado admitidos:
	append -> Agrega los datos del DF a la lista de archivos que ya existen en la ubicación de destino especificada.
	overwrite -> Sobreescribe completamente cualquier archivo de datos que ya exista en la ubicación de destino especificada con lo datos del DF.
	error -> Modo         Si existe la ubicación
	errorIfExist -> por   de destino especificada,
	default -> defecto    DataFrameWriter arrojará un error.
	ignore -> Si existe la ubicación de destino especificada, no hará nada.
*/


val df = spark.read.parquet("/FileStore/section7/lectura44/datos.parquet")

// Escribir el df como csv cambiándole el delimitador

df.write.format("csv").option("sep", "|").save("/FileStore/section7/lectura44/csv")

dbutils.fs.ls("/FileStore/section7/lectura44/csv")

// El número de archivos escritos en el directorio de salida corresponde al número de particiones que tiene un DataFrame

df.rdd.getNumPartitions

df.repartition(2).write.format("csv").option("sep", "|").mode("overwrite").save("/FileStore/section7/lectura44/csv")//overwrite eliminará todos los archivos y carpetasque existan en la dirección que le proporcionemos

dbutils.fs.ls("/FileStore/section7/lectura44/csv")

// Particionando los datos

df.printSchema

import org.apache.spark.sql.functions.col

df.select(col("comments_disabled")).distinct.show

val dfFiltrado = df.filter(col("comments_disabled").isin("False", "True"))

dfFiltrado.write.partitionBy("comments_disabled").parquet("/FileStore/section7/lectura44/parquet")//.partitionB creará tantas carpetas como valores diferentes tenga la columna indicada

dbutils.fs.ls("/FileStore/section7/lectura44/parquet")
