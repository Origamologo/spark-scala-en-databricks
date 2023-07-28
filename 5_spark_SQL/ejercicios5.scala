val casosRaw = spark.read.option("header", "true").option("inferSchema", "true").csv("<path>")
casosRaw.printSchema

//Vamos a hacer un nuevo DF eliminado espacios en el nombre de la primero columna
val casos  =casosRaw.withColumnRenamed(" case_id", "case_id")

//Determinar las 3 ciudades con más casos confirmados y que sólo aparezcan las columnas provincia, ciudad y caso confirmado
import org.apache.spark.sql.functions.{col, desc}

casos.orderBy(desc("confirmed")).select(
	col("province"), 
	col("city"), 
	col("confirmed")
)
.filter(col("city") =!= "-" and col("city") =¡= "from other city")//Eliminamos los registros sin ciudad
.show(3, false)


val pacienteInfo = spark.read.option("header", "true").option("inferSchema", "true").csv("<>")
pacienteInfo.printSchema

//Vamos a segurarnos de que no contenga pacientes duplicados
pacienteInfo.count
pacienteInfo.select(col("patient_id")).distinc.count//con .distinc eliminamos los duplicados. Con .count comprobamos que hay un duplicado

val pacientes = pacienteInfo.dropDuplicates("patient_id")//Eliminamos duplicados en patient_id
pacientes.count

//Cuántos pacientes informaron por quién se contagiaron. Obtenga los que tengan informado por quién se contagiaron
import org.apache.spark.sql.functions.count

pacientes.select(count(col("indected_by")).as("conteo")).show

val pacientesConInfoContagios = pacientes.filter(col("indected_by").iNotNull)
pacientesConInfoContagios.count

//Obtener solo los pacientes femeninos a partir del DF que acabamos de obtener
val pacientesConInfoContagiosFem = pacientesConInfoContagios.filter(col("sex") === "female").drop("released_date", "deceased_date")
pacientesConInfoContagiosFem.printSchema

display(pacientesConInfoContagiosFem)

//Establecer el número de particiones del DF en dos. Escribir el DF resultante en un archivo parquet en modo overwrite, dividiendo la salida por provincia
pacientesConInfoContagiosFem.rdd.getNumPartitions

pacientesConInfoContagiosFem.repartiton(2).write.partitionBy("province").mode("overwrite").parquet("<path>")
dbutils.fs.ls("<path>")
