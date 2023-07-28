// Persistencia de DataFrames
/*Tenemos disponibles los mismos tipos de persistencia para los DFs que para los RDDs.
Requerirá mucha menos memoria guardar en caché un DF que un RDD*/

val df = spark.read.parquet("/FileStore/section7/datos.parquet")

df.persist

df.unpersist

df.cache

df.unpersist

import org.apache.spark.storage.StorageLevel

df.persist(StorageLevel.DISK_ONLY)

df.unpersist

df.persist(StorageLevel.MEMORY_AND_DISK)

df.count
