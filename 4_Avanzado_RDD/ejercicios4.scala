val sc = spark.sparkContext

val importes = sc.textFile("<path_to_file>")

importes.getNumPartitions

/*Para saber el número ideal de particiones para un RDD, 
lo ideal es trabajar con tantas particiones como cores tengamos disponibles,
así que debemos ver cómo está configurado nuestro entrono de trabajo*/

sc.defaultParallelism//Nos da la configuración del cluster que hemos creado

//Usar acumuladores para para obtener el total de ventas realizadas y el importe total de ventas

val totalVentas = sc.longAccumulator("totalVentas")
val importeTotal = sc.longAccumulator("importeTotal")

importes.foreach(x => totalVentas.add(1))//También valía importes.count
importes.foreach(x => importeTotal.add(x.toInt))

//Restar 10 a cada precio usando una variable broadcast para acelerar el proceso

val impuestos = sc.broadcast(10)

val ventaReal = importes.map(x => x.toInt - impuestos.value)//Crea un RDD con los valores de importes menos el valor de la broadcast

impuestos.destroy()

//Persistir el RDD ventaReal deb los siguientes niveles de persistencia:

import org.apache.spark.storage.StorageLevel

ventaReal.cache()//Es lo mismo que ventaReal.persist(StorageLevel.MEMORY_ONLY)
ventaReal.unpersist

ventaReal.persist(StorageLevel.DISK_ONLY)
ventaReal.unpersist

ventaReal.persist(StorageLevel.MEMORY_AND_DISK)
ventaReal.unpersist
