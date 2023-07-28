// Almacenamiento en caché
/*
Permite que spark conserve los datos en todos los cálculos y operaciones. 
Acelera los cálculos, sobre todo los interactivos.
Funciona almacenando la porción de RDD que permita la memoria;
si los datos para almacenar en caché son más grandes que la memoria disponible 
disminuirá el rendimiento porque se usará disco en lugar de memoria.
*/

val sc = spark.sparkContext

val rdd = sc.parallelize(1 to 10)

import org.apache.spark.storage.StorageLevel

//persist() puede usar memoria, disco o ambos
//cache() es sinónimo de persist(MEMORY_ONLY)

rdd.persist(StorageLevel.MEMORY_ONLY)/*Almacena RDD como objetos Java deserializados en la JVM. 
Si el RDD no cabe en la memoria, algunas particiones no se almacenarán en caché y se volverán 
a calcular sobre la marcha cada vez que se necesiten. Este es el nivel por defecto.
Si los RDD caben en memoria, esta es la opción más rápida para el rendimiento de ejecución*/

rdd.unpersist()

rdd.persist(StorageLevel.MEMORY_AND_DISK)/*Almacena RDD como objetos Java deserializados en la JVM.
Si el RDD no cabe en la memoria, almacena las particiones que no quepan en el disco
y las lee desde allí cuando sea necesario*/

rdd.persist(StorageLevel.DISK_ONLY)/*Almacena las RDD solo en disco.
No debe usarse a menos que los cálculos sean muy costosos*/

rdd.unpersist()

/*Utilice almacenamiento replicado para una mejor tolerancia a fallas si puede ahorrar la memoria adicional necesaria.
Esto evitará que se vuelvan a calcular las particiones perdidas para obtener la mejor disponibilidad*/

rdd.persist(StorageLevel.MEMORY_AND_DISK_2)//Igual que los niveles anteriores, pero replica cada partición en dos nodos del cluster

rdd.unpersist()

rdd.persist(StorageLevel.MEMORY_ONLY_2) //Igual que los niveles anteriores, pero replica cada partición en dos nodos del cluster

rdd.unpersist()

rdd.persist(StorageLevel.DISK_ONLY_2)//Igual que los niveles anteriores, pero replica cada partición en dos nodos del cluster

rdd.unpersist()

rdd.cache
