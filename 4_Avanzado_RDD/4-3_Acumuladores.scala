// Acumuladores
/*Son variables compartidas entre ejecutores que normalmente
se utilizan para agregar contadores a su programa spark.
Spark de forma predetermida proporciona acumuladores de tipo long, double y collection.
Los acumuladores proporcionan una serie de funciones para trabajar con ellos
como por ejemplo isZero, reset, add, value...*/

val sc = spark.sparkContext

// longAccumulator

val acumuladorLong = sc.longAccumulator("longAccumulator")

sc.parallelize(1 to 5).foreach(x => acumuladorLong.add(x))//Va a guardar en el acumulador la suma de los nº del 1 al 5

acumuladorLong.reset//reseteamos el valor del acumulador

acumuladorLong.value

acumuladorLong.isZero

// doubleAccumulator

val acumuladorDouble = sc.doubleAccumulator("doubleAccumulator")

sc.parallelize(1 to 50).foreach(x => acumuladorDouble.add(1))//Este acumulador es un contador
