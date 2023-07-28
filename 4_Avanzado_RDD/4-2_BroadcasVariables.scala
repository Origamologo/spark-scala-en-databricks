// Broadcast Variable
/*Las variables broadcast dson variables compartidas entre todos los ejecutores.
Se crean una vez en el driver y luego se leen solo en los ejecutores.*/

val sc = spark.sparkContext

val uno = 1

val brUno = sc.broadcast(uno)//Esta variable estará disponible para que todas las tareas puedan usar los datos de brUno

brUno.value//Para ver el contenido de la variable

brUno.value + 1

brUno.destroy//Para eliminar la variable

brUno.value + 1
