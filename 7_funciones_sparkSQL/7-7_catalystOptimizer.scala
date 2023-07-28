// Catalyst Optimizer
/*Spark Catalyst es un optimizador de consultas y segundo componente del módulo sparkSQL.
Sigue los siguientes pasos:

PLAN LÓGICO: traduce la lógica de procesamiento de datos escrita por el usuario en un plan lógico.
	1.-Crea un plan lógico a partir de un objeto DataFrame o del árbol de sintaxis 
	abstracta de la consulta SQL analizada. El plan lógico es una representación interna
	de la lógica de procesamiento de datos del usuario en forma de árbol de operaciones 
	y expresión.
	2.-Analiza el plan lógico apra resolver las referencias y asegurarse de que sean válidas.
	3.-Aplica al plan lógico una serie de optimizaciones basadas en reglas y en costos. Tanto
	las reglas como los costos siguen el principio de podar los datos innecesarios lo antes
	posible y minimizar el costo por operador.

PLAN FÍSICO:
	1.-Una vez que se optimiza el plan lógico, Catalyst generará uno o más planes físicos 
	utilizando los operadores físicos que coinciden con el motor de ejecución de spark.
	2.-Además de las optimizaciones realizadas en la fase del plan lógico, la fase del plan
	físico realiza sus propias optimizaciones basadas en raglas, que siguen el mismo principio
	de poda descrito en el punto 3 del plan lógico.
	3.-Finalmente Catalyst genera el código de bytes de Java del plan físico más económico.*/


val vuelos = spark.read.parquet("/FileStore/section8/vuelos.parquet")

display(vuelos)

import org.apache.spark.sql.functions.col

val nuevoDF = vuelos.filter(col("MONTH")isin(6, 7, 8))
		    .withColumn("distancia_tiempo_aire", col("DISTANCE") / col("AIR_TIME"))
		    .select(
			col("AIR_TIME"),
			col("distancia_tiempo_aire")
			).where(col("AIRLINE").isin("AA", "DL", "AS"))

nuevoDF.explain(true)//Esto nos muestra el plan de catalyst a la hora de ejecutar las funciones
