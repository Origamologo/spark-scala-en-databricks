# **spark-scala-en-databricks**
Apuntes de scala con spark en databriks
<br>

![Alt Text](https://github.com/Origamologo/spark-scala-en-databricks/blob/main/pics/spark.gif)

#
:rotating_light: <span style="color:red;">**AVISO**</span> :rotating_light:

* Estos apuntes de scala con spark en databriks están guapísimos porque se han hecho siguiendo el curso *Big Data y Spark: ingeniería de datos con Python y pyspark*, que está guapísimo y que podéis encontrar [aquí](https://www.udemy.com/course/draft/4860666/?referralCode=26A5490540622567AE88).
* En los apuntes se llama a diferentes fuentes de datos que no están incluídas en este repo. Las encontrareis en el [curso](https://www.udemy.com/course/draft/4860666/?referralCode=26A5490540622567AE88).
* El código a veces difiere, en ocasiones bastante, de lo que se ve en el [curso](https://www.udemy.com/course/draft/4860666/?referralCode=26A5490540622567AE88), sobre todo en la resolución de los ejercicios. Esto se debe a dos motivos:
  1. En el [curso](https://www.udemy.com/course/draft/4860666/?referralCode=26A5490540622567AE88) se hace todo en google collab y yo lo he hecho en local.
  2. Tengo mi propio estilo y manías programando y hay cosas que me gustan de una manera en concreto. Esto no significa que piense que el instructor, que es excelente, lo haga mal, tan sólo quuiere decir que tengo mis manías.

:warning: Baja un poco más y podrás ver el índice temático con lo que se cubre en cada lección, así te será más fácil encontrar información sobre una temática en concreto.
#
## Contenido

Los apuntes constan de 7 lecciones que encontrarás en  :open_file_folder: Lecciones\
A cada lección le corersponde un jupyter con ejercicios que encontrarás en  :open_file_folder: Ejercicios

## Índice

### :open_file_folder: Lecciones:

#### :mortar_board: ***1_SparkSession_RDD.ipynb***
  1. **SparkSession**
     * ¿Qué es?
     * ¿Cómo se crea?
       
  2. **RDD**
     * ¿Qué es?
     * ¿Cómo se crea?

#
#### :mortar_board: ***2_Transformaciones_RDD.ipynb***
  1. ***Transformaciones en un RDD***
     * Tipos de transformaciones
       
       1-1. **Función map**\
       1-2. **Función flatMap**\
       1-3. **Función filter**\
       1-4. **Función coalesce**\
       1-5. **Función repartition**\
       1-6. **Función reduceByKey**

#
#### :mortar_board: ***3_Acciones_RDD.ipynb***
  1. **Acciones en un RDD**
     * Tipos de acciones

       1-1. **Función reduce**\
       1-2. **Función count**\
       1-3. **Función collect**\
       1-4. **Funciones take, max y saveAsTextFile**

#
#### :mortar_board: ***4_avanzado_RDD.ipynb***
  1. **Almacenamiento en caché**
     * Valores posibles para el nivel de almacenamiento
     * ¿Qué nivel de almacenamiento elegir?
       
  2. **Partition y shuffling**
     
     2-1. **Particionadores**\
     2-2. **Shuffling (mezcla de datos)**
    
  4. **Broadcast variables**
     
  5. **Accumulator variables**

#
#### :mortar_board: ***5_spark_SQL.ipynb***
* ¿Qué es?
1. **Creación de DFs**
   
   1-1. **Crear un DF a partir de un RDD**\
   1-2. **Crear un DF a partir de fuentes de datos**
   * DataFrameReader
     
2. **Trabajo con columnas**
   
3. **Transformaciones**

   3-1. **Funciones select y selectExpr**\
   3-2. **Funciones filter y where**\
   3-3. **Funciones distinc y dropDuplicates**\
   3-4. **Funciones sort, orderBy y limit**\
   3-5. **Funciones whithColumn y withColumnRenamed**\
   3-6. **Funciones drop, sample y randomSplit**

4. **Trabajo con datos incorrectos o faltantes**
   
   4-1. **Eliminar nulos**\
   4-2. **Rellenar los valores nulos**

5. **Acciones sobre un DF en SparkSQL**
   
   5-1. **show()**\
   5-2. **take()**\
   5-3. **head()**\
   5-4. **collect()**

6. **Escritura de DFs**
   * append
   * overwrite
   * error, error if exist, default
   * ignore

7. **Persistencia de DFs**
   
#
#### :mortar_board: ***6_spark_SQL_avanzado.ipynb***
1. **Agregaciones**

   1-1. **Funciones count, countDistinct y aprox_count_distinct**\
   1-2. **Funciones min y max**\
   1-3. **Funciones sum, sum_distinct y avg**\
   1-4. **Agregación con agrupación**\
   1-5. **Varias agregaciones por grupo**\
   1-6. **Agregación con pivote**

2. **Joins**
   
   2-1. **Inner Join**\
   2-2. **Left Outer Join**\
   2-3. **Right Outer Join**\
   2-4. **Full Outer Join**\
   2-5. **Left Anti Join**\
   2-6. **Left Semi Join**\
   2-7. **Cross Join**

3. **Manejo de nombres de columnas duplicados**
   
4. **Shuffle Hash Join y Broadcast Hash Join**

#
#### :mortar_board: ***7_funciones_sparkSQL.ipynb***
1. **Funciones de fecha y hora**

   1-1. **Convertir de string a fecha y darle formato**
   * to_date, to_timestamp, date_format
     
   1-2. **Cálculos de fecha y hora**
   * datediff, months_between, last_day
     
   1-3. **Extraer valores específicos de una columna date**
   * year, month, dayofmonth, dayofyear, hour, minute, second

2. **Funciones para trabajar con strings**
   
   2-1. **Transformación de un string**
   * ltrim, rtrim, trim
   * lpad, rpad
   * concat_ws, lower, upper, initcap, reverse
  
   2-2. **Aplicación de expresiones regulares**
   * regexp_replace

3. **Funciones para trabajar con colecciones**
   
   3-1. **Trabajar con arrays**
   * size, sort_array, array_contains, explode

   3-2. **Trabajar con json**

4. **Funciones when, coalesce y lit**
5. **Funciones definidas por el usuario (UDF)**
   
   5-1. **Registrar el UDF con la función register()**\
   5-2. **Crear el UDF con la funcion udf()**\
   5-3. **Notación @udf**\
   5-4. **Pandas UDF**

6. **Funciones de ventana**

   6-1. **row_number**\
   6-2. **rank**\
   6-3. **dense_rank**\
   6-4. **Agregaciones con especificaciones de ventana**

7. **Catalyst Optimizer**
   * Plan Lógico
   * Plan Físico
