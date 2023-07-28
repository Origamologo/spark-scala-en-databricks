# **Spark + Scala en databricks**
Apuntes de scala con spark en databriks
<br>

![Alt Text](https://github.com/Origamologo/spark-scala-en-databricks/blob/main/pics/spark.gif)

#
:rotating_light: <span style="color:red;">**AVISO**</span> :rotating_light:

* Estos apuntes de scala con spark en databriks están guapísimos porque se han hecho siguiendo el curso *Spark y Scala en Databricks: Big Data e ingeniería de datos*, que está guapísimo y que podéis encontrar [aquí](https://www.udemy.com/course/draft/4860666/?referralCode=26A5490540622567AE88).
* En los apuntes se llama a diferentes fuentes de datos que no están incluídas en este repo. Las encontrareis en el [curso](https://www.udemy.com/course/draft/4860666/?referralCode=26A5490540622567AE88).
* El código a veces difiere, en ocasiones bastante, de lo que se ve en el [curso](https://www.udemy.com/course/draft/4860666/?referralCode=26A5490540622567AE88), sobre todo en la resolución de los ejercicios. Esto se debe a dos motivos:
  1. El código está plagado de mis propias notas , explicaciones y aclaraciones, dando en ocasiones diferentes caminos para hacer lo mismo.
  2. Tengo mi propio estilo y manías programando y hay cosas que me gustan de una manera en concreto. Esto no significa que piense que el instructor, que es excelente, lo haga mal, tan sólo quiere decir que tengo mis manías.

:warning: Baja un poco más y podrás ver el índice temático con lo que se cubre en cada lección, así te será más fácil encontrar información sobre una temática en concreto.
#
## Contenido

Los apuntes constan de 7 lecciones divididas en 7  :open_file_folder: carpetas\
A cada lección le corersponde un archivo con ejercicios que encontrarás en cada una de las  :open_file_folder: carpetas.

## Índice

### :mortar_board: Lecciones:

#### :open_file_folder: ***1_SparkSession_RDD***
       1-1. **SparkSession**
           * ¿Qué es?
           * ¿Cómo se crea?
       
       1-2. **RDD**
           * ¿Qué es?
           * ¿Cómo se crea?

#
#### :open_file_folder: ***2_Transformaciones_RDD.ipynb***
***Transformaciones en un RDD***
     * Tipos de transformaciones
       
       2-1. **Función map**
       2-2. **Función flatMap**
       2-3. **Función filter**
       2-4. **Función coalesce**
       2-5. **Función repartition**
       2-6. **Función reduceByKey**

#
#### :open_file_folder: ***3_Acciones_RDD.ipynb***
**Acciones en un RDD**
     * Tipos de acciones

       3-1. **Función reduce**
       3-2. **Función count**
       3-3. **Función collect**
       3-4. **Funciones take, max y saveAsTextFile**

#
#### :open_file_folder: ***4_Avanzado_RDD.ipynb***
       4-1. **Almacenamiento en caché**
           * Valores posibles para el nivel de almacenamiento
           * ¿Qué nivel de almacenamiento elegir?

       4-2. **Broadcast variables**
       4-3. **Acumuladores**

#
#### :open_file_folder: ***5_spark_SQL.ipynb***
* ¿Qué es?
**Creación de DFs**
   
       5-1. **Crear un DF a partir de un RDD**
       5-2. **Crear un DF a partir de fuentes de datos**
       5-3. **Trabajo con columnas**

**Transformaciones**

       5-4. **Funciones select y selectExpr**
       5-5. **Funciones filter y where**
       5-6. **Funciones distinc y dropDuplicates**
       5-7. **Funciones whithColumn y withColumnRenamed**
       5-8. **Funciones drop, sample y randomSplit**

**Trabajo con datos incorrectos o faltantes**
   
       5-9. **Eliminar o sustiruir nulos**

**Acciones sobre un DF en SparkSQL**
   
       5-10. **show(), head(), take(), collect() y count**

**Escritura de DFs**

       5-11. **append, overwrite, error, error if exist, default, ignore

**Persistencia de DFs**

       5-12. persist, cache, unpersist
   
#
#### :open_file_folder: ***6_spark_SQL_avanzado.ipynb***
**Agregaciones**

       6-1. **Funciones count, countDistinct y aprox_count_distinct**\
       6-2. **Funciones min y max**\
       6-3. **Funciones sum, sum_distinct y avg**\
       6-4. **Agregación con agrupación**\
       6-5. **Varias agregaciones por grupo**\
       6-6. **Agregación con pivote**

**Joins**
   
       6-7. **Inner Join**\
       6-8. **Left Outer Join**\
       6-9. **Right Outer Join**\
       6-10. **Full Outer Join**\
       6-11. **Left Anti Join**\
       6-12. **Left Semi Join**\
       6-13. **Cross Join**

**Manejo de nombres de columnas duplicados**

       6-14. **Nombres de columna duplicados**
   
**Shuffle Hash Join y Broadcast Hash Join**

       6-15. **Qué son y cómo forzar el broadcast**

#
#### :open_file_folder: ***7_funciones_sparkSQL.ipynb***
**Funciones de fecha y hora**

       7-1. **Convertir de string a fecha y darle formato, calcular fechas y extraer valores**
           * to_date, to_timestamp, date_format
           * datediff, months_between, last_day
           * year, month, dayofmonth, dayofyear, hour, minute, second

**Funciones para trabajar con strings**
   
       7-2. **Transformación de un string y aplicación de regex**
           * ltrim, rtrim, trim
           * lpad, rpad
           * concat_ws, lower, upper, initcap, reverse
           * regexp_replace

**Funciones para trabajar con colecciones**
   
       7-3. **Trabajar con arrays y con json**
           * size, sort_array, array_contains, explode
           * from_json, to_json, getItem()

**Funciones para evaluar condiciones y para sustituir nulos**

       7-4. Funciones when + otherwise y coalesce + lit
   
**Definir y aplicar funciones (UDF)**
   
       7-5. User Defined Functions (UDF)

**Aplicar operaciones a grupos**

       7-6. Funciones de ventana
           * row_number, rank, dense_rank
           * Agregaciones con especificaciones de ventana

8. **Optimización automática de las consultas**

        7-7. Catalyst Optimizer
            * Plan Lógico
   
           * Plan Físico
