// Crear un DataFrame a partir de un RDD

val sc = spark.sparkContext

val rdd = sc.parallelize(1 to 10).map(x => (x, x * x))

rdd.collect

import spark.implicits._

val dfSinNombreDeColumnas = rdd.toDF()//Creamos el DF

dfSinNombreDeColumnas.show()//Vemos cómo está constituído el DF

val dfConNombreDeColumnas = rdd.toDF("numero", "cuadrado")//Le pasamos el nombre de las columnas como parámetros

dfConNombreDeColumnas.show()

// Creando un dataFrame a partir de un RDD con schema

val rddData = sc.parallelize(Seq((1, "jose", 34.5), (2, "julia", 45.9)))

// Transformando el RDD para que contenga instancias de Row

val rddFila = rddData.map(x => Row(x._1, x._2, x._3))

// Creando el schema
import org.apache.spark.sql.types._

// Vía 1
val schema = StructType(Array(//Creamos un array
  StructField("id", IntegerType, true),//nombre del campo, tipo de dato, si acepta o no nulos
  StructField("nombre", StringType, true),
  StructField("monto", DoubleType, true)
  )
)

// Vía 2
// La segunda alternativa será crear un esquema a partir de un DLL usando la funcion fromDDL()

val dllSchemaStr = "`id` INT, `nombre` STRING, `monto` DOUBLE"//Se crea un string con el nombre del campo y el tipo de dato

val ddlSchema = StructType.fromDDL(dllSchemaStr)//Creamos el schema con fromDDL

// Creando los dataframe

val df1 = spark.createDataFrame(rddFila, schema)

val df2 = spark.createDataFrame(rddFila, ddlSchema)

println("df1")
df1.show()

println("df2")
df2.show()

df1.printSchema//Nos da el esquema del DF
df2.printSchema

//Crear un dataframe a partir de un rango de números

spark.range(5).toDF("id").show()

spark.range(0, 20, 2).toDF("id").show()
