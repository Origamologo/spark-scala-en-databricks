// Funciones drop, sample y randomSplit
/*drop elimina las columnas especificadas del DF
sample devuelve un conjunto de filas seleccionado aleatoriamente del DF. Le damos un valor entre 0 y 1 que representa el porcentaje de filas que seleccionará. Se le puede añadir un seed para que siempre seleccione las mismas
randomsplit devuelve 1 o mas DFs y se usa para separar el train-split de ML. Le indicaremos con valores entre 0 y 1 el porcentaje de filas que irá a cada nuevo DF*/

val df = spark.read.parquet("/FileStore/section7/datos.parquet")

// drop

df.printSchema

val dfReducido = df.drop("comments_disabled", "ratings_disabled", "video_error_or_removed")

dfReducido.printSchema

// sample

val dfSample = df.sample(0.9)

val numFilas = df.count

val numSample = dfSample.count

val dfSampleSeed = df.sample(0.9, 1234)

dfSampleSeed.count

val dfSampleReplace = df.sample(true, 0.9, 1234)//El true habilita para repetir filas el tormar la muestra

dfSampleReplace.count

// randomSplit

val Array(train, test) = df.randomSplit(Array(0.9, 0.1))//Primero le domos los nombres de los nuevos DFs y luego aplicamos el randomSplit

println(df.count)

println(train.count)

println(test.count)
