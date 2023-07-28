// Funciones para trabajo con strings

val df = spark.read.parquet("/FileStore/section9/strings.parquet")

df.printSchema

df.show(false)

// trim, ltrim, rtrim

import org.apache.spark.sql.functions.{col, trim, ltrim, rtrim}

df.select(
  col("nombre"),
  ltrim(col("nombre")).as("lt"),//Retira los espacios a la izquierda
  rtrim(col("nombre")).as("rt"),//Retira los espacios a la derecha
  trim(col("nombre")).as("tr")//Retira los espacios a la izquierda y a la derecha
).show(false)

// lpad y rpad

import org.apache.spark.sql.functions.{lpad, rpad}

df.select(
  col("nombre"),
  lpad(trim(col("nombre")), 8, "-").as("lp"),//Rellena a la izquierda del string hasta cumplir con la longitud y el caracter que le proporcionemos
  rpad(trim(col("nombre")), 8, "=").as("rp")//Rellena a la derecha del string hasta cumplir con la longitud y el caracter que le proporcionemos

).show(false)

// concatenación, mayúscula, minúscula y reversa

import org.apache.spark.sql.functions.{concat_ws, lower, upper, initcap, reverse}

df.select(
  concat_ws(" ", col("sujeto"), col("verbo"), col("adjetivo")).as("frase")//Concatena el contenido de columnas, separándolo por el caracter que le indiquemos
).select(
  col("frase"),
  lower(col("frase")).as("lw"),//Convierte a minúscula todos los caracteres de una columna
  upper(col("frase")).as("up"),//Convierte a mayúscula todos los caracteres de una columna
  initcap(col("frase")).as("ic"),//Convierte a mayúscula la letra inicial de cada una de las palabras que componen un string
  reverse(col("frase")).as("rev")//Invierte la frase
).show(false)

// regexp_replace

import org.apache.spark.sql.functions.regexp_replace

df.select(
  concat_ws(" ", col("sujeto"), col("verbo"), col("adjetivo")).as("frase")
).select(
  col("frase"),
  regexp_replace(col("frase"), "Spark|es", "lindo").as("regexp")//Aplica un regex y sustitu,ye las coincidencias por lo que indiquemos
).show(false)
