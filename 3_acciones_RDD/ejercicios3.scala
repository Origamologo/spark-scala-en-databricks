<RDD>.top(<number>)//Nos da el número de valores del rdd que le indiquemos, empezando por el más alto

def facctotial(n: Int) Int = {
  if (n == 0) {
    return 1
  { else {
    returnsc.parallelize(1 to n).reduce(_*_)
  }
}

def sumaImpar(n: Int): Int = {
    if (n<=1) {
      return -1
    } else {
      return sc.parallelize(1 to n).filter(_ % 2 != 0).reduce(_+_)
    }

}
