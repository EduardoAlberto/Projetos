package oobject

object sequence extends App{
  val aSequence = Seq(1,2,3,4)
  println(aSequence)
  println(aSequence.reverse)
}

object TuplesMaps extends App{
  val aTuple = (2, "Hello, Scala")
  println(aTuple._1)
  println(aTuple.copy(_2 = "goodby Java"))
  println(aTuple.swap)
}