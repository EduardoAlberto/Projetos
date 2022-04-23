package oobject

object anonymousFunctions extends App{
  val doubler = (x: Int) => x * 2
  val justdoSomething: () => Int = () => 3


  println(doubler(3))
  println(justdoSomething())

}
