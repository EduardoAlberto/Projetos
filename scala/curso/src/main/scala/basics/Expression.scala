package basics

object Expression extends App {
  val x = 1 + 2 // Expression
  println(x)
  println(2 + 3 * 4)

  var aVariable = 2
  aVariable += 2
  println(aVariable)

  //if expressipm
  val aCondition = true
  val aConditionedValue = if (aCondition) 5 else 3
  println(aConditionedValue)
  println(if(aCondition) 5  else 3)
  println( 1 + 3)

  var i = 0
  val awhile = while (i < 10){
      println(i)
      i += 1
    // NEVER WHITE THIS AGAIN
    // EVERYTHING in Scala is an Expression !

    val aWeirdValue = (aVariable = 3)
    println(aWeirdValue)

    // Code blocks
    val aCodeBlock = {
        val y = 2
        val z = y + 1

        if (z > 2) "hello " else "goodbye"
      }
      //1. differnce between "hello workd" vs println ("hello workd")
      //2.
      val someValue = {
        2 < 3
      }
      println(someValue)

      val someOtherValue = {
        if (someValue) 239 else 986
        42
      }
      println(someOtherValue)
  }


}
