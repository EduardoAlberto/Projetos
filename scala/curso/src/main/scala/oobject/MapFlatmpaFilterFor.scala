package oobject

object MapFlatmpaFilterFor extends App {
  val list = List(1, 2, 3)
  println(list)
  println(list.head)

  //mao
  println(list.map(_ + 1))
  println(list.map(_ + "is a number"))

  //filter
  println(list.filter(_ % 2 == 0))

  //flatMap
  val toPair = (x: Int) => List(x, x+1)
  println(list.flatMap(toPair))

  //print all combination between two lists
  val numbers = List( 1,2,3,4)
  val chars = List('a','b','c','d')
  val colors = List("black", "white")

  //List()
  val comb = numbers.flatMap(n => chars.map(c => "" + c + n))
  val combs = numbers.flatMap(n => chars.flatMap(c => colors.map(color => ""+ c + n + color)))
  println(comb)
  println(combs)

  //foreach
  list.foreach(println)

  //for-compreehensions
  val forCombi = for {n <- numbers
    if n % 2 == 0
      c <- chars
      color <- colors
  } yield  "" + c + n + "-" + color
    println(forCombi)
  for {
    n <- numbers
  }println(n)
  //systax overland
  list.map( x =>
    x * 2
  )
}
