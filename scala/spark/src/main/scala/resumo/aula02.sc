// FLOW CONTROL
if ( 1 > 3) println("Impossible !") else println("the world makes sens")
if ( 1 > 3) {
  println("Impossible")
  println("Really?")
} else {
  println("the world makes sens")
  println("still.")
}
// MATCHING
val number = 30
number match{
  case 1 => println("One")
  case 2 => println("Two")
  case 3 => println("Three")
  case _ => println("something else")
}

for (x <- 1 to 4) {
  val squared = x * x
  println(squared)
}
var x =10
while (x >= 0){
  println(x)
  x -= 1
}
x = 0
do {println(x); x+=1 } while (x <= 10)
//Expressions

{val x =10;  x + 20 }
println({val x = 10; x + 20})

// fibonatinn
def fibonati(x: Int): Int = {
  x * x
}

def cubeIt(x: Int): Int = {
  x * x * x
}

def transform(x: Int, f: Int => Int): Int = {
  f(x)
}
val result = transform(2, cubeIt)


println(fibonati(2))
println(cubeIt(3))

println(result)
transform(3, x => x * x * x)
transform(10, x => x / 2)
transform(3, x => {val y = x * 2; y * y})

//data structures

//tuples
val captain = ("Picard", "Enterprise-D", "NCC-1701-D")
println(captain)
println(captain._1)
println(captain._2)
println(captain._3)

//LIST
//LIKE A TUPLE, BUT MORE FUNCTIONALITY
val lista = List("Enterprise", "Defiant", "Voyager", "Deep")
println(lista(1))

//Inicio FIm
println(lista.head)
println(lista.tail)

// loop for para percorrer toda uma lista
for (ship <- lista){
  println(ship)
}

val backwardShips = lista.map((ship: String) => {ship.reverse})
for (ship <- backwardShips){println(ship)}

// reduce()
val listaNumero = List(1, 2, 3, 4, 5)
val sum = listaNumero.reduce((x: Int, y: Int) => x + y)
println(sum)

//Filter()
val cinco = listaNumero.filter((x: Int) => x != 5)
val tres = listaNumero.filter(_ != 3)

//Cibcatebate lists
val maisNumeros = List(6,7,8)
val lotsOfNumbers = listaNumero ++ maisNumeros
val reverter = listaNumero.reverse
val sortear = reverter.sorted
val duplicar = listaNumero ++ listaNumero
val destintos = duplicar.distinct
val maiorValor = listaNumero.max
val total = listaNumero.sum

//MAPS
val shipMap = Map("Kirk" -> "Enterprise", "Picard" -> "Enterprise-D", "Sisko" -> "Deep Space Nine", "Janeway " -> "Voyager")
println(shipMap("Kirk"))
println(shipMap.contains("Picard"))