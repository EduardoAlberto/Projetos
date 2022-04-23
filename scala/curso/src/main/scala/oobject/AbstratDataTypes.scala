package oobject

object AbstratDataTypes extends App{
  abstract class Animal {
    val creatureType: String
    def eat: Unit
  }
  class Dog extends Animal {
    override val creatureType: String = "Canine"
    def  eat: Unit = println("crunch crunch")
  }
  trait Carnivore {
    def eat(animal: Animal): Unit
  }
  class Crocodile extends Animal with Carnivore {
    val creatureType: String = "croc"
    def eat: Unit = println("nomnomnom")
    def eat(animal: Animal): Unit = println(s"i`m a croc and I`m eating  ${animal.creatureType}")
  }
  val dog = new Dog
  val croc = new Crocodile
  croc.eat(dog)
}
