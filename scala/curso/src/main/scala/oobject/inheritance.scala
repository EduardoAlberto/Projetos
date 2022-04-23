package oobject

import com.sun.prism.shader.AlphaOne_ImagePattern_Loader

object inheritance extends App{
  class Animal {
    val creatureType = "wild"
    protected def eat = println("nomnom")
  }

  class Cat extends  Animal{
    def crucch ={
      eat
      println("crunch crunch")
    }
  }
  val cat = new Cat
  cat.crucch

  class Person(name: String, age: Int){
    def this(name: String) =  this(name, 0)
  }
  class Adult (name: String, age: Int, idCard: String) extends Person(name, age)
  class Dog extends Animal {
    override val creatureType = "domestic"
    override def eat = println("crunch, crunch")
  }
  val dog = new Dog
  dog.eat
  println(dog.creatureType)

}
