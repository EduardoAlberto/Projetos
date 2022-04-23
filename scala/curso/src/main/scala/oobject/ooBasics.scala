package oobject

//constructor
class Person(name: String, val age:Int) {
  val x = 2
  println(1 + 3)
  def greet (name: String): Unit = println(s"${this.name} saus: hi $name")
  def greet(): Unit = println(s"Hi, I am $name")
  def this(name: String) = this(name, 0)
  def this() = this("John Doe")
}

class Writer(firstName: String, surname: String, val year: Int){
  def fullName: String = firstName + " " + surname
}

class Novel (name: String, year: Int, author: Writer){
  def authorAge = year - author.year
  def isWrittenBy (author: Writer) = author == this.author
  def copy(newYear: Int): Novel = new Novel(name, newYear, author)
}

class Counter(val count: Int = 0){
  def inc = {
    println("incrementing")
    new Counter(count + 1)
  }
  def dec = {
    println("decrement")
    new Counter(count - 1)
  }
  def inc(n: Int ): Counter = {
    if (n <= 0)this
    else inc.inc(n-1)
  }
  def dec(n: Int ): Counter =
    if (n <= 0) this
    else dec.dec(n-1)

  def print = println(count)
}

object ooBasics extends App{
  val person = new Person(name = "John", age = 0)
  println(person.x)
  person.greet("Daniel")
  person.greet()

  val author = new Writer("Charles","Dickens",1812)
  val imposter = new Writer(firstName = "Charles", surname = "Dickens", year = 1812)
  val novel = new Novel("Great Expectations", 1862, author)

  println(novel.authorAge)
  println(novel.isWrittenBy(author))

  val counter = new Counter
  counter.inc.print
  counter.inc(10).print


}
