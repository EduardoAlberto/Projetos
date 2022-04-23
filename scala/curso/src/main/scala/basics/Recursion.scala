package basics

import scala.annotation.tailrec
import scala.jdk.Accumulator

object Recursion extends App {

  def factorial(n: Int): Int =
    if (n <= 1)1
    else{
      println("Computing factorial of "+ n + "- I first need factorial of " + (n-1))
      val result = n * factorial(n-1)
      println("Computed factorial of " + n)
      result
    }
  println(factorial(10))
  def anotherFactorial (n: Int): BigInt = {
    @tailrec
    def factHelper (x: Int, accumulator: BigInt): BigInt =
      if (x <= 1) accumulator
      else factHelper(x - 1, x * accumulator)
    factHelper(n, 1)
  }
  print(anotherFactorial(5000))
  @tailrec
  def concatenateTailrec(aString: String, n: Int, accumulator: String): String =
    if (n <= 0) accumulator
    else concatenateTailrec(aString, n-1, aString + accumulator)
  println(concatenateTailrec("hello", 3, ""))
  def isPrime(n: Int): Boolean ={
    def isPrimeTailrec(t: Int, isStillPrime: Boolean): Boolean =
      if (!isStillPrime) false
      else if (t <=1)true
      else isPrimeTailrec(t - 1, n % t !=0 && isStillPrime)
    isPrimeTailrec(n / 2, true)
  }
  println(isPrime(2003))
  println(isPrime(629))

  def fibonacci (n: Int): Int = {
    def fiboTailerc(i: Int, last: Int, nexToLast: Int): Int =
      if (i >= n)last
      else fiboTailerc(i + 1, last + nexToLast, last)
    if (n <= 2)1
    else fiboTailerc(2, 1, 1)
  }
  println(fibonacci(8))

}
