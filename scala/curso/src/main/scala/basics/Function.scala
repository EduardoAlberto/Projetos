package basics

import com.sun.javafx.binding.SelectBinding.AsString

object Function extends  App {
    def aFunction(a: String, b: Int): String = {
      a + "" + b
    }
    println(aFunction("Hello", 3))

    def aParameterssFunction(): Int = 42
    println(aParameterssFunction())


    def aRepeatFunction(aString: String, n: Int): String = {
        if (n == 1) aString
        else aString + aRepeatFunction(aString, n-1)
    }
    print(aRepeatFunction("Hello", 3))

    def asFunctionWithDideEffects(aString: String): Unit = println(aString)

    def aBigFunction (n: Int): Int = {
        def aSmallerFunction(a: Int, b: Int): Int = a + b
        aSmallerFunction(n, n-1)
    }
    // greeting funcion (name, age) => "Hi, my name is $name and I am $age years old"
    def greetngForkids (name: String, age: Int): String =
        "Hi my nome is " + name + " and I am " + age + " years old."
    println(greetngForkids("David", 12))

    // Factorial Function 1 * 2 * 3 * .. * n
    def factorial (n: Int): Int =
        if (n <=0) 1
        else n * factorial(n-1)
    println(factorial(n=5))

    /* A Fibonacci function
       f(1) = 1
       f(2) = 1
       f(m) = f(n - 1) + f(n - 2)*/
    def fibonacci (n: Int): Int =
        if (n <= 2) 1
        else fibonacci(n-1) + fibonacci(n-2)
    println(fibonacci(n = 8))

    //Tests if a number is prime.
    def isPrime(n: Int): Boolean = {
        def isPrimeUntil(t: Int): Boolean =
            if (t <= 1) true
            else n % t != 0 && isPrimeUntil(t-1)
        isPrimeUntil(n / 2)
    }
    println(isPrime(n = 37))
    println(isPrime(2003))
    println(isPrime(37 * 17))

}
