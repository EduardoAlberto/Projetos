package basics

object DefaultArgs extends App {
  def trFact (n: Int, acc: Int = 1): Int =
    if (n <= 1)acc
    else trFact(n-1, n*acc)
  val fact10 = trFact(n=10, acc = 2)

  def savePciture(format: String = "jpg", width: Int = 1920, height: Int = 1080): Unit = println("saving picture")
  savePciture("bmp", width = 800,height = 600)

}
