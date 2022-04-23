package flickr

object ClienteFlickr extends App{
  val apiKey = "7936251ff32dea3bb308103e4c87c107"
  val method = "flickr.photos.search"
  val tags   = "scala"
  val url    = s"https://api.flickr.com/services/rest/?method=$method&api_key=$apiKey&tags=$tags"

  scala.io.Source.fromURL(url).getLines().foreach(println)
}
