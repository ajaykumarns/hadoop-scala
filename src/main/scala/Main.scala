object Main extends App{
  val regex = """(?i)(jpg|gif|jpeg)""".r
  for (word <- regex.findAllIn("96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] \"GET /cat.JPG HTTP/1.1\" 200 12433")){
    println(word)
  }
}