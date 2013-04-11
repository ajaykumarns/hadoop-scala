/**
 * Created with IntelliJ IDEA.
 * User: anadathur
 * Date: 4/4/13
 * Time: 12:22 AM
 * To change this template use File | Settings | File Templates.
 */

trait trait1{
  val x: String
  val y: Int
  println("Main method running...")
}

trait trait2{
  println("Here I am trait2")
}

object Test extends App with trait2 with trait1{
  val x = "Hello"
  val y = 123
  println("Args are: " + args)
}
