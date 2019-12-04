
import scala.util.{Failure, Success, Try}

class Am(val x:String){
  def disp(y:String)={
    println(this.x+y)
  }
}

val c1 = classOf[Am]
c1.getMethods

Try{1/5} match {
  case Failure(x) => {
    x match {
      case x:ArithmeticException => println("fail " + x.getMessage)
      case _ => println("Fail")
    }

  }
  case Success(x) => println(s"Succes ${x}")
}