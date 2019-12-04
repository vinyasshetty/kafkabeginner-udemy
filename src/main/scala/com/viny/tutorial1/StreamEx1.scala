package com.viny.tutorial1

object StreamEx1 {

  def main(args: Array[String]): Unit = {

    val st1 = Stream.from(1)
    var cnt  = 0
    st1.takeWhile( _ => cnt < 10).foreach(x=> {
      cnt+=1
      println(x)})

    val l1 = List(1,2,3,4,5,6,7).iterator

    var cnt1 =0
    println("hello")
    l1.takeWhile(_ => cnt<4).foreach(x=>{
      cnt1+=1
      println(x)
    })

  }

}
