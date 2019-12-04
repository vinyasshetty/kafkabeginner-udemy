package com.viny.tutorial1

object ThreadEx2 {

  def main(args: Array[String]): Unit = {

    val t1 = new Runnable {
      override def run(): Unit = {
        println("hello")
        Thread.sleep(10000)
      }
    }

    val thread1 = new Thread(t1)

    thread1.start()
    println("hello main")

  }

}
