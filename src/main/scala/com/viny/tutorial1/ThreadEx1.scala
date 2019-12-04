package com.viny.tutorial1

import java.util.concurrent.Executors

object ThreadEx1 {
  def main(args: Array[String]): Unit = {

    val thread = new Thread{
      override def run(): Unit = {
        Thread.sleep(1000)
        println("hello")
        println("In thread " + Thread.currentThread().getName())
      }
    }

    println(Thread.currentThread().getName())
    thread.start()
    //Thread.sleep(1000)
    println("Hello in main")

    val execs = Executors.newFixedThreadPool(10)

  }

}
