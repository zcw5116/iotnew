package com.zyuc.test

import java.util.concurrent.Executors

import org.apache.hadoop.fs.Path

/**
  * Created by zhoucw on 下午11:53.
  */
object Test {
  def main(args: Array[String]): Unit = {

    val dayid="20180225"

    var i=0;
    val executor = Executors.newFixedThreadPool(10)
    for( i<-0 until 24){
        executor.execute(new Runnable {
          override def run(): Unit = {
            println(i)
          }
        })

    }

    executor.shutdown()
  }
}
