package com.zyuc.stat.utils

/**
  * Created by slview on 17-7-2.
  */
object MathUtil {
  def divOpera(numerator:String, denominator:String ):String = {
    try {
      val ratio = if (denominator.toInt <=0 ) 0 else  if(denominator.toInt > numerator.toInt)  numerator.toFloat / denominator.toInt else 1
      f"$ratio%1.4f"
    } catch {
      case e => {
        println(e.getMessage)
        "0"
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(divOpera("3","4"))
  }
}
