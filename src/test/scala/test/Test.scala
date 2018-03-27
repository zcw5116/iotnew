package test

/**
  * Created by zhoucw on 17-10-26.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val rkey = "201710261310_3g_P100002368_C_-1_3608FFFF1C70"
    val a = rkey.split("_", 6)
    println(a(5))
  }
}
