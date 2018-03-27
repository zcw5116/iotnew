package com.zyuc.stat.utils

import java.io.PrintWriter

/**
 * Created by wangpf on 2017/6/20.
 * desc:一些常规处理的函数
 */
object CommonUtils extends Serializable {
  /**
   * Created by wangpf on 2017/6/20.
   * desc:更新BPTime文件
   */
  def updateBptime(fileName: String, BPTime: String) = {
    val out = new PrintWriter(fileName)
    out.print(BPTime)
    out.close()
  }
}
