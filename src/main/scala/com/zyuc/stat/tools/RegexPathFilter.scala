package com.zyuc.stat.tools

import org.apache.hadoop.fs.{PathFilter, Path}

/**
 * Created by wangpf on 2017/6/28.
 * desc:hdfs文件过滤方法实现类
 */
class RegexPathFilter(regexparam: String, moodparam: Int) extends PathFilter {
  private val regex: String = regexparam
  private val mood: Int = moodparam

  @Override
  def accept(path: Path): Boolean =  {
    var flag = false
    if (mood == 0) {
      flag = !path.toString().matches(regex)
    } else {
      flag = path.toString().matches(regex)
    }

    return flag
  }
}

object RegexPathFilter {
  val MOOD_FILTER: Int = 0  //将匹配的文件删除
  val MOOD_SELECT: Int = 1  //将匹配的文件保留
}
