package com.zyuc.stat.singleton

/**
 * Created by wangpf on 2017/7/24.
 */
object IotSourceKafkaDealSingleton extends Serializable {
  private lazy val timeType = "([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})".r

  def getTimeType = timeType
}
