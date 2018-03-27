package com.zyuc.stat.tools

import java.io.InputStream
import java.util.Properties

/**
 * Created by wangpf on 2017/7/17.
 */
trait GetProperties {
  def inputStreamArray: Array[InputStream]

  lazy val props: Properties = {
    val props = new Properties
    inputStreamArray.foreach( inputStream => {
      props.load(inputStream)
      inputStream.close()
    })

    props
  }
}
