package com.github.gordon.squeue.file.data

import java.io.File

/**
  * Created by Gordon Chan on 2017-11-11 00:34:00.
  */
private[squeue] class ReadDataFile(dir: File, readIndex: Int, dataFileSizeMb: Int, var readPos: Int)
  extends DataFile(dir, readIndex, dataFileSizeMb) {

  /**
    * 读取一条数据
    *
    * @return
    */
  def readNext(commit: Boolean): Array[Byte] = readData(1, commit)(0)

  /**
    * 删除一条数据
    *
    * @return 返回被移除数据的字节数
    */
  def remove(): Int = removeData(1)

  /**
    * 读取n条数据
    *
    * @return
    */
  def readData(num: Int, commit: Boolean): Array[Array[Byte]] = {
    mbBuffer.position(readPos)
    val res = new Array[Array[Byte]](num)
    var delta = 0
    for (i <- 0 until num) {
      val len = mbBuffer.getInt
      val buf = new Array[Byte](len)
      mbBuffer.get(buf)
      delta += 4 + len
      res(i) = buf
    }
    if (commit) readPos += delta
    res
  }

  /**
    * 删除n条数据
    *
    * @return 返回被移除数据的字节数
    */
  def removeData(num: Int): Int = {
    mbBuffer.position(readPos)
    var totalMsgLen = 0
    for (_ <- 0 until num) {
      val len = mbBuffer.getInt
      mbBuffer.position(readPos + 4 + len)
      totalMsgLen += len
    }
    readPos += 4 * num + totalMsgLen
    totalMsgLen
  }
}

