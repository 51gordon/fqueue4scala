package com.github.gordon.squeue.file.data

import java.io.File
import java.util.concurrent.{Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}

/**
  * Created by Gordon Chan on 2017-11-11 00:34:00.
  */
private[squeue] class WriteDataFile(
  dir: File,
  index: Int,
  dataFileSizeMb: Int,
  private var writePos: Int)
  extends DataFile(dir, index, dataFileSizeMb) {

  //  @volatile var shouldClose = false

  // 每隔一段时间自动flush
  val pool: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
    override def newThread(r: Runnable): Thread = new Thread(r, "DataFileForceThread")
  })
  pool.scheduleWithFixedDelay(new Runnable {
    override def run(): Unit = mbBuffer.force()
  }, 500, 500, TimeUnit.MILLISECONDS)

  /**
    * 是否已写满
    *
    * @param buf 数据
    * @return
    */
  def isFull(buf: Array[Byte]): Boolean = {
    (writePos + 4 + buf.length) > file.length()
  }

  //  /**
  //    * 是否已写满
  //    *
  //    * @param bufList 数据
  //    * @return
  //    */
  //  def isFull(bufList: Array[Array[Byte]]): Boolean = {
  //    pos + bufList.map(4 + _.length).sum > FILE_LIMIT
  //  }

  /**
    * 写数据
    *
    * @param buf
    */
  def write(buf: Array[Byte]): Unit = {
    // 写数据
    mbBuffer.position(writePos)
    mbBuffer.putInt(buf.length)
    mbBuffer.put(buf)
    writePos += (4 + buf.length)
  }

  /**
    * 写数据
    *
    * @param bufList
    */
  def write(bufList: Array[Array[Byte]]): Unit = {
    // 写数据
    mbBuffer.position(writePos)
    for (buf <- bufList) {
      mbBuffer.putInt(buf.length)
      mbBuffer.put(buf)
    }
    writePos += bufList.map(4 + _.length).sum
  }

  def readEndPosFromFile(): Int = {
    mbBuffer.position(12)
    mbBuffer.getInt()
  }

  /**
    * 读取writePos，写入到endPos
    */
  def writeEndPos2File(endPos: Int): Unit = {
    mbBuffer.position(12)
    mbBuffer.putInt(endPos)
  }

  /**
    * 关闭资源
    */
  override def close(): Unit = {
    // 设置close标志位
    //    shouldClose = true

    // 关闭force线程池
    closePool(pool)

    writeEndPos2File(writePos)

    super.close()
  }
}

