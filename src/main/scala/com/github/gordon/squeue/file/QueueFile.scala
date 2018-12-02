package com.github.gordon.squeue.file

import java.nio.MappedByteBuffer
import java.util.concurrent.{ExecutorService, TimeUnit}

import com.github.gordon.squeue.common.FQueueException

/**
  * queue文件接口
  * Created by Gordon Chan on 2017-11-11 00:44:00.
  */
private[squeue] trait QueueFile extends AutoCloseable {

  val VERSION = 1
  val MAGIC_CHARSET = "iso-8859-1"
  val DATA_FILE_HEADER_LENGTH = 16

  /**
    * 定义该文件的magic
    *
    * @return
    */
  def magic(): String

  /**
    * 初始化文件内容
    */
  def initFile(): Unit

  /**
    * 加载文件到内存
    */
  def loadFile(): Unit

  def closePool(pool: ExecutorService): Unit = {
    pool.shutdown()
    pool.awaitTermination(Int.MaxValue, TimeUnit.DAYS)
  }

  /**
    * 读取magic
    *
    * @return
    */
  def checkMagic(mbBuffer: MappedByteBuffer): Unit = {
    // read magic
    val magicBuf = new Array[Byte](8)
    mbBuffer.get(magicBuf)
    if (new String(magicBuf) != magic()) {
      throw FQueueException("File format error, magic is incorrect!")
    }
  }

}
