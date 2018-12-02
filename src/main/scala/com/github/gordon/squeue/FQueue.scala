package com.github.gordon.squeue

import java.io.File

import com.github.gordon.squeue.core.FileQueue

/**
  * Created by Gordon Chan on 2017-11-11 00:22:00.
  */
class FQueue(
  val directory: File,
  val createDirIfNotExist: Boolean = true,
  val dataFileSizeMb: Int = 16
) extends java.util.AbstractQueue[Array[Byte]] with AutoCloseable {

  private val queue = new FileQueue(directory, createDirIfNotExist, dataFileSizeMb)

  override def iterator() = throw new UnsupportedOperationException

  override def size(): Int = synchronized(queue.size())

  override def offer(buff: Array[Byte]): Boolean = synchronized {
    checkBuf(buff)
    queue.add(buff)
    true
  }

  private def checkBuf(buf: Array[Byte]): Unit = {
    if (buf == null) throw new NullPointerException()
    if (buf.length > (dataFileSizeMb << 20)) {
      throw new IllegalArgumentException(s"Data too large, max length is ${dataFileSizeMb}mb")
    }
  }

  override def peek(): Array[Byte] = {
    queue.peek().orNull
  }

  override def poll(): Array[Byte] = synchronized {
    queue.poll().orNull
  }

  override def clear(): Unit = synchronized(queue.clear())

  override def close(): Unit = synchronized(queue.close())
}
