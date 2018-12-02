package com.github.gordon.squeue.file.index

import java.io.{File, RandomAccessFile}
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode

import com.github.gordon.squeue.common.FQueueException
import com.github.gordon.squeue.file.QueueFile

/**
  * Created by Gordon Chan on 2017-11-11 00:34:00.
  */
private[squeue] class IndexFile extends QueueFile {

  // index file name
  val INDEX_FILE_NAME = "sfq.idx"

  // file format
  //  8, magic
  //  4, version
  //  4, read index
  //  4, read pos
  //  4, write index
  //  4, write pos
  //  8, queue size
  val INDEX_LIMIT_LENGTH = 36

  // file index and offset
  private var _readIdx: Int = -1
  private var _readPos: Int = -1
  private var _writeIdx: Int = -1
  private var _writePos: Int = -1
  private var _queueSize: Int = -1

  def readIdx = _readIdx

  def readPos = _readPos

  def writeIdx = _writeIdx

  def writePos = _writePos

  def queueSize = _queueSize

  def rotateReadFile(): Unit = {
    mbBuffer.position(12)
    _readIdx += 1
    mbBuffer.putInt(_readIdx)

    _readPos = DATA_FILE_HEADER_LENGTH
    mbBuffer.putInt(_readPos)
  }

  def rotateWriteFile(): Unit = {
    mbBuffer.position(20)
    _writeIdx += 1
    mbBuffer.putInt(_writeIdx)

    _writePos = DATA_FILE_HEADER_LENGTH
    mbBuffer.putInt(_writePos)
  }

  var file: File = _
  var raFile: RandomAccessFile = _
  var fc: FileChannel = _
  var mbBuffer: MappedByteBuffer = _

  def this(parent: File) = {
    this()
    this.file = new File(parent, INDEX_FILE_NAME)
    val exists = file.exists()
    raFile = new RandomAccessFile(file, "rwd")
    fc = raFile.getChannel
    mbBuffer = fc.map(MapMode.READ_WRITE, 0, INDEX_LIMIT_LENGTH)

    if (!exists) initFile()

    loadFile()
  }

  override def magic(): String = "sque_idx"

  override def initFile(): Unit = {
    mbBuffer.position(0)
    mbBuffer.put(magic().getBytes(MAGIC_CHARSET)) // magic(start:0)
    mbBuffer.putInt(VERSION) // version(start:8)
    mbBuffer.putInt(1) // put read index(start:12)
    mbBuffer.putInt(DATA_FILE_HEADER_LENGTH) // put read pos(start:16)
    mbBuffer.putInt(1) // put write index(start:20)
    mbBuffer.putInt(DATA_FILE_HEADER_LENGTH) // put write pos(start:24)
    mbBuffer.putInt(0) // put size pos(start:28)
  }

  /**
    * 加载文件
    */
  override def loadFile(): Unit = {
    if (raFile.length() < INDEX_LIMIT_LENGTH) {
      throw FQueueException("Index file format error, length incorrect!")
    }
    mbBuffer.position(0)
    checkMagic(mbBuffer)
    mbBuffer.getInt() // version
    _readIdx = mbBuffer.getInt()
    _readPos = mbBuffer.getInt()
    _writeIdx = mbBuffer.getInt()
    _writePos = mbBuffer.getInt()
    _queueSize = mbBuffer.getInt()
  }

  //  /**
  //    * 更新读文件的index
  //    *
  //    * @param idx
  //    */
  //  def updateReadIdx(idx: Int): Unit = {
  //    mbBuffer.position(12)
  //    mbBuffer.putInt(idx)
  //    this.readIdx = idx
  //  }

  /**
    * 滚动读文件的位置
    *
    * @param posDelta 读位置的增量
    */
  private def forwardReadPos(posDelta: Int): Unit = {
    val newPos = readPos + posDelta
    mbBuffer.position(16)
    mbBuffer.putInt(newPos)
    this._readPos = newPos
  }

  //  /**
  //    * 更新写文件的index
  //    *
  //    * @param idx
  //    */
  //  def updateWriteIdx(idx: Int): Unit = {
  //    mbBuffer.position(20)
  //    mbBuffer.putInt(idx)
  //    this.writeIdx = idx
  //  }

  /**
    *
    * 滚动写文件的位置
    *
    * @param posDelta 写位置的增量
    */
  private def forwardWritePos(posDelta: Int): Unit = {
    val newPos = writePos + posDelta
    mbBuffer.position(24)
    mbBuffer.putInt(newPos)
    this._writePos = newPos
  }

  private def updateQueueSize(newQueueSize: Int): Unit = {
    mbBuffer.position(28)
    mbBuffer.putInt(newQueueSize)
  }

  def increment(msgLen: Int): Unit = {
    increment(1, msgLen)
  }

  def increment(msgNum: Int, totalMsgLen: Int): Unit = {
    // 队列数据条数 + msgNum
    _queueSize += msgNum
    updateQueueSize(_queueSize)

    // 写位置 + (4 * msgNum + totalMsgLen)
    forwardWritePos(4 * msgNum + totalMsgLen)
  }

  def decrement(msgLen: Int): Unit = {
    decrement(1, msgLen)
  }

  def decrement(msgNum: Int, totalMsgLen: Int): Unit = {
    // 队列数据条数 - msgNum
    _queueSize -= msgNum
    updateQueueSize(_queueSize)

    // 读位置 + (4 * msgNum + totalMsgLen)
    forwardReadPos(4 * msgNum + totalMsgLen)
  }

  /**
    * 队列数据条数
    *
    * @return
    */
  def size(): Int = queueSize

  /**
    * 清空索引文件内容
    */
  def clear(): Unit = {
    mbBuffer.clear()
    mbBuffer.force()
    initFile()
    loadFile()
  }

  override def close(): Unit = {
    mbBuffer.force()
    mbBuffer.clear()
    fc.close()
    raFile.close()

    mbBuffer = null
    fc = null
    raFile = null
  }
}
