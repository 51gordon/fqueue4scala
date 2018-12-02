package com.github.gordon.squeue.file.data

import java.io.{File, RandomAccessFile}
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode

import com.github.gordon.squeue.file.QueueFile

import scala.util.matching.Regex

/**
  * Created by Gordon Chan on 2017-11-11 00:34:00.
  */
private[squeue] class DataFile extends QueueFile {

  private var _fileIndex: Int = _
  private var _fileEndPos: Int = _

  var file: File = _
  var raFile: RandomAccessFile = _
  var fc: FileChannel = _
  var mbBuffer: MappedByteBuffer = _

  def this(parent: File, index: Int, dataFileSizeMb: Int) = {
    this()
    this._fileIndex = index
    this.file = new File(parent, s"sfq_$index.dat")
    val exists = file.exists()
    val expectFileLength = DATA_FILE_HEADER_LENGTH + 4 + dataFileSizeMb * (1 << 20)
    this.raFile = new RandomAccessFile(file, "rwd")
    this.fc = raFile.getChannel
    val fileLength = math.max(file.length().toInt, expectFileLength)
    this.mbBuffer = fc.map(MapMode.READ_WRITE, 0, fileLength)

    if (!exists) initFile()

    loadFile()
  }

  override def magic(): String = "sque_dat"

  override def initFile(): Unit = {
    mbBuffer.put(magic().getBytes(MAGIC_CHARSET)) // put magic(start: 0)
    mbBuffer.putInt(VERSION) // put version(start:8)
    mbBuffer.putInt(-1) // put write end position
  }

  override def loadFile(): Unit = {
    mbBuffer.position(0)
    checkMagic(mbBuffer)
    mbBuffer.getInt() // version
    _fileEndPos = mbBuffer.getInt() // get file end pos
  }

  def fileEndPos(): Int = {
    mbBuffer.position(12)
    mbBuffer.getInt()
  }

  //  def readEndPosFromFile(): Int = {
  //    mbBuffer.position(12)
  //    mbBuffer.getInt()
  //  }

  //  /**
  //    * 读取writePos，写入到endPos
  //    */
  //  def writeEndPos2File(endPos: Int): Unit = {
  //    mbBuffer.position(12)
  //    mbBuffer.putInt(endPos)
  //  }

  def getIndexByFileName: Int = _fileIndex

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

object DataFile {
  val matchPattern: String = "sfq_\\d+\\.dat"
  val capturePattern: Regex = "sfq_(\\d+)\\.dat".r

  def getIndexByFileName(name: String): Int = {
    capturePattern.findFirstMatchIn(name) match {
      case Some(a) => a.group(1).toInt
      case None => -1
    }
  }

  def isDataFile(name: String): Boolean = name.matches(matchPattern)
}