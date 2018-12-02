package com.github.gordon.squeue.core

import java.io.{File, FilenameFilter}

import com.github.gordon.squeue.file.data._
import com.github.gordon.squeue.file.index.IndexFile

/**
  * Thread unsafe queue implementation
  * Created by Gordon Chan on 2017-11-11 00:26:00.
  */
private[squeue] class FileQueue {
  private var dir: File = _
  private var dataFileSizeMb: Int = _
  // 初始化索引文件和待处理的数据文件
  private var idxFile: IndexFile = _
  private var readFile: ReadDataFile = _
  private var writeFile: WriteDataFile = _

  def this(dir: File, createDirIfNotExist: Boolean, dataFileSizeMb: Int) = {
    this()
    (dir.exists(), dir.isFile) match {
      case (true, false) =>
      // do nothing
      case (true, true) =>
        throw new IllegalArgumentException(s"$dir not a directory!")
      case (false, _) =>
        if (createDirIfNotExist) dir.mkdirs()
        else throw new IllegalArgumentException(s"Directory $dir not exist!")
    }
    this.dir = dir
    this.dataFileSizeMb = dataFileSizeMb
    this.idxFile = new IndexFile(dir)
    this.readFile = new ReadDataFile(dir, idxFile.readIdx, dataFileSizeMb, idxFile.readPos)
    this.writeFile = new WriteDataFile(dir, idxFile.writeIdx, dataFileSizeMb, idxFile.writePos)
  }

  def add(buf: Array[Byte]): Unit = {
    if (writeFile.isFull(buf)) {
      rotateWriteFile()
    }
    writeFile.write(buf)
    idxFile.increment(buf.length)
  }

  //  def add(bufList: Array[Array[Byte]]): Unit = {
  //    if (writeHandler.isFull(bufList)) {
  //      rotateWriteFile()
  //    }
  //    writeHandler.write(bufList)
  //    idxFile.incrementSize(bufList.length, bufList.map(_.length).sum)
  //  }

  def add(bufList: Array[Array[Byte]]): Unit = {
    for (buf <- bufList) {
      if (writeFile.isFull(buf)) {
        rotateWriteFile()
      }
      writeFile.write(buf)
    }
    idxFile.increment(bufList.length, bufList.map(_.length).sum)
  }

  private def runHandler[T](f: () => T): Option[T] = {
    idxFile.readIdx - idxFile.writeIdx match {
      case delta: Int if delta == 0 => // 读写同一个文件
        if (idxFile.readPos < idxFile.writePos) Some(f()) else None
      case delta: Int if delta < 0 => // 读文件落后于写文件
        if (idxFile.readPos >= readFile.fileEndPos) rotateReadFile() // 如果当前文件已经读完，那么滚动到下一个文件继续读
        Some(f())
      case delta: Int if delta > 0 => // 这种情况不可能出现
        throw new IllegalStateException("Read index > write index, this is a bug!")
    }
  }

  private def readNext(commit: Boolean): Option[Array[Byte]] = {
    runHandler(() => readFile.readNext(commit))
  }

  private def readData(maxNum: Int, commit: Boolean): Array[Array[Byte]] = {
    idxFile.size() match {
      case i if i == 0 => Array()
      case i if i > 0 =>
        val min = math.min(i, maxNum.toLong).toInt
        runHandler(() => readFile.readData(min, commit)).get
    }
  }

  /**
    * 获取一条数据，不从队列删除
    *
    * @return
    */
  def peek(): Option[Array[Byte]] = {
    readNext(false)
  }

  /**
    *
    * @param maxNum 一次性获取maxNum条记录，不从队列删除
    * @return
    */
  def peek(maxNum: Int): Array[Array[Byte]] = {
    readData(maxNum, commit = false)
  }

  /**
    * 获取一条数据，并从队列删除
    *
    * @return
    */
  def poll(): Option[Array[Byte]] = {
    val bufOpt = readNext(true)
    if (bufOpt.isDefined) {
      idxFile.decrement(bufOpt.get.length)
    }
    bufOpt
  }

  /**
    *
    * @param maxNum 一次性poll的记录条数
    * @return
    */
  def poll(maxNum: Int): Array[Array[Byte]] = {
    val bufList = readData(maxNum, commit = true)
    if (bufList.length > 0) {
      idxFile.decrement(bufList.length, bufList.map(_.length).sum)
    }
    bufList
  }

  /**
    * 移除队列下一个元素
    */
  def remove(): Unit = {
    runHandler(() => {
      val len = readFile.remove()
      idxFile.decrement(len)
    })
  }

  /**
    *
    * @param num 一次性remove的记录条数
    * @return
    */
  def remove(num: Int): Unit = {
    runHandler(() => {
      val len = readFile.removeData(num)
      idxFile.decrement(num, len)
    })
  }

  /**
    * 获取队列数据条数
    *
    * @return
    */
  def size(): Int = idxFile.size()

  /**
    * 清空文件队列
    */
  def clear(): Unit = {
    idxFile.clear()

    readFile.close()
    writeFile.close()

    cleanAllDataFile()

    readFile = new ReadDataFile(dir, idxFile.readIdx, dataFileSizeMb, idxFile.readPos)
    writeFile = new WriteDataFile(dir, idxFile.writeIdx, dataFileSizeMb, idxFile.writePos)
  }

  /**
    * 关闭文件队列
    */
  def close(): Unit = {
    writeFile.close()
    readFile.close()
    idxFile.close()

    cleanReadDoneDataFile()

    writeFile = null
    readFile = null
    idxFile = null
  }

  private def rotateReadFile(): Unit = {
    idxFile.rotateReadFile()
    readFile.close()
    readFile = new ReadDataFile(dir, idxFile.readIdx, dataFileSizeMb, idxFile.readPos)
  }

  private def rotateWriteFile(): Unit = {
    idxFile.rotateWriteFile()
    writeFile.close()
    writeFile = new WriteDataFile(dir, idxFile.writeIdx, dataFileSizeMb, idxFile.writePos)
  }

  private val datFilter = new FilenameFilter {
    override def accept(dir: File, name: String): Boolean = DataFile.isDataFile(name)
  }

  /**
    * 清理已经读取完的文件
    */
  private def cleanReadDoneDataFile(): Unit = {
    dir.listFiles(datFilter).foreach { f =>
      val index = DataFile.getIndexByFileName(f.getName)
      if (index > 0 && index < idxFile.readIdx) f.delete()
    }
  }

  /**
    * 清理所有数据文件
    */
  private def cleanAllDataFile(): Unit = {
    dir.listFiles(datFilter).foreach(_.delete())
  }
}
