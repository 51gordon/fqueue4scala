package com.github.gordon.squeue

import java.io.{DataInputStream, File, FileInputStream}

trait TraitTest {
  def printIdxFileInfo(rootDir: File, prompt: String): Unit = {
    println("*" * 20 + prompt + "*" * 20)
    val idxFile = new File(rootDir, "sfq.idx")
    val fis = new FileInputStream(idxFile)
    val dis = new DataInputStream(fis)

    val magicBuff = new Array[Byte](8)
    dis.read(magicBuff)
    println(s"magic       : ${new String(magicBuff)}")

    println(s"version     : ${dis.readInt()}")

    println(s"read index  : ${dis.readInt()}")
    println(s"read pos    : ${dis.readInt()}")
    println(s"write index : ${dis.readInt()}")
    println(s"write pos   : ${dis.readInt()}")
    println(s"queue size  : ${dis.readInt()}")
    dis.close()
    fis.close()
  }

  def printDataFileInfo(rootDir: File, index: Int, prompt: String): Unit = {
    println("*" * 20 + prompt + "*" * 20)
    val idxFile = new File(rootDir, s"sfq_$index.dat")
    val fis = new FileInputStream(idxFile)
    val dis = new DataInputStream(fis)

    val magicBuff = new Array[Byte](8)
    dis.read(magicBuff)
    println(s"magic       : ${new String(magicBuff)}")

    println(s"version     : ${dis.readInt()}")

    println(s"file end pos: ${dis.readInt()}")
    dis.close()
    fis.close()
  }
}
