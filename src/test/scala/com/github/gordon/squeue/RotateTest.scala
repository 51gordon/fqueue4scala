package com.github.gordon.squeue

import java.io.File
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class RotateTest extends FunSuite with BeforeAndAfterEach with TraitTest {

  var queue: FQueue = _
  val rootDir = new File(sys.props("user.dir"), "test-data-3")

  override def beforeEach() {
    FileUtils.deleteQuietly(rootDir)
    queue = new FQueue(rootDir, dataFileSizeMb = 1)
  }

  override def afterEach() {
    queue.close()
  }

  test("test offer x times and pool x times") {
    val times = 500000
    val uuid = UUID.randomUUID().toString
    for (i <- Range(0, times)) {
      queue.offer(s"$uuid-$i".getBytes())
      assert(queue.size() == i + 1)
    }
    assert(queue.size() == times)
    printIdxFileInfo(rootDir, "idx")
    printDataFileInfo(rootDir,1,"1.dat")

    for (i <- Range(0, times)) {
      println(s"i: $i")
      if (i == 23036) {
        println(s"i: $i")
      }
      val value = new String(queue.poll())
      assert(s"$uuid-$i" == value)
      assert(queue.size() == times - i - 1)
    }
    assert(queue.size() == 0)
  }


}
