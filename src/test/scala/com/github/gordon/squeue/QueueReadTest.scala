package com.github.gordon.squeue

import java.io._

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class QueueReadTest extends FunSuite with BeforeAndAfterEach with TraitTest {

  var queue: FQueue = _
  val rootDir = new File(sys.props("user.dir"), "test_data-2")

  override def beforeEach() {
    FileUtils.deleteQuietly(rootDir)
    queue = new FQueue(rootDir, dataFileSizeMb = 1)
  }

  override def afterEach() {
  }

  test("test print queue file info") {
    printIdxFileInfo(rootDir, "before offer")
    queue.offer("12345678".getBytes())

    printIdxFileInfo(rootDir, "after offer(before poll)")
    queue.poll()

    printIdxFileInfo(rootDir, "after poll(before close)")
    queue.close()

    printIdxFileInfo(rootDir, "after close")
  }

}
