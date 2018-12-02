package com.github.gordon.squeue

import java.io.File
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class FQueueTest extends FunSuite with BeforeAndAfterEach with TraitTest {

  var queue: FQueue = _
  val rootDir = new File(sys.props("user.dir"), "test-data-1")

  override def beforeEach() {
    FileUtils.deleteQuietly(rootDir)
    queue = new FQueue(rootDir, dataFileSizeMb = 1)
  }

  override def afterEach() {
    queue.close()
  }

  test("testOfferOnce") {
    queue.offer("abc".getBytes())
  }

  test("testPoll") {
    val value = "abc"
    queue.offer(value.getBytes())
    assert(new String(queue.poll()) == value)
  }

  test("testClear") {
    val value = "abc"
    queue.offer(value.getBytes())
    queue.clear()
    assert(queue.size() == 0)
  }

  test("testPeek") {
    val value = "abc"
    queue.offer(value.getBytes())
    assert(new String(queue.peek()) == value)
  }

  test("testSize") {
    assert(queue.size() == 0)
    val value = "abc"
    queue.offer(value.getBytes())
    assert(queue.size() == 1)
    queue.poll()
    assert(queue.size() == 0)
  }

  test("test offer 10 times and pool 10 times") {
    for (i <- Range(0, 10)) {
      queue.offer(s"abc-$i".getBytes())
      assert(queue.size() == i + 1)
    }
    assert(queue.size() == 10)
    for (i <- Range(0, 10)) {
      val value = new String(queue.poll())
      assert(value == s"abc-$i")
      assert(queue.size() == 10 - i - 1)
    }
    assert(queue.size() == 0)
  }

  test("test reopen") {
    val max = 100000
    val uuid = UUID.randomUUID().toString
    for (i <- Range(0, max)) {
      queue.offer(s"$uuid-$i".getBytes())
      assert(queue.size() == i + 1)
    }
    val a1 = new String(queue.poll())
    println(s"a1: $a1")
    queue.close()
    printIdxFileInfo(rootDir, "after close")
    queue = new FQueue(rootDir, dataFileSizeMb = 1)
    printIdxFileInfo(rootDir, "new FQueue")
    assert(queue.size() == max - 1)
    for (i <- Range(1, max)) {
      printIdxFileInfo(rootDir, "before poll")
      val value = new String(queue.poll())
      printIdxFileInfo(rootDir, "after poll")
      println("value: " + value)
      assert(value == s"$uuid-$i")
      assert(queue.size() == max - i - 1)
    }
    assert(queue.size() == 0)
  }
}
