import org.tuplejump.lmdb._
import java.io.File

import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers, FlatSpec}

class LMDBTest extends FunSuite with Matchers {
  val conf = ConfigFactory.load()
  val dbPath = conf.getString("dbPath") + "/lmdbTest"
  new File(dbPath).mkdir()
  val lmdb = new LMDB(dbPath)

  test("write and read string") {
    lmdb.write("a1", "x")
    lmdb.write("b1", "y")
    lmdb.write("c2", "z")

    lmdb.read("a1") should be("x")
    lmdb.read("b1") should be("y")
    lmdb.read("c2") should be("z")
  }

  test("write and read bytes") {
    val testValue: String = "world"
    val value: Array[Byte] = testValue.getBytes
    lmdb.byteWrite("hello", value)
    val readValue: String = new String(lmdb.byteRead("hello"))
    readValue should be(testValue)
  }

  test("key count") {
    lmdb.keyCount should be(4)
  }

  test("delete data") {
    lmdb.delete("a1")
    lmdb.keyCount should be(3)
    lmdb.write("a1", "x")
  }

  test("all key-values") {
    //val allData = lmdb.readAllData
    val allKV = lmdb.allKeyValues
    val byteData = allKV.get("hello")
    val strData = new String(byteData.get)
    strData should be("world")
    //  allKV.foreach(x => println(x._1 + "->" + new String(x._2)))
    //  println("Keys containing 1")
    //  allKV.foreach(x => if (x._1.contains("1")) println(x._1 + "->" + new String(x._2)))
  }

}
