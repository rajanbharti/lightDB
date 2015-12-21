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
    lmdb.write("a", "x")
    lmdb.write("b", "y")
    lmdb.write("c", "z")

    lmdb.read("a") should be("x")
    lmdb.read("b") should be("y")
    lmdb.read("c") should be("z")
  }

  test("write and read bytes") {
    val testValue: String = "world"
    val value: Array[Byte] = testValue.getBytes
    lmdb.byteWrite("hello", value)
    val readValue: String = new String(lmdb.byteRead("hello"), "UTF-8")
    readValue should be(testValue)
  }

  test("key count") {
    lmdb.keyCount should be(4)
  }

  test("delete data") {
    lmdb.delete("a")
    lmdb.keyCount should be(3)
    lmdb.write("a", "x")
  }

}
