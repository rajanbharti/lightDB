
import java.io._

import com.typesafe.config.{ConfigFactory, Config}

import org.scalatest._
import org.scalatest.BeforeAndAfter
import tuplejump.lmdb._
import DataTypes._

class DBTest extends FunSuite with BeforeAndAfter {

  val conf = ConfigFactory.load()
  lazy val dbPath = conf.getString("dbPath")

  test("create table") {
    val db = new DB(dbPath)
    val columns = Map("sensorId" -> INT.toString, "temperature" -> INT.toString,
      "timestamp" -> INT.toString, "description" -> TEXT.toString)
    db.create("sensor_data", columns, "sensorId", "timestamp")

    assert(new File(dbPath + "/" + "sensor_data").exists)
  }

  test("write data") {
    val data = Map("sensorId" -> 1, "temperature" -> 23,
      "timestamp" -> 123, "description" -> "out temperature")
     val data2 = Map("sensorId" -> 1, "temperature" -> 23,
       "timestamp" -> 125, "description" -> "out temperature")
     val data3 = Map("sensorId" -> 2, "temperature" -> 23,
       "timestamp" -> 124, "description" -> "inside temperature")
     val data4 = Map("sensorId" -> 3, "temperature" -> 24,
       "timestamp" -> 122, "description" -> "out temperature2")
    val tablePath = dbPath + "/" + "sensor_data"
    val db = new DB(dbPath)
    db.insert("sensor_data", data)
    db.insert("sensor_data", data2)
    db.insert("sensor_data", data3)
    db.insert("sensor_data", data4)

    assert(new File(tablePath + "/" + "Some(1)").exists)
    assert(new File(tablePath + "/" + "Some(2)").exists)
    assert(new File(tablePath + "/" + "Some(3)").exists)
    assert(!new File(tablePath + "/" + "Some(4)").exists)
  }

  test("read data") {
    val db = new DB(dbPath)
    val columns = List("temperature", "description")
    val readData = db.getData("sensor_data", columns, 1, 123)
    assert(readData.get("temperature") == Some(Right(23)))
    assert(readData.get("description")==Some(Left("out temperature")))

  }


}



