
import java.io._

import com.typesafe.config.{ConfigFactory, Config}

import org.scalatest._

import org.tuplejump.lmdb._
import DataTypes._

class DBTest extends FunSuite with Matchers {

  val conf = ConfigFactory.load()
  lazy val dbPath = conf.getString("dbPath")

  test("create table") {
    val db = new DB(dbPath)
    val columns = Array("sensorId", "temperature", "timestamp", "description")
    val dataTypes = Array(INT, INT, INT, TEXT)
    db.create("sensor_data", columns, dataTypes, partitionKey = "sensorId", clusteringKey = "timestamp")

    assert(new File(dbPath + "/" + "sensor_data").exists)
  }

  test("write data") {
    val columns = Array("sensorId", "temperature", "timestamp", "description")
    val data1 = Array(1, 23, 123, "out temperature")

    val data2 = Array(1, 25, 125, "out temperature")
    val data3 = Array(2, 23, 124, "inside temperature")
    val data4 = Array(3, 24, 122, "out temperature2")
    val tablePath = dbPath + "/" + "sensor_data"
    val db = new DB(dbPath)
    db.insert("sensor_data", columns, data1)
    db.insert("sensor_data", columns, data2)
    db.insert("sensor_data", columns, data3)
    db.insert("sensor_data", columns, data4)

    assert(new File(tablePath + "/" + "Some(1)").exists)
    assert(new File(tablePath + "/" + "Some(2)").exists)
    assert(new File(tablePath + "/" + "Some(3)").exists)
    assert(!new File(tablePath + "/" + "Some(4)").exists)
  }

  test("read data") {
    val db = new DB(dbPath)

    val readData1 = db.getRecord("sensor_data", 1, Some(125))
    assert(readData1.get("temperature").contains(25))
    assert(readData1.get("description").contains("out temperature"))

    val readData2 = db.getRecord("sensor_data", 2, Some(124))
    assert(readData2.get("temperature").contains(23))
    assert(readData2.get("description").contains("inside temperature"))

    val readData3 = db.getRecord("sensor_data", 1, Some(123))
    assert(readData3.get("temperature").contains(23))
    assert(readData3.get("description").contains("out temperature"))

    val readData4 = db.getRecord("sensor_data", 3, Some(122))
    assert(readData4.get("temperature").contains(24))
    assert(readData4.get("description").contains("out temperature2"))
  }

  test("selected columns") {
    val db = new DB(dbPath)
    val columns = List("temperature")

    val readData1 = db.getRecordByColumns("sensor_data", 1, Some(125), columns)

    readData1.size should be(1)
    readData1.contains("temperature") should be(true)
    readData1.get("temperature") should be(Some(25))

    val dataByPartition = db.getPartitionRecordsByColumns("sensor_data", 1, columns)
    dataByPartition.foreach(x => x.size should be(1))
  }

  test("multi record fetch") {
    val db = new DB(dbPath)

    val readData = db.allRecordsByPartition("sensor_data", 1)
    assert(readData(0).get("temperature").contains(23))
    assert(readData(1).get("temperature").contains(25))

    val readData2 = db.allRecordsByPartition("sensor_data", 2)
    assert(readData2(0).get("temperature").contains(23))
    assert(readData2(0).get("description").contains("inside temperature"))
  }

  test("total record count") {
    val db = new DB(dbPath)
    val recordCount = db.allRecords("sensor_data").length
    assert(4 == recordCount)
  }

  test("fetch all records") {
    val db = new DB(dbPath)
    val records = db.allRecords("sensor_data")
    records.foreach(x => println(x))
  }

  test("update test") {
    val tableName = "sensor_data"
    val db = new DB(dbPath)

    val newData = Map("temperature" -> 27, "description" -> "out temperature3")
    db.update(tableName, newData, partitionKeyData = 1, clusteringKeyData = 123)
    assert(db.getRecord(tableName, 1, 123) == newData)

    val oldData = Map("temperature" -> 23, "description" -> "out temperature")
    db.update(tableName, oldData, partitionKeyData = 1, clusteringKeyData = 123)
    assert(db.getRecord(tableName, 1, 123) == oldData)
  }

  test("delete test") {
    val db = new DB(dbPath)
    db.delete("sensor_data", 1, 123)
    assert(db.allRecords("sensor_data").length == 3)
    val columns = Array("sensorId", "temperature", "timestamp", "description")
    val data = Array(1, 23, 123, "out temperature")

    db.insert("sensor_data", columns, data)
    assert(db.allRecords("sensor_data").length == 4)
  }


}
