package tuplejump.lmdb

import java.io._
import java.nio.file.{Path, Files}

import DataTypes._
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.ArrayBuffer

class DB(dbPath: String) extends LazyLogging {

  val PARTITION_KEY = "pKey"
  val CLUSTERING_KEY = "cKey"

  def create(tableName: String, columns: Map[String, String], partitionKey: String, clusteringKey: String): Unit = {
    val tablePath = dbPath + "/" + tableName
    new File(tablePath).mkdir
    val lmdbManager = new LMDB(tablePath)
    columns.foreach(keyVal => lmdbManager.write(keyVal._1, keyVal._2.toString))
    lmdbManager.write(PARTITION_KEY, partitionKey)
    lmdbManager.write(CLUSTERING_KEY, clusteringKey)
  }

  def insert(tableName: String, data: Map[String, Any]) = {
    val tablePath = dbPath + "/" + tableName
    val lmdb = new LMDB(tablePath)
    val partitionKey = lmdb.read(PARTITION_KEY)
    val clusteringKey = lmdb.read(CLUSTERING_KEY)

    val pKeyPath = tablePath + "/" + data.get(partitionKey)
    if (new File(pKeyPath).exists()) {}
    else {
      new File(pKeyPath).mkdir()
    }

    val byteOutStream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val out: DataOutputStream = new DataOutputStream(byteOutStream)
    data.foreach(
      keyVal => {
        if (keyVal._1 != partitionKey && keyVal._1 != clusteringKey) {
          val columnNameBytes = keyVal._1.getBytes
          val colBytesLength = columnNameBytes.length
          out.writeInt(colBytesLength)
          out.write(columnNameBytes)
          keyVal._2 match {
            case _: Int => out.writeInt(keyVal._2.asInstanceOf[Int])
            case _: String => val value = keyVal._2.asInstanceOf[String]
              val valueBytes = value.getBytes
              val valBytesLength = valueBytes.length
              out.writeInt(valBytesLength)
              out.write(valueBytes)
          }
        }
      }
    )
    val dataBytes = byteOutStream.toByteArray
    lmdb.dbPath = pKeyPath
    logger.debug(pKeyPath)

    val clusteringKeyValue = data.get(clusteringKey).toString
    logger.debug(clusteringKeyValue + ":" + new String(dataBytes))
    lmdb.byteWrite(clusteringKeyValue, dataBytes)
  }

  //get a single record using parttion key and clustering key
  def getRecord(tableName: String, columns: List[String],
                partitionKeyData: Any, clusteringKeyData: Any): Map[String, Either[String, Int]] = {
    val tablePath = dbPath + "/" + tableName
    val lmdbManager = new LMDB(tablePath)
    val partitionKey = lmdbManager.read(PARTITION_KEY) // read partition key from table information

    lmdbManager.dbPath = tablePath + "/Some(" + partitionKeyData.toString + ")" //traverse to selected partition

    val dataBytes = lmdbManager.byteRead(clusteringKeyData.toString) //load data from clustering key
    getData(tablePath, dataBytes)
  }

  //get all records from a partition
  def allRecordsByPartition(tableName: String,
                            partitionKeyData: Any): Array[Map[String, Either[String, Int]]] = {
    val tablePath = dbPath + "/" + tableName
    val lmdbManager = new LMDB(tablePath)
    val partitionKey = lmdbManager.read(PARTITION_KEY) // read partition key from table information

    lmdbManager.dbPath = tablePath + "/Some(" + partitionKeyData.toString + ")" //traverse to selected partition
    val dataBytesArray = lmdbManager.readAllValues() //read all rows to list of byte array

    val records: Array[Map[String, Either[String, Int]]] =
      new Array[Map[String, Either[String, Int]]](dataBytesArray.size())
    for (a <- 0 until dataBytesArray.size()) {
      val rec = getData(tablePath, dataBytesArray.get(a))
      records(a) = rec
    }
    records
  }

  def allRecords(tableName: String): Array[Map[String, Either[String, Int]]] = {
    val tablePath = dbPath + "/" + tableName
    val partitions = partitionList(tablePath)
    val lmdb = new LMDB(tablePath)
    var recordCount: Int = 0
    //read total no. of records
    partitions.foreach(x => {
      lmdb.dbPath = tablePath + "/" + x
      recordCount = recordCount + lmdb.keyCount()

    })
    //creating array based of total no. of records
    val resultSet: Array[Map[String, Either[String, Int]]] =
      new Array[Map[String, Either[String, Int]]](recordCount)
    //fetching data for each partition and storing in array
    var index: Int = 0

    partitions.foreach(x => {
      lmdb.dbPath = tablePath + "/" + x
      val partitionRecords = lmdb.readAllValues() //read byteArray of records
      for (i <- 0 until partitionRecords.size()) {
        val record = getData(tablePath, partitionRecords.get(i)) //convert byteArray to data
        resultSet(index) = record
        index = index + 1
      }
    })
    resultSet
  }

  //create (column -> value) map from data bytes
  private def getData(tablePath: String, dataBytes: Array[Byte]): Map[String, Either[String, Int]] = {
    val lmdbManager: LMDB = new LMDB(tablePath)
    var data = scala.collection.mutable.Map[String, Either[String, Int]]()

    val in: DataInputStream = new DataInputStream(new ByteArrayInputStream(dataBytes))

    lmdbManager.dbPath = tablePath //move to table information
    try {
      while (in.available() > 0) {
        //read reading byte length of string of column name
        val columnSize = in.readInt()
        //read column name and convert to String

        val colByteValue: Array[Byte] = new Array[Byte](columnSize)

        in.read(colByteValue, 0, columnSize)
        val columnName = new String(colByteValue)
        //check corresponding data type of column and read desired data type
        if (lmdbManager.read(columnName) == DataTypes.INT.toString) {
          val columnData = in.readInt()
          data += (columnName -> Right(columnData))
        }
        else {
          val dataSize = in.readInt()
          val dataBytesValue: Array[Byte] = new Array[Byte](dataSize)
          in.read(dataBytesValue, 0, dataSize)
          val columnData = new String(dataBytesValue)
          data += (columnName -> Left(columnData))
        }
      }
    } catch {
      case e: EOFException => in.close()
    }
    data.toMap
  }

  def partitionList(tablePath: String): Array[String] = {
    val fileList = new File(tablePath).list()
    val partitions: Array[String] = new Array[String](fileList.length - 2)
    var j = 0
    for (i <- 0 until fileList.length) {
      if (fileList(i) != "lock.mdb" && fileList(i) != "data.mdb") {
        partitions(j) = fileList(i)
        j = j + 1
      }
    }
    partitions
  }
}