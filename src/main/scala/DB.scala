package org.tuplejump.lmdb

import java.io._
import java.nio.file.{Path, Files}


import com.typesafe.scalalogging.LazyLogging
import org.fusesource.lmdbjni.LMDBException

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
    if (!new File(pKeyPath).exists()) {
      new File(pKeyPath).mkdir()
    }
    val dataBytes: Array[Byte] = mapToBytes(data, partitionKey, clusteringKey)
    lmdb.dbPath = pKeyPath
    val clusteringKeyValue = data.get(clusteringKey).toString
    lmdb.byteWrite(clusteringKeyValue, dataBytes)
  }

  private def mapToBytes(data: Map[String, Any], partitionKey: String, clusteringKey: String): Array[Byte] = {
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
    dataBytes
  }

  def delete(tableName: String, partitionKeyData: Any, clusteringKeyData: Any) = {
    val tablePath = dbPath + "/" + tableName
    val pKeyPath = tablePath + "/Some(" + partitionKeyData.toString + ")"
    val lmdb = new LMDB(pKeyPath)
    lmdb.delete(clusteringKeyData.toString)
  }

  def update(tableName: String, data: Map[String, Any],
             partitionKeyData: Any, clusteringKeyData: Any) = {

    var newMap = getRecord(tableName, partitionKeyData, clusteringKeyData)

    data.foreach(x => {
      newMap = newMap + x
    })
    delete(tableName, partitionKeyData, clusteringKeyData)

    val dataBytes: Array[Byte] = mapToBytes(newMap)
    val partitionPath = dbPath + "/" + tableName + "/Some(" + partitionKeyData.toString + ")"
    val lmdb = new LMDB(partitionPath)

    lmdb.byteWrite(clusteringKeyData.toString, dataBytes)
  }

  private def mapToBytes(newMap: Map[String, Any]): Array[Byte] = {
    val byteOutStream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val out: DataOutputStream = new DataOutputStream(byteOutStream)
    newMap.foreach(keyVal => {
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
    })
    val dataBytes = byteOutStream.toByteArray
    dataBytes
  }

  //get a single record using partition key and clustering key
  def getRecord(tableName: String,
                partitionKeyData: Any, clusteringKeyData: Any): Map[String, Any] = {
    val tablePath = dbPath + "/" + tableName
    val lmdbManager = new LMDB(tablePath)
    val partitionKey = lmdbManager.read(PARTITION_KEY) // read partition key from table information

    lmdbManager.dbPath = tablePath + "/Some(" + partitionKeyData.toString + ")" //traverse to selected partition

    val dataBytes = lmdbManager.byteRead(clusteringKeyData.toString) //load data from clustering key
    getData(tablePath, dataBytes)
  }

  //get all records from a partition
  def allRecordsByPartition(tableName: String,
                            partitionKeyData: Any): Array[Map[String, Any]] = {
    val tablePath = dbPath + "/" + tableName

    val lmdb = new LMDB(tablePath)

    lmdb.dbPath = tablePath + "/Some(" + partitionKeyData.toString + ")" //traverse to selected partition
    val dataBytesArray = lmdb.readAllValues //read all rows to list of byte array

    val records: Array[Map[String, Any]] =
      new Array[Map[String, Any]](dataBytesArray.size())
    for (a <- 0 until dataBytesArray.size()) {
      val rec = getData(tablePath, dataBytesArray.get(a))
      records(a) = rec
    }
    records
  }

  def allRecords(tableName: String): Array[Map[String, Any]] = {
    val tablePath = dbPath + "/" + tableName
    val partitions = partitionList(tablePath)
    val lmdb = new LMDB(tablePath)
    var recordCount: Int = 0
    //read total no. of records
    partitions.foreach(x => {
      lmdb.dbPath = tablePath + "/" + x
      recordCount = recordCount + lmdb.keyCount
    })
    //creating array based of total no. of records
    val resultSet: Array[Map[String, Any]] =
      new Array[Map[String, Any]](recordCount)
    //fetching data for each partition and storing in array
    var index: Int = 0
    try {
      partitions.foreach(x => {
        lmdb.dbPath = tablePath + "/" + x
        val partitionRecords = lmdb.readAllValues //read byteArray of records
        for (i <- 0 until  partitionRecords.size()) {
          val record = getData(tablePath, partitionRecords.get(i)) //convert byteArray to data
          resultSet(index) = record
          index = index + 1
        }
      })
    }
    catch {
      case e: LMDBException => println("Invalid query/Data not present")
    }
    resultSet
  }

  //create (column -> value) map from data bytes
  private def getData(tablePath: String, dataBytes: Array[Byte]): Map[String, Any] = {
    val lmdb: LMDB = new LMDB(tablePath)
    var data = scala.collection.mutable.Map[String, Any]()

    val in: DataInputStream = new DataInputStream(new ByteArrayInputStream(dataBytes))

    lmdb.dbPath = tablePath //move to table information
    try {
      while (in.available() > 0) {
        //read reading byte length of string of column name
        val columnSize = in.readInt()
        //read column name and convert to String

        val colByteValue: Array[Byte] = new Array[Byte](columnSize)

        in.read(colByteValue, 0, columnSize)
        val columnName = new String(colByteValue)
        //check corresponding data type of column and read desired data type
        if (lmdb.read(columnName) == DataTypes.INT.toString) {
          val columnData = in.readInt()
          data += (columnName -> columnData)
        }
        else {
          val dataSize = in.readInt()
          val dataBytesValue: Array[Byte] = new Array[Byte](dataSize)
          in.read(dataBytesValue, 0, dataSize)
          val columnData = new String(dataBytesValue)
          data += (columnName -> columnData)
        }
      }
    } catch {
      case e: EOFException => in.close()
    }
    data.toMap
  }

  private def partitionList(tablePath: String): Array[String] = {
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