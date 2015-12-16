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
    val pARTITION_KEY = lmdb.read(PARTITION_KEY)
    val cLUSTERING_KEY = lmdb.read(CLUSTERING_KEY)

    val pKeyPath = tablePath + "/" + data.get(pARTITION_KEY)
    if (new File(pKeyPath).exists()) {}
    else {
      new File(pKeyPath).mkdir()
    }

    val byteOutStream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val out: DataOutputStream = new DataOutputStream(byteOutStream)
    data.foreach(
      keyVal => {
        if (keyVal._1 != pARTITION_KEY && keyVal._1 != cLUSTERING_KEY) {
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

    val clusteringKeyValue = data.get(cLUSTERING_KEY).toString
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
  def getAllRecords(tableName: String, columns: List[String],
                    partitionKeyData: Any): Array[Map[String, Either[String, Int]]] = {
    val tablePath = dbPath + "/" + tableName
    val lmdbManager = new LMDB(tablePath)
    val partitionKey = lmdbManager.read(PARTITION_KEY) // read partition key from table information

    lmdbManager.dbPath = tablePath + "/Some(" + partitionKeyData.toString + ")" //traverse to selected partition
    val dataBytesArray = lmdbManager.readAllValues() //read all rows to list of byte array

    val record: Array[Map[String, Either[String, Int]]] =
      new Array[Map[String, Either[String, Int]]](dataBytesArray.size())
    for (a <- 0 until dataBytesArray.size()) {
      val rec = getData(tablePath, dataBytesArray.get(a))
      record(a) = rec
    }
    record
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
}