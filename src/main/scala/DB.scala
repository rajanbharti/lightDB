package tuplejump.lmdb

import java.io._
import java.nio.file.{Path, Files}

import DataTypes._


class DB(dbPath: String) {

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
    val lmdbManager = new LMDB(tablePath)
    val partitionKey = lmdbManager.read(PARTITION_KEY)
    val clusteringKey = lmdbManager.read(CLUSTERING_KEY)

    val byteOutStream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val out: DataOutputStream = new DataOutputStream(byteOutStream)
    val pKeyPath = tablePath + "/" + data.get(partitionKey)

    data.foreach(keyVal => {
      if (keyVal._1 != partitionKey || keyVal._1 != clusteringKey) {
        val column = keyVal._1.getBytes
        val columnLength = column.length
        out.write(columnLength)
        out.write(column)
        keyVal._2 match {
          case _: Int =>
            out.write(keyVal._2.asInstanceOf[Int])
          case _: String =>
            val dataBytes = keyVal._2.asInstanceOf[String].getBytes
            val dataLength = dataBytes.length
            out.write(dataLength)
            out.write(dataBytes)
        }
      }
    })
    val byteRecord: Array[Byte] = byteOutStream.toByteArray

    if (new File(pKeyPath).exists()) {}
    else {
      new File(pKeyPath).mkdir
    }
    out.close()
    lmdbManager.dbPath = pKeyPath
    lmdbManager.byteWrite(data.get(clusteringKey).toString, byteRecord)
  }


  def getData(tableName: String, columns: List[String],
              partitionKeyData: Any, clusteringKeyData: Any): Map[String, Either[String, Int]] = {
    val tablePath = dbPath + "/" + tableName
    val lmdbManager = new LMDB(tablePath)
    val partitionKey = lmdbManager.read(PARTITION_KEY)

    lmdbManager.dbPath = tablePath + "/Some(" + partitionKeyData.toString + ")"
    val dataBytes = lmdbManager.byteRead(clusteringKeyData.toString)
    var data = scala.collection.mutable.Map[String, Either[String, Int]]()
    val byteInStream: ByteArrayInputStream = new ByteArrayInputStream(dataBytes)
    val in: DataInputStream = new DataInputStream(byteInStream)
    lmdbManager.dbPath = tablePath
    try {
      while (in.available() != 0) {
        val columnSize = in.readInt()
        var colByteValue: Array[Byte] = new Array[Byte](columnSize)
        in.read(colByteValue, 0, columnSize)
        val columnName = new String(colByteValue)
        if (lmdbManager.read(columnName) == DataTypes.INT.toString) {
          val columnData = in.readInt()
          data += (columnName -> Right(columnData))
        }
        else {
          val dataSize = in.readInt()
          var dataBytesValue: Array[Byte] = new Array[Byte](dataSize)
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



