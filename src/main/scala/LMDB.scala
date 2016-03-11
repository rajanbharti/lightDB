package org.tuplejump.lmdb

import java.io._
import java.util

import com.typesafe.scalalogging.LazyLogging
import org.fusesource.lmdbjni.Constants._
import org.fusesource.lmdbjni._

import scala.collection.mutable

object LMDB {
  @throws(classOf[IOException])
  private def serialize(`object`: AnyRef): Array[Byte] = {
    val out: ByteArrayOutputStream = new ByteArrayOutputStream
    val os: ObjectOutputStream = new ObjectOutputStream(out)
    os.writeObject(`object`)
    out.toByteArray
  }

  @throws(classOf[IOException])
  @throws(classOf[ClassNotFoundException])
  private def deserialize(data: Array[Byte]): AnyRef = {
    val in: ByteArrayInputStream = new ByteArrayInputStream(data)
    val is: ObjectInputStream = new ObjectInputStream(in)
    is.readObject
  }
}

class LMDB extends LazyLogging {
  private[lmdb] var dbPath: String = null

  //  val env: Env = new Env(dbPath)
  // val db: Database = env.openDatabase

  def this(dbPath: String) {
    this()
    this.dbPath = dbPath
  }

  def write(key: String, value: String) {
    val env: Env = new Env(dbPath)
    val db: Database = env.openDatabase
    val tx: Transaction = env.createWriteTransaction
    val cursor: BufferCursor = db.bufferCursor(tx)
    cursor.first
    cursor.keyWriteUtf8(key)
    cursor.valWriteUtf8(value)
    cursor.overwrite
    cursor.close
    tx.commit
    db.close
    env.close
  }

  def write(key: Int, value: String) {
    val env: Env = new Env(dbPath)
    val db: Database = env.openDatabase
    val tx: Transaction = env.createWriteTransaction
    val cursor: BufferCursor = db.bufferCursor(tx)
    cursor.first
    cursor.keyWriteInt(key)
    cursor.valWriteUtf8(value)
    cursor.overwrite
    cursor.close()
    tx.commit()
    db.close()
  }

  def read(key: String): String = {

    val env: Env = new Env(dbPath)
    val db: Database = env.openDatabase
    val tx: Transaction = env.createReadTransaction
    val cursor: BufferCursor = db.bufferCursor(tx)
    cursor.first
    cursor.seek(bytes(key))
    cursor.keyUtf8(0)
    val value = cursor.valUtf8(0).getString.toString
    db.close
    value
  }

  def byteWrite(key: String, value: Array[Byte]) {
    val env: Env = new Env(dbPath)
    val db: Database = env.openDatabase

    val tx: Transaction = env.createWriteTransaction
    val cursor: BufferCursor = db.bufferCursor(tx)
    cursor.first
    cursor.keyWriteUtf8(key)
    cursor.valWriteBytes(value)
    cursor.overwrite
    cursor.close
    tx.commit
    db.close
    env.close
  }

  def byteRead(key: String): Array[Byte] = {
    var value: Array[Byte] = null
    val env: Env = new Env(dbPath)
    val db: Database = env.openDatabase
    val tx: Transaction = env.createReadTransaction
    val cursor: BufferCursor = db.bufferCursor(tx)
    cursor.first
    cursor.seek(bytes(key))
    cursor.keyUtf8(0)
    value = cursor.valBytes
    cursor.close
    db.close
    env.close
    value
  }

  @throws(classOf[IOException])
  def writeObject(key: String, `object`: AnyRef) {
    val objectBytes: Array[Byte] = LMDB.serialize(`object`)
    byteWrite(key, objectBytes)
  }

  @throws(classOf[IOException])
  @throws(classOf[ClassNotFoundException])
  def readObject(key: String): AnyRef = {
    val objectBytes: Array[Byte] = byteRead(key)
    LMDB.deserialize(objectBytes)
  }


  def keyCount: Int = {
    var count: Int = 0
    val env: Env = new Env(dbPath)
    val db: Database = env.openDatabase
    val tx: Transaction = env.createReadTransaction()
    val cursor: BufferCursor = db.bufferCursor(tx)
    cursor.first
    while (cursor.next) {
      cursor.keyByte(0)
      cursor.valByte(0)
      count += 1
    }
    cursor.close
    db.close
    // env.close
    return count + 1
  }

  def readNValues(valuesCount: Int): util.ArrayList[Array[Byte]] = {
    val env: Env = new Env(dbPath)
    val db: Database = env.openDatabase
    val tx: Transaction = env.createReadTransaction()
    val cursor: BufferCursor = db.bufferCursor(tx)
    val records: util.ArrayList[Array[Byte]] = new util.ArrayList[Array[Byte]](valuesCount)
    cursor.first
    var i: Int = 0
    while (cursor.next && i < valuesCount) {
      cursor.keyByte(0)
      cursor.keyUtf8(0)
      var record: Array[Byte] = null
      record = cursor.valBytes
      records.add(record)
      i += 1
    }
    cursor.close
    tx.close()
    return records
  }

  def readAllValues: util.ArrayList[Array[Byte]] = {
    val env: Env = new Env(dbPath)
    val db: Database = env.openDatabase
    val tx: Transaction = env.createReadTransaction()
    val cursor: BufferCursor = db.bufferCursor(tx)
    val records: util.ArrayList[Array[Byte]] = new util.ArrayList[Array[Byte]](keyCount + 1)
    cursor.first
    var i: Int = 0
    cursor.keyUtf8(0)
    var record: Array[Byte] = null
    record = cursor.valBytes
    records.add(record)
    while (cursor.next && i <= keyCount) {
      cursor.keyUtf8(0)
      record = cursor.valBytes
      records.add(record)
      i += 1
    }
    cursor.close
    // db.close
    //   env.close
    records
  }

  def allKeyValues: Map[String, Array[Byte]] = {
    val env: Env = new Env(dbPath)
    val db: Database = env.openDatabase
    val tx: Transaction = env.createReadTransaction()
    val cursor: BufferCursor = db.bufferCursor(tx)
    var data = scala.collection.mutable.Map[String, Array[Byte]]()
    cursor.first
    var i: Int = 0
    cursor.keyUtf8(0)
    data += (cursor.keyUtf8(0).getString -> cursor.valBytes())
    while (cursor.next && i < keyCount - 1) {
      cursor.keyUtf8(0)
      data += (cursor.keyUtf8(0).getString -> cursor.valBytes())
      i += 1
    }
    data.toMap
  }

  def delete(key: String) {
    val env: Env = new Env(dbPath)
    val db: Database = env.openDatabase
    val tx: Transaction = env.createWriteTransaction
    val cursor: BufferCursor = db.bufferCursor(tx)
    cursor.seek(bytes(key))
    cursor.delete
    tx.commit
    cursor.close

  }
}
