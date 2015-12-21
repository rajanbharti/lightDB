package org.tuplejump.lmdb

object DataTypes extends Enumeration {
  type DataTypes = Value
  val INT, TEXT = Value

  override def toString() =
    this match {
      case INT => "INT"
      case TEXT => "TEXT"
    }
}
