package com.bluelabs.akkaaws.impl


import akka.util.ByteString

private[akkaaws] object Utils {
  def encodeHex(bytes: Array[Byte]): String = {
    scodec.bits.BitVector(bytes).toHex
  }

  def encodeHex(bytes: ByteString): String = {
    encodeHex(bytes.toArray)
  }
}
