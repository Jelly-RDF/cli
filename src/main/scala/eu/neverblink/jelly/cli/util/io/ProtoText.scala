package eu.neverblink.jelly.cli.util.io

import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.{DynamicMessage, TextFormat}

/** Protobuf text format for Jelly messages, done through DynamicMessage and the message descriptor,
  * so we don't need the Google-style proto classes.
  */
object ProtoText:
  private val printer = TextFormat.printer().escapingNonAscii(false)

  def print(descriptor: Descriptor, bytes: Array[Byte]): String =
    printer.printToString(DynamicMessage.parseFrom(descriptor, bytes))

  def parse(descriptor: Descriptor, text: String): DynamicMessage =
    val builder = DynamicMessage.newBuilder(descriptor)
    TextFormat.merge(text, builder)
    builder.build()
