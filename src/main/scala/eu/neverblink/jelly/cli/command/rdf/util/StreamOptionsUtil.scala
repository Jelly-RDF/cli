package eu.neverblink.jelly.cli.command.rdf.util

import eu.neverblink.jelly.core.proto.google.v1 as google
import eu.neverblink.jelly.core.proto.v1.RdfStreamOptions

object StreamOptionsUtil:
  
  def prettyPrint(streamOptions: RdfStreamOptions): String = {
    val streamOptionsBytes = streamOptions.toByteArray
    val googleStreamOptions = google.RdfStreamOptions.parseFrom(streamOptionsBytes)
    return googleStreamOptions.toString
  }
