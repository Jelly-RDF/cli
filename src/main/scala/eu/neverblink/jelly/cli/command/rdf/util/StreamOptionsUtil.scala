package eu.neverblink.jelly.cli.command.rdf.util

import eu.neverblink.jelly.cli.util.io.ProtoText
import eu.neverblink.jelly.core.proto.v1.RdfStreamOptions

object StreamOptionsUtil:

  def prettyPrint(streamOptions: RdfStreamOptions): String =
    ProtoText.print(RdfStreamOptions.getDescriptor, streamOptions.toByteArray)
