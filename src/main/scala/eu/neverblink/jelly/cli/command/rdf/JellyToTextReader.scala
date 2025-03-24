package eu.neverblink.jelly.cli.command.rdf

import eu.ostrzyciel.jelly.core.IoUtils
import eu.ostrzyciel.jelly.core.proto.v1.RdfStreamFrame
import org.apache.jena.atlas.web.ContentType

import org.apache.jena.riot.system.StreamRDF
import org.apache.jena.sparql.util.Context

import java.io.InputStream

/** Reads Jelly RDF data from an InputStream. Automatically detects whether the input is a single
  * frame (non-delimited) or a stream of frames (delimited).
  */
def readJellyBinary(
    in: InputStream,
    baseURI: String,
    ct: ContentType,
    output: StreamRDF,
    context: Context,
): Unit =
  output.start()
  try {
    IoUtils.autodetectDelimiting(in) match
      case (false, newIn) =>
        // Non-delimited Jelly file
        // In this case, we can only read one frame
        val frame = RdfStreamFrame.parseFrom(newIn)
        println("processFrame(frame)")
      case (true, newIn) =>
        // Delimited Jelly file
        // In this case, we can read multiple frames
        Iterator.continually(RdfStreamFrame.parseDelimitedFrom(newIn))
          .takeWhile(_.isDefined)
          .foreach { maybeFrame => println("processFrame(maybeFrame.get)") }
  } finally {
    output.finish()
  }
