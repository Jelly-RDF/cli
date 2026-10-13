package eu.neverblink.jelly.cli.util.jena.riot

import eu.neverblink.jelly.cli.CriticalException
import eu.neverblink.jelly.convert.jena.JenaConverterFactory
import eu.neverblink.jelly.core.RdfEncoder
import eu.neverblink.jelly.core.proto.v1.{PhysicalStreamType, RdfStreamFrame, RdfStreamOptions}
import org.apache.jena.graph.Triple
import org.apache.jena.riot.system.StreamRDF
import org.apache.jena.sparql.core.Quad

import java.io.OutputStream

/** Writes a Jelly-RDF 1.2 stream of RDF Messages, ending a message when [[endMessage]] is called.
  *
  * @param options
  *   stream options, with the physical type set and the stream type MESSAGES
  * @param frameSize
  *   target number of statements per frame
  * @param delimited
  *   whether the output is delimited. If not, there can be only one frame.
  */
final class JellyMessageWriter(
    options: RdfStreamOptions,
    frameSize: Int,
    delimited: Boolean,
    out: OutputStream,
) extends StreamRDF:
  private var frameCount = 0
  private val encoder = JenaConverterFactory.getInstance().encoder(
    RdfEncoder.Params.of(
      options,
      if delimited then frameSize else RdfEncoder.MAX_FRAME_SIZE,
      writeFrame,
    ),
  )
  private val triples = options.getPhysicalType == PhysicalStreamType.TRIPLES

  private def writeFrame(frame: RdfStreamFrame): Unit =
    frameCount += 1
    if delimited then frame.writeDelimitedTo(out)
    else if frameCount > 1 then
      throw CriticalException(
        "The output does not fit in a single frame, so it cannot be written as non-delimited " +
          "output.",
      )
    else frame.writeTo(out)

  override def start(): Unit = ()

  override def triple(triple: Triple): Unit =
    encoder.handleTriple(triple.getSubject, triple.getPredicate, triple.getObject)

  override def quad(quad: Quad): Unit =
    if triples then throw CriticalException("Cannot write quads to a Jelly TRIPLES stream.")
    encoder.handleQuad(quad.getSubject, quad.getPredicate, quad.getObject, quad.getGraph)

  /** Ends the current message: the statements since the previous one make up one message. */
  def endMessage(): Unit = encoder.handleMessageEnd()

  override def base(base: String): Unit = ()

  override def version(version: String): Unit = ()

  override def prefix(prefix: String, iri: String): Unit = ()

  override def finish(): Unit =
    encoder.flush()
    out.flush()
