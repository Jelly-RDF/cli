package eu.neverblink.jelly.cli.util.jena.riot

import eu.neverblink.jelly.cli.CriticalException
import eu.neverblink.jelly.convert.jena.JenaConverterFactory
import eu.neverblink.jelly.core.ProtoEncoder
import eu.neverblink.jelly.core.memory.RowBuffer
import eu.neverblink.jelly.core.proto.v1.{PhysicalStreamType, RdfStreamFrame, RdfStreamOptions}
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.riot.system.StreamRDF
import org.apache.jena.sparql.core.Quad

import java.io.OutputStream

/** Writes Jelly-RDF, ending a frame only when [[endFrame]] is called. This is for grouped streams,
  * where each frame is one graph or dataset.
  *
  * Namespace declarations are not written.
  *
  * @param options
  *   stream options, with the physical type set
  * @param delimited
  *   whether the output is delimited. If not, there can be only one frame.
  */
final class JellyFrameWriter(options: RdfStreamOptions, delimited: Boolean, out: OutputStream)
    extends StreamRDF:
  private val buffer = RowBuffer.newLazyImmutable()
  private val encoder = JenaConverterFactory.getInstance().encoder(
    ProtoEncoder.Params.of(options, false, buffer),
  )
  private val physicalType = options.getPhysicalType
  // Only used in GRAPHS streams
  private var currentGraph: Node = null
  private var frameCount = 0

  override def start(): Unit = ()

  override def triple(triple: Triple): Unit = physicalType match
    case PhysicalStreamType.TRIPLES =>
      encoder.handleTriple(triple.getSubject, triple.getPredicate, triple.getObject)
    case PhysicalStreamType.GRAPHS =>
      handleGraph(Quad.defaultGraphIRI)
      encoder.handleTriple(triple.getSubject, triple.getPredicate, triple.getObject)
    case _ =>
      encoder.handleQuad(triple.getSubject, triple.getPredicate, triple.getObject, null)

  override def quad(quad: Quad): Unit = physicalType match
    case PhysicalStreamType.TRIPLES =>
      throw CriticalException("Cannot write quads to a Jelly TRIPLES stream.")
    case PhysicalStreamType.GRAPHS =>
      handleGraph(quad.getGraph)
      encoder.handleTriple(quad.getSubject, quad.getPredicate, quad.getObject)
    case _ =>
      encoder.handleQuad(quad.getSubject, quad.getPredicate, quad.getObject, quad.getGraph)

  private def handleGraph(graph: Node): Unit =
    if currentGraph == null || currentGraph != graph &&
      !(Quad.isDefaultGraph(currentGraph) && Quad.isDefaultGraph(graph))
    then
      if currentGraph != null then encoder.handleGraphEnd()
      encoder.handleGraphStart(graph)
      currentGraph = graph

  /** Writes out everything since the previous frame as one frame, even if that's nothing. */
  def endFrame(): Unit =
    if currentGraph != null then
      encoder.handleGraphEnd()
      currentGraph = null
    frameCount += 1
    if frameCount == 1 && buffer.isEmpty then buffer.appendMessage().setOptions(encoder.getOptions)
    if !delimited && frameCount > 1 then
      throw CriticalException(
        "The output has more than one frame, so it cannot be written as non-delimited output.",
      )
    val frame = RdfStreamFrame.newInstance()
    buffer.forEach(row => frame.addRows(row))
    if delimited then frame.writeDelimitedTo(out) else frame.writeTo(out)
    buffer.clear()

  override def base(base: String): Unit = ()

  override def version(version: String): Unit = ()

  override def prefix(prefix: String, iri: String): Unit = ()

  override def finish(): Unit = out.flush()
