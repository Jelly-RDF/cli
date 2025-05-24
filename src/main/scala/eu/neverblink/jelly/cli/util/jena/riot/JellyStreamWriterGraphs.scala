package eu.neverblink.jelly.cli.util.jena.riot

import eu.neverblink.jelly.convert.jena.JenaConverterFactory
import eu.neverblink.jelly.convert.jena.riot.JellyFormatVariant
import eu.neverblink.jelly.core.ProtoEncoder
import eu.neverblink.jelly.core.memory.RowBuffer
import eu.neverblink.jelly.core.proto.v1.RdfStreamFrame
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.riot.system.StreamRDF
import org.apache.jena.sparql.core.Quad

import java.io.OutputStream

/** A stream writer that writes RDF data in Jelly format, using the GRAPHS physical stream type.
  *
  * JellyStreamWriter in jelly-jena only supports TRIPLES and QUADS physical stream types, so this
  * is needed to support the remaining GRAPHS physical stream type.
  */
final class JellyStreamWriterGraphs(opt: JellyFormatVariant, out: OutputStream) extends StreamRDF:
  private val buffer = RowBuffer.newLazyImmutable()
  // We don't set any options here – it is the responsibility of the caller to set
  // a valid stream type (GRAPHS).
  private val encoder = JenaConverterFactory.getInstance().encoder(
    ProtoEncoder.Params.of(
      opt.getOptions,
      opt.isEnableNamespaceDeclarations,
      buffer,
    ),
  )
  private var currentGraph: Node = null

  // No need to handle this, the encoder will emit the header automatically anyway
  override def start(): Unit = ()

  override def triple(triple: Triple): Unit =
    handleGraph(Quad.defaultGraphIRI)
    encoder.handleTriple(triple.getSubject, triple.getPredicate, triple.getObject)
    if opt.isDelimited && buffer.size >= opt.getFrameSize then flushBuffer()

  override def quad(quad: Quad): Unit =
    handleGraph(quad.getGraph)
    encoder.handleTriple(
      quad.getSubject,
      quad.getPredicate,
      quad.getObject,
    )
    if opt.isDelimited && buffer.size >= opt.getFrameSize then flushBuffer()

  // Not supported
  override def base(base: String): Unit = ()

  override def prefix(prefix: String, iri: String): Unit =
    if opt.isEnableNamespaceDeclarations then
      encoder.handleNamespace(prefix, NodeFactory.createURI(iri))
      if opt.isDelimited && buffer.size >= opt.getFrameSize then flushBuffer()

  private def handleGraph(graph: Node): Unit =
    if currentGraph == null then
      // First graph in the stream
      encoder.handleGraphStart(graph)
      currentGraph = graph
    else if Quad.isDefaultGraph(currentGraph) then
      if !Quad.isDefaultGraph(graph) then
        // We are switching default -> named
        encoder.handleGraphEnd()
        encoder.handleGraphStart(graph)
        currentGraph = graph
    else if Quad.isDefaultGraph(graph) || graph != currentGraph then
      // We are switching named -> named or named -> default
      encoder.handleGraphEnd()
      encoder.handleGraphStart(graph)
      currentGraph = graph

  // Flush the buffer and finish the stream
  override def finish(): Unit =
    if currentGraph != null then
      encoder.handleGraphEnd()
      currentGraph = null
    if !opt.isDelimited then
      // Non-delimited variant – whole stream in one frame
      val frame = RdfStreamFrame.newInstance()
      buffer.forEach { row =>
        frame.addRows(row)
      }
      frame.writeTo(out)
    else if !buffer.isEmpty then flushBuffer()
    out.flush()

  private def flushBuffer(): Unit =
    val frame = RdfStreamFrame.newInstance()
    buffer.forEach { row =>
      frame.addRows(row)
    }
    frame.writeDelimitedTo(out)
    buffer.clear()
