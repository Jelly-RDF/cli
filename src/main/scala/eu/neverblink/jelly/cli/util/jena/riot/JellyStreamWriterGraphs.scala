package eu.neverblink.jelly.cli.util.jena.riot

import eu.ostrzyciel.jelly.convert.jena.JenaConverterFactory
import eu.ostrzyciel.jelly.convert.jena.riot.JellyFormatVariant
import eu.ostrzyciel.jelly.core.ProtoEncoder
import eu.ostrzyciel.jelly.core.proto.v1.{RdfStreamFrame, RdfStreamRow}
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.riot.system.StreamRDF
import org.apache.jena.sparql.core.Quad

import java.io.OutputStream
import scala.collection.mutable.ListBuffer

/** A stream writer that writes RDF data in Jelly format, using the GRAPHS physical stream type.
  *
  * JellyStreamWriter in jelly-jena only supports TRIPLES and QUADS physical stream types, so this
  * is needed to support the remaining GRAPHS physical stream type.
  */
final class JellyStreamWriterGraphs(opt: JellyFormatVariant, out: OutputStream) extends StreamRDF:
  private val buffer: ListBuffer[RdfStreamRow] = new ListBuffer[RdfStreamRow]()
  // We don't set any options here – it is the responsibility of the caller to set
  // a valid stream type (GRAPHS).
  private val encoder = JenaConverterFactory.encoder(
    ProtoEncoder.Params(
      opt.opt,
      opt.enableNamespaceDeclarations,
      Some(buffer),
    ),
  )
  private var currentGraph: Node = null

  // No need to handle this, the encoder will emit the header automatically anyway
  override def start(): Unit = ()

  override def triple(triple: Triple): Unit =
    handleGraph(Quad.defaultGraphIRI)
    encoder.addTripleStatement(triple)
    if opt.delimited && buffer.size >= opt.frameSize then flushBuffer()

  override def quad(quad: Quad): Unit =
    handleGraph(quad.getGraph)
    encoder.addTripleStatement(
      quad.getSubject,
      quad.getPredicate,
      quad.getObject,
    )
    if opt.delimited && buffer.size >= opt.frameSize then flushBuffer()

  // Not supported
  override def base(base: String): Unit = ()

  override def prefix(prefix: String, iri: String): Unit =
    if opt.enableNamespaceDeclarations then
      encoder.declareNamespace(prefix, iri)
      if opt.delimited && buffer.size >= opt.frameSize then flushBuffer()

  private def handleGraph(graph: Node): Unit =
    if currentGraph == null then
      // First graph in the stream
      encoder.startGraph(graph)
      currentGraph = graph
    else if Quad.isDefaultGraph(currentGraph) then
      if !Quad.isDefaultGraph(graph) then
        // We are switching default -> named
        encoder.endGraph()
        encoder.startGraph(graph)
        currentGraph = graph
    else if Quad.isDefaultGraph(graph) || graph != currentGraph then
      // We are switching named -> named or named -> default
      encoder.endGraph()
      encoder.startGraph(graph)
      currentGraph = graph

  // Flush the buffer and finish the stream
  override def finish(): Unit =
    if currentGraph != null then
      encoder.endGraph()
      currentGraph = null
    if !opt.delimited then
      // Non-delimited variant – whole stream in one frame
      val frame = RdfStreamFrame(rows = buffer.toList)
      frame.writeTo(out)
    else if buffer.nonEmpty then flushBuffer()
    out.flush()

  private def flushBuffer(): Unit =
    val frame = RdfStreamFrame(rows = buffer.toList)
    frame.writeDelimitedTo(out)
    buffer.clear()
