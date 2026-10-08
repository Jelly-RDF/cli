package eu.neverblink.jelly.cli.command.sparql.util

import eu.neverblink.jelly.cli.CriticalException
import eu.neverblink.jelly.convert.jena.sparql.JenaSparqlConverterFactory
import eu.neverblink.jelly.core.proto.v1.sparql.{
  SparqlResultsFrame,
  SparqlResultsOptions,
  SparqlStreamType,
}
import eu.neverblink.jelly.core.sparql.{JellySparqlOptions, SparqlResultsHandler}
import org.apache.jena.graph.Node
import org.apache.jena.sparql.core.Var

import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

/** One result set of a Jelly-SPARQL stream. */
enum SparqlResultSet:
  case Select(variables: IndexedSeq[Var], rows: Iterator[Array[Node]])
  case Ask(value: Boolean)

/** Reads the result sets of a Jelly-SPARQL stream one by one.
  *
  * Unlike Jena's reader, this one pulls frames from the stream only when it needs them, so it can
  * be read side by side with another stream.
  *
  * @throws CriticalException
  *   when it reaches a trailer saying that a result set is incomplete
  */
final class SparqlResultSetReader(
    frames: Iterator[SparqlResultsFrame],
    supportedOptions: SparqlResultsOptions = JellySparqlOptions.DEFAULT_SUPPORTED_OPTIONS,
) extends Iterator[SparqlResultSet]:
  private enum Event:
    case Start(variables: IndexedSeq[Var])
    case Row(values: Array[Node])
    case Ask(value: Boolean)
    case Trailer(error: String)

  private val events = mutable.Queue[Event]()

  private val handler = new SparqlResultsHandler[Node]:
    override def handleVariables(variables: java.util.List[String]): Unit =
      events += Event.Start(variables.asScala.map(Var.alloc).toIndexedSeq)
    override def createRowBuffer(size: Int): Array[Node] = new Array[Node](size)
    // The decoder reuses the row buffer, so it must be copied
    override def handleRow(row: Array[Node]): Unit = events += Event.Row(row.clone())
    override def handleAskResult(value: Boolean): Unit = events += Event.Ask(value)
    override def handleTrailer(error: String): Unit = events += Event.Trailer(error)

  private val decoder = JenaSparqlConverterFactory.getInstance().decoder(
    handler,
    supportedOptions.clone().setStreamType(SparqlStreamType.PUNCTUATED),
  )

  // Rows of the current result set, which must be skipped before the next one starts
  private var rows: Iterator[Array[Node]] = Iterator.empty

  /** Whether the stream is PUNCTUATED. Known once [[hasNext]] was called. */
  def isPunctuated: Boolean =
    Option(decoder.getSparqlOptions).exists(_.getStreamType == SparqlStreamType.PUNCTUATED)

  /** Reads frames until the decoder reports something, or the stream ends. */
  private def peek: Option[Event] =
    while events.isEmpty && frames.hasNext do decoder.ingestFrame(frames.next())
    events.headOption

  private def checkError(error: String): Unit =
    if error.nonEmpty then
      throw CriticalException(s"The producer could not complete the result set: $error")

  @tailrec
  override def hasNext: Boolean =
    rows.foreach(_ => ())
    peek match
      case Some(Event.Trailer(error)) =>
        // The end of the previous result set
        events.dequeue()
        checkError(error)
        hasNext
      case Some(Event.Start(_) | Event.Ask(_)) => true
      // The decoder reports rows only after the variables
      case Some(Event.Row(_)) => throw IllegalStateException("Row outside of a result set")
      case None => false

  override def next(): SparqlResultSet =
    if !hasNext then throw java.util.NoSuchElementException()
    events.dequeue() match
      case Event.Start(variables) =>
        rows = rowIterator()
        SparqlResultSet.Select(variables, rows)
      case Event.Ask(value) => SparqlResultSet.Ask(value)
      case e => throw IllegalStateException(s"Unexpected $e")

  /** In a PUNCTUATED stream, a trailer ends the result set. In a FLAT stream, it may be followed by
    * the repeated stream options and more rows of the same result set.
    */
  private def rowIterator(): Iterator[Array[Node]] = new Iterator[Array[Node]]:
    override def hasNext: Boolean = peek match
      case Some(Event.Row(_)) => true
      case Some(Event.Trailer(error)) =>
        checkError(error)
        if isPunctuated then false
        else
          events.dequeue()
          hasNext
      case _ => false

    override def next(): Array[Node] =
      if !hasNext then throw java.util.NoSuchElementException()
      events.dequeue() match
        case Event.Row(values) => values
        case e => throw IllegalStateException(s"Unexpected $e")
