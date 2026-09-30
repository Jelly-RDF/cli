package eu.neverblink.jelly.cli.command.sparql

import caseapp.*
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.sparql.util.*
import eu.neverblink.jelly.cli.util.io.{IoUtil, ProtoText}
import eu.neverblink.jelly.convert.jena.sparql.JenaSparqlConverterFactory
import eu.neverblink.jelly.core.proto.v1.sparql.{SparqlResultsFrame, SparqlResultsOptions}
import eu.neverblink.jelly.core.sparql.{JellySparqlOptions, SparqlResultsHandler}
import org.apache.jena.graph.Node
import org.apache.jena.riot.{Lang, RIOT}
import org.apache.jena.riot.resultset.ResultSetReaderRegistry
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.util.FmtUtils

import java.io.InputStream
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.Using

object SparqlValidatePrint:
  val validFormats: List[SparqlFormat] = List(
    SparqlFormat.Json,
    SparqlFormat.Xml,
    SparqlFormat.Csv,
    SparqlFormat.Tsv,
    SparqlFormat.JellySparql,
  )
  lazy val validFormatsString: String = SparqlFormat.validFormatsString(validFormats)

@HelpMessage(
  "Validates a Jelly-SPARQL stream.\nIf no additional options are specified, " +
    "only basic validations are performed. You can also validate the stream against " +
    "a reference SPARQL results file, check the stream options, its delimiting, and whether " +
    "it ends with a trailer.\n" +
    "A stream whose trailer reports an error (an incomplete result set) is always invalid.\n" +
    "If an error is detected, the program will exit with a non-zero code.\n" +
    "Otherwise, the program will exit with code 0.\n" +
    "When comparing to a reference file, the solutions must come in the same order, and blank " +
    "nodes are compared up to renaming.\n" +
    "Note: this command works in a streaming manner, also when comparing: the solutions are " +
    "compared one by one, as they are read.",
)
@ArgsName("<file-to-validate>")
case class SparqlValidateOptions(
    @Recurse
    common: JellyCommandOptions = JellyCommandOptions(),
    @HelpMessage(
      "SPARQL results file to compare the input stream to. If not specified, no comparison is done.",
    )
    compareToFile: Option[String] = None,
    @HelpMessage(
      "Format of the SPARQL results file to compare the input stream to. If not specified, " +
        "the format is inferred from the file name. Possible values: " +
        SparqlValidatePrint.validFormatsString,
    )
    compareToFormat: Option[String] = None,
    @HelpMessage(
      "Jelly-SPARQL file with the expected stream options. If not specified, the options are " +
        "not checked.",
    )
    optionsFile: Option[String] = None,
    @HelpMessage(
      "Whether the input stream should be checked to be delimited or undelimited. " +
        "Possible values: 'either', 'true', 'false'. Default: 'either'.",
    )
    delimited: String = "either",
    @HelpMessage(
      "Whether the stream must end with a trailer, which tells the reader that the result set " +
        "is complete. Default: false",
    )
    requireTrailer: Boolean = false,
) extends HasJellyCommandOptions

object SparqlValidate extends JellyCommand[SparqlValidateOptions]:
  private enum Delimiting:
    case Either, Delimited, Undelimited

  /** The result set of the reference file. The solutions are read as they are compared. */
  private enum Reference:
    case Select(variables: Seq[Var], rows: Iterator[Binding])
    case Ask(value: Boolean)

  override def names: List[List[String]] = List(List("sparql", "validate"))

  override def group = "sparql"

  override def doRun(options: SparqlValidateOptions, remainingArgs: RemainingArgs): Unit =
    val delimiting = options.delimited match
      case "" | "either" => Delimiting.Either
      case "true" => Delimiting.Delimited
      case "false" => Delimiting.Undelimited
      case _ =>
        throw InvalidArgument(
          "--delimited",
          options.delimited,
          Some("Valid values: true, false, either"),
        )
    val expectedOptions = options.optionsFile.map(SparqlJellyUtil.loadOptionsFromFile)
    val referenceLang = options.compareToFile.map(referenceFormat(_, options.compareToFormat))
    val (inputStream, _) = getIoStreamsFromOptions(remainingArgs.remaining.headOption, None)
    SparqlJellyUtil.translateErrors {
      val (delimited, frames) = SparqlJellyUtil.iterateSparqlStreamWithDelimitingInfo(inputStream)
      validateDelimiting(delimiting, delimited)
      (options.compareToFile, referenceLang) match
        case (Some(fileName), Some(lang)) =>
          // Keep the reference file open while its solutions are compared
          Using.resource(IoUtil.inputStream(fileName)) { is =>
            validateContent(frames, expectedOptions, Some(readReference(is, lang)))
          }
        case _ => validateContent(frames, expectedOptions, None)
    }

  private def validateDelimiting(expected: Delimiting, delimited: Boolean): Unit =
    expected match
      case Delimiting.Either => ()
      case Delimiting.Delimited =>
        if !delimited then
          throw CriticalException("Expected delimited input, but the file was not delimited")
      case Delimiting.Undelimited =>
        if delimited then
          throw CriticalException("Expected undelimited input, but the file was delimited")

  private def validateOptions(
      streamOptions: SparqlResultsOptions,
      expectedOptions: Option[SparqlResultsOptions],
  ): Unit =
    expectedOptions.foreach { expected =>
      if streamOptions != expected then
        throw CriticalException(
          s"Stream options do not match the expected options in ${getOptions.optionsFile.get}\n" +
            s"Expected: ${printOptions(expected)}\n" +
            s"Actual: ${printOptions(streamOptions)}",
        )
    }
    if streamOptions.getVersion <= 0 then
      throw CriticalException(
        "The version field in SparqlResultsOptions is <= 0. " +
          "This field MUST be set to a positive value.",
      )

  private def printOptions(options: SparqlResultsOptions): String =
    ProtoText.print(SparqlResultsOptions.getDescriptor, options.toByteArray)

  /** Pushes all frames through the decoder, which catches most of the errors, and checks the rest.
    *
    * @param reference
    *   the result set to compare the input to, if any
    */
  private def validateContent(
      frames: Iterator[SparqlResultsFrame],
      expectedOptions: Option[SparqlResultsOptions],
      reference: Option[Reference],
  ): Unit =
    if !frames.hasNext then throw CriticalException("Empty input stream")
    val handler = ComparingHandler(reference)
    // The stream options are compared with the expected ones separately. Here, we only make sure
    // that the decoder accepts lookup tables as large as the expected ones.
    val supported = JellySparqlOptions.DEFAULT_SUPPORTED_OPTIONS.clone()
    expectedOptions.foreach { e =>
      supported
        .setMaxNameTableSize(e.getMaxNameTableSize.max(supported.getMaxNameTableSize))
        .setMaxPrefixTableSize(e.getMaxPrefixTableSize.max(supported.getMaxPrefixTableSize))
        .setMaxDatatypeTableSize(e.getMaxDatatypeTableSize.max(supported.getMaxDatatypeTableSize))
    }
    val decoder = JenaSparqlConverterFactory.getInstance().decoder(handler, supported)
    var firstOptions: Option[SparqlResultsOptions] = None
    // Whether the current stream ended with a trailer. Repeated options start a new stream.
    var trailerSeen = false
    def checkTrailer(): Unit =
      // An ASK result is complete on its own, so it does not need a trailer
      if getOptions.requireTrailer && !trailerSeen && handler.askResult.isEmpty then
        throw CriticalException("The stream does not end with a trailer")

    for (frame, i) <- frames.zipWithIndex do
      Option(frame.getOptions) match
        case Some(o) if firstOptions.isEmpty =>
          validateOptions(o, expectedOptions)
          firstOptions = Some(o)
        case Some(o) =>
          if !firstOptions.contains(o) then
            throw CriticalException(
              s"Later occurrence of stream options in frame $i does not match the first",
            )
          checkTrailer()
          trailerSeen = false
        case None if firstOptions.isEmpty =>
          throw CriticalException("First frame in the input stream does not contain stream options")
        case None => ()
      decoder.ingestFrame(frame)
      Option(frame.getTrailer).foreach { trailer =>
        if trailer.getError.nonEmpty then
          throw CriticalException(
            s"The trailer in frame $i says that the result set is incomplete: ${trailer.getError}",
          )
        trailerSeen = true
      }
    checkTrailer()
    handler.finish()

  /** Checks what the decoder reads from the stream against the reference, if there is one. */
  private class ComparingHandler(reference: Option[Reference]) extends SparqlResultsHandler[Node]:
    var askResult: Option[Boolean] = None
    private var variables: IndexedSeq[Var] = IndexedSeq()
    // Number of solutions read so far
    private var index = 0L
    private val blankNodes = BlankNodeMapping()

    private def fileName = getOptions.compareToFile.getOrElse("")

    override def handleVariables(variables: java.util.List[String]): Unit =
      this.variables = variables.asScala.map(Var.alloc).toIndexedSeq
      reference match
        case Some(Reference.Ask(_)) =>
          throw CriticalException(s"Expected an ASK result, as in $fileName, but got solutions")
        case Some(Reference.Select(expected, _)) if expected != this.variables =>
          throw CriticalException(
            s"Expected variables ${expected.mkString(" ")}, as in $fileName, " +
              s"but got ${this.variables.mkString(" ")}",
          )
        case _ => ()

    override def createRowBuffer(size: Int): Array[Node] = new Array[Node](size)

    override def handleRow(row: Array[Node]): Unit =
      reference match
        case Some(Reference.Select(_, rows)) =>
          if !rows.hasNext then
            throw CriticalException(
              s"Expected $index solutions, as in $fileName, but the input stream has more",
            )
          val expected = rows.next()
          val matches = variables.indices.forall { i =>
            blankNodes.sameTerm(expected.get(variables(i)), row(i))
          }
          if !matches then
            throw CriticalException(
              s"Solution $index does not match the one in $fileName\n" +
                s"Expected: ${formatRow(variables.map(expected.get))}\n" +
                s"Actual: ${formatRow(row.toIndexedSeq)}",
            )
        case _ => ()
      index += 1

    override def handleAskResult(value: Boolean): Unit =
      askResult = Some(value)
      reference match
        case Some(Reference.Ask(expected)) if expected != value =>
          throw CriticalException(s"Expected the ASK result to be $expected, but it was $value")
        case Some(Reference.Select(_, _)) =>
          throw CriticalException(s"Expected solutions, as in $fileName, but got an ASK result")
        case _ => ()

    /** Checks that the reference has no more solutions than the input. */
    def finish(): Unit = reference match
      case Some(Reference.Select(_, rows)) if rows.hasNext =>
        throw CriticalException(
          s"Expected ${index + rows.size} solutions, as in $fileName, but got $index",
        )
      case _ => ()

    private def formatRow(values: Seq[Node]): String =
      variables.zip(values).map { (v, n) =>
        s"$v=${if n == null then "(unbound)" else FmtUtils.stringForNode(n)}"
      }.mkString(" ")

  /** Compares terms, with blank nodes matched up to renaming.
    *
    * As the solutions are compared in order, the first time a blank node label is seen on one side,
    * it is paired with the label on the other side. From then on, the pair must always come
    * together.
    */
  private class BlankNodeMapping:
    private val expectedToActual = mutable.HashMap[Node, Node]()
    private val actualToExpected = mutable.HashMap[Node, Node]()

    /** Whether the terms are the same. Nulls stand for unbound values. */
    def sameTerm(expected: Node, actual: Node): Boolean =
      if expected == null || actual == null then expected eq actual
      else if expected.isBlank && actual.isBlank then
        expectedToActual.get(expected) match
          case Some(paired) => paired == actual
          case None if actualToExpected.contains(actual) => false
          case None =>
            expectedToActual(expected) = actual
            actualToExpected(actual) = expected
            true
      else if expected.isTripleTerm && actual.isTripleTerm then
        val e = expected.getTriple
        val a = actual.getTriple
        sameTerm(e.getSubject, a.getSubject) && sameTerm(e.getPredicate, a.getPredicate) &&
        sameTerm(e.getObject, a.getObject)
      else expected == actual

  /** Picks the format of the reference file. */
  private def referenceFormat(fileName: String, formatName: Option[String]): Lang =
    val format = formatName match
      case Some(name) =>
        SparqlFormat.find(name).filter(SparqlValidatePrint.validFormats.contains).getOrElse {
          throw InvalidFormatSpecified(name, SparqlValidatePrint.validFormatsString)
        }
      case None =>
        SparqlFormat.inferFormat(fileName)
          .filter(SparqlValidatePrint.validFormats.contains)
          .getOrElse {
            throw InvalidFormatSpecified("", SparqlValidatePrint.validFormatsString)
          }
    format match
      case f: SparqlFormat.Jena => f.jenaLang
      case f => throw CriticalException(s"Cannot read $f for comparison")

  /** Starts reading the reference file. The solutions are read lazily, as they are compared. */
  private def readReference(inputStream: InputStream, lang: Lang): Reference =
    val result = ResultSetReaderRegistry.getFactory(lang).create(lang)
      .readAny(inputStream, RIOT.getContext.copy())
    if result.isBoolean then Reference.Ask(result.getBooleanResult.booleanValue)
    else
      val rs = result.getResultSet
      Reference.Select(
        rs.getResultVars.asScala.map(Var.alloc).toSeq,
        Iterator.continually(rs).takeWhile(_.hasNext).map(_.nextBinding()),
      )
