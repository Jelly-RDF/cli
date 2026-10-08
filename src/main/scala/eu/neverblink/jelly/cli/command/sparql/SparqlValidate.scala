package eu.neverblink.jelly.cli.command.sparql

import caseapp.*
import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.sparql.util.*
import eu.neverblink.jelly.cli.util.io.{IoUtil, ProtoText}
import eu.neverblink.jelly.convert.jena.sparql.JenaSparqlConverterFactory
import eu.neverblink.jelly.core.proto.v1.sparql.{
  SparqlResultsFrame,
  SparqlResultsOptions,
  SparqlStreamType,
}
import eu.neverblink.jelly.core.sparql.{JellySparqlOptions, SparqlResultsHandler}
import org.apache.jena.graph.Node
import org.apache.jena.riot.{Lang, RIOT}
import org.apache.jena.riot.rowset.RowSetReaderRegistry
import org.apache.jena.sparql.core.Var
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
      "SPARQL results file to compare the input stream to. Can be given several times, for the " +
        "result sets of a PUNCTUATED stream. If not specified, no comparison is done.",
    )
    compareToFile: List[String] = Nil,
    @HelpMessage(
      "Format of the SPARQL results files to compare the input stream to. If not specified, " +
        "the format is inferred from the file names. Possible values: " +
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
        "is complete. Producers must always write one, so a stream without it is most likely " +
        "truncated. Use --require-trailer=false to check such a stream anyway. Default: true",
    )
    requireTrailer: Boolean = true,
) extends HasJellyCommandOptions

object SparqlValidate extends JellyCommand[SparqlValidateOptions]:
  private enum Delimiting:
    case Either, Delimited, Undelimited

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
    val referenceFiles =
      options.compareToFile.map(f => f -> referenceFormat(f, options.compareToFormat))
    val (inputStream, _) = getIoStreamsFromOptions(remainingArgs.remaining.headOption, None)
    SparqlJellyUtil.translateErrors {
      val (delimited, frames) = SparqlJellyUtil.iterateSparqlStreamWithDelimitingInfo(inputStream)
      validateDelimiting(delimiting, delimited)
      if referenceFiles.isEmpty then validateContent(frames, expectedOptions, None)
      else
        // Keep the reference files open while their solutions are compared
        Using.resource(References(referenceFiles)) { references =>
          validateContent(frames, expectedOptions, Some(references))
        }
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
    validateVersion(streamOptions)

  private def validateVersion(streamOptions: SparqlResultsOptions): Unit =
    if streamOptions.getVersion <= 0 then
      throw CriticalException(
        "The version field in SparqlResultsOptions is <= 0. " +
          "This field MUST be set to a positive value.",
      )

  private def printOptions(options: SparqlResultsOptions): String =
    ProtoText.print(SparqlResultsOptions.getDescriptor, options.toByteArray)

  /** Pushes all frames through the decoder, which catches most of the errors, and checks the rest.
    *
    * @param references
    *   the result sets to compare the input to, if any
    */
  private def validateContent(
      frames: Iterator[SparqlResultsFrame],
      expectedOptions: Option[SparqlResultsOptions],
      references: Option[References],
  ): Unit =
    if !frames.hasNext then throw CriticalException("Empty input stream")
    val handler = ComparingHandler(references)
    // The stream options are compared with the expected ones separately. Here, we only make sure
    // that the decoder accepts lookup tables as large as the expected ones.
    val supported = JellySparqlOptions.DEFAULT_SUPPORTED_OPTIONS.clone()
      .setStreamType(SparqlStreamType.PUNCTUATED)
    expectedOptions.foreach { e =>
      supported
        .setMaxNameTableSize(e.getMaxNameTableSize.max(supported.getMaxNameTableSize))
        .setMaxPrefixTableSize(e.getMaxPrefixTableSize.max(supported.getMaxPrefixTableSize))
        .setMaxDatatypeTableSize(e.getMaxDatatypeTableSize.max(supported.getMaxDatatypeTableSize))
    }
    val decoder = JenaSparqlConverterFactory.getInstance().decoder(handler, supported)
    var firstOptions: Option[SparqlResultsOptions] = None
    // Whether the previous frame ended with a trailer. Repeated options in a FLAT stream start a
    // new segment of a concatenated stream, which should also end with a trailer. In a PUNCTUATED
    // stream, the decoder only accepts them after a trailer.
    var trailerSeen = false
    def checkTrailer(): Unit =
      if getOptions.requireTrailer && !trailerSeen then
        throw CriticalException("The stream does not end with a trailer")

    for (frame, i) <- frames.zipWithIndex do
      Option(frame.getOptions) match
        case Some(o) if firstOptions.isEmpty =>
          validateOptions(o, expectedOptions)
          firstOptions = Some(o)
          handler.punctuated = o.getStreamType == SparqlStreamType.PUNCTUATED
        case Some(o) =>
          // Repeated options need not be the same as the first ones, but must be valid on their own
          validateVersion(o)
          checkTrailer()
        case None if firstOptions.isEmpty =>
          throw CriticalException("First frame in the input stream does not contain stream options")
        case None => ()
      decoder.ingestFrame(frame)
      Option(frame.getTrailer).foreach { trailer =>
        if trailer.getError.nonEmpty then
          throw CriticalException(
            s"The trailer in frame $i says that ${handler.resultSetName} is incomplete: " +
              trailer.getError,
          )
      }
      trailerSeen = frame.getTrailer != null
    checkTrailer()
    handler.finish()

  /** The result sets of the reference files, in order, with the name of the file each one is from.
    * The files are opened as they are needed, and the solutions are read as they are compared.
    */
  private class References(files: Seq[(String, Lang)])
      extends Iterator[(String, SparqlResultSet)],
        AutoCloseable:
    private val remainingFiles = files.iterator
    private val opened = mutable.ArrayBuffer[InputStream]()
    private var fileName = ""
    private var resultSets: Iterator[SparqlResultSet] = Iterator.empty

    override def hasNext: Boolean =
      while !resultSets.hasNext && remainingFiles.hasNext do
        val (name, lang) = remainingFiles.next()
        val is = IoUtil.inputStream(name)
        opened += is
        fileName = name
        resultSets = readReference(is, lang)
      resultSets.hasNext

    override def next(): (String, SparqlResultSet) =
      if !hasNext then throw java.util.NoSuchElementException()
      (fileName, resultSets.next())

    override def close(): Unit = opened.foreach(_.close())

  /** Checks what the decoder reads from the stream against the references, if there are any. */
  private class ComparingHandler(references: Option[References]) extends SparqlResultsHandler[Node]:
    // Set once the stream options are known
    var punctuated = false
    // Index of the current result set
    private var resultSetIndex = -1L
    // The result set to compare the current one to, and the file it is from
    private var reference: Option[(String, SparqlResultSet)] = None
    private var variables: IndexedSeq[Var] = IndexedSeq()
    // Number of solutions read so far in the current result set
    private var index = 0L
    private var blankNodes = BlankNodeMapping()

    /** How to refer to the current result set in messages. */
    def resultSetName: String =
      if punctuated then s"result set ${resultSetIndex.max(0)}" else "the result set"

    // Only in a PUNCTUATED stream, where there may be several result sets
    private def inResultSet: String = if punctuated then s" in result set $resultSetIndex" else ""

    /** Moves on to the next result set, and the next reference. */
    private def startResultSet(): Unit =
      finishResultSet()
      resultSetIndex += 1
      index = 0
      // Blank node labels are scoped to one result set
      blankNodes = BlankNodeMapping()
      reference = references.map { refs =>
        if !refs.hasNext then
          throw CriticalException(
            s"Expected $resultSetIndex result sets, as in the reference files, " +
              "but the input stream has more",
          )
        refs.next()
      }

    override def handleVariables(variables: java.util.List[String]): Unit =
      startResultSet()
      this.variables = variables.asScala.map(Var.alloc).toIndexedSeq
      reference match
        case Some((file, SparqlResultSet.Ask(_))) =>
          throw CriticalException(s"Expected an ASK result, as in $file, but got solutions")
        case Some((file, SparqlResultSet.Select(expected, _))) if expected != this.variables =>
          throw CriticalException(
            s"Expected variables ${expected.mkString(" ")}, as in $file, " +
              s"but got ${this.variables.mkString(" ")}$inResultSet",
          )
        case _ => ()

    override def createRowBuffer(size: Int): Array[Node] = new Array[Node](size)

    override def handleRow(row: Array[Node]): Unit =
      reference match
        case Some((file, SparqlResultSet.Select(_, rows))) =>
          if !rows.hasNext then
            throw CriticalException(
              s"Expected $index solutions, as in $file, but the input stream has more" +
                inResultSet,
            )
          val expected = rows.next()
          val matches = variables.indices.forall(i => blankNodes.sameTerm(expected(i), row(i)))
          if !matches then
            throw CriticalException(
              s"Solution $index$inResultSet does not match the one in $file\n" +
                s"Expected: ${formatRow(expected.toIndexedSeq)}\n" +
                s"Actual: ${formatRow(row.toIndexedSeq)}",
            )
        case _ => ()
      index += 1

    override def handleAskResult(value: Boolean): Unit =
      startResultSet()
      reference match
        case Some((file, SparqlResultSet.Ask(expected))) if expected != value =>
          throw CriticalException(
            s"Expected the ASK result to be $expected, as in $file, but it was $value" +
              inResultSet,
          )
        case Some((file, SparqlResultSet.Select(_, _))) =>
          throw CriticalException(
            s"Expected solutions, as in $file, but got an ASK result$inResultSet",
          )
        case _ => ()

    /** Checks that the reference has no more solutions than the input. */
    private def finishResultSet(): Unit = reference match
      case Some((file, SparqlResultSet.Select(_, rows))) if rows.hasNext =>
        throw CriticalException(
          s"Expected ${index + rows.size} solutions, as in $file, but got $index$inResultSet",
        )
      case _ => ()

    /** Checks that the references have no more result sets or solutions than the input. */
    def finish(): Unit =
      finishResultSet()
      references.foreach { refs =>
        if refs.hasNext then
          val count = resultSetIndex + 1
          throw CriticalException(
            s"Expected ${count + refs.size} result sets, as in the reference files, " +
              s"but got $count",
          )
      }

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
  private def readReference(inputStream: InputStream, lang: Lang): Iterator[SparqlResultSet] =
    if lang == SparqlFormat.JellySparql.jenaLang then
      SparqlResultSetReader(SparqlJellyUtil.iterateSparqlStream(inputStream))
    else
      val result = RowSetReaderRegistry.getFactory(lang).create(lang)
        .readAny(inputStream, RIOT.getContext.copy())
      if result.isBoolean then Iterator(SparqlResultSet.Ask(result.booleanResult.booleanValue))
      else
        val rowSet = result.rowSet
        val vars = rowSet.getResultVars.asScala.toIndexedSeq
        Iterator(
          SparqlResultSet.Select(
            vars,
            Iterator.continually(rowSet).takeWhile(_.hasNext).map(_.next())
              .map(b => vars.map(b.get).toArray),
          ),
        )
