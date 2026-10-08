package eu.neverblink.jelly.cli.util.jena

import eu.neverblink.jelly.cli.*
import eu.neverblink.jelly.cli.command.rdf.util.JellyUtil
import eu.neverblink.jelly.cli.command.sparql.util.{
  SparqlJellyUtil,
  SparqlResultSet,
  SparqlResultSetReader,
}
import eu.neverblink.jelly.cli.util.jena.riot.{JellyFrameWriter, JellyWriterUtil}
import eu.neverblink.jelly.convert.jena.JenaConverterFactory
import eu.neverblink.jelly.convert.jena.sparql.JellySparqlLanguage
import eu.neverblink.jelly.core.JellyOptions
import eu.neverblink.jelly.core.RdfHandler.AnyStatementHandler
import eu.neverblink.jelly.core.proto.v1.*
import eu.neverblink.jelly.core.proto.v1.sparql.SparqlResultsOptions
import eu.neverblink.jelly.core.sparql.JellySparqlOptions
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.riot.RIOT
import org.apache.jena.riot.system.StreamRDF
import org.apache.jena.riot.rowset.RowSetWriterRegistry
import org.apache.jena.sparql.core.{Quad, Var}
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.exec.RowSetStream

import java.io.{InputStream, OutputStream}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.*

/** Converts between Jelly-RDF streams and Jelly-SPARQL result sets.
  *
  * A statement becomes a solution of ?s ?p ?o (triples) or ?s ?p ?o ?g (quads), where ?g is unbound
  * for the default graph. The other way round, the variables are taken by position: subject,
  * predicate, object, and, if there is a fourth one, graph.
  */
object RdfSparqlConverter:

  /** Default options of the Jelly-SPARQL output, for the given Jelly-RDF input options.
    *
    * This is the BIG preset, with the RDF version inferred from the input.
    */
  def defaultSparqlOptions(inputOptions: RdfStreamOptions): SparqlResultsOptions =
    JellySparqlOptions.BIG.clone().setRdfVersion(
      if inputOptions.getRdfStar then RdfVersion.RDF_VERSION_1_2 else RdfVersion.RDF_VERSION_1_1,
    )

  /** Converts a Jelly-RDF stream to a Jelly-SPARQL result set, one solution per statement.
    *
    * Whether the result set has the ?g variable depends on the physical type of the input: TRIPLES
    * streams give 3 variables, QUADS and GRAPHS streams give 4. Namespace declarations are dropped.
    *
    * @param outputOptions
    *   makes the stream options of the output from the options of the input
    * @param valuesPerFrame
    *   maximum number of values (cells) in one output frame
    * @param delimited
    *   whether the output should be delimited
    */
  def rdfToSparql(
      inputStream: InputStream,
      outputStream: OutputStream,
      outputOptions: RdfStreamOptions => SparqlResultsOptions,
      valuesPerFrame: Int,
      delimited: Boolean,
  ): Unit =
    SparqlJellyUtil.translateErrors {
      val (inputOptions, vars, bindings) = readStatements(inputStream)
      val context = RIOT.getContext.copy()
        .set(JellySparqlLanguage.SYMBOL_STREAM_OPTIONS, outputOptions(inputOptions))
        .set(JellySparqlLanguage.SYMBOL_MAX_VALUES_PER_FRAME, valuesPerFrame)
        .set(JellySparqlLanguage.SYMBOL_DELIMITED_OUTPUT, delimited)
      RowSetWriterRegistry
        .getFactory(JellySparqlLanguage.JELLY_SPARQL)
        .create(JellySparqlLanguage.JELLY_SPARQL)
        .write(outputStream, RowSetStream.create(vars.asJava, bindings.asJava), context)
      outputStream.flush()
    }

  /** Reads the Jelly-RDF stream frame by frame, turning every statement into a solution.
    *
    * @return
    *   the options of the input, the variables of the result set, and a lazy iterator of its
    *   solutions
    */
  private def readStatements(
      inputStream: InputStream,
  ): (RdfStreamOptions, Seq[Var], Iterator[Binding]) =
    val frames = JellyUtil.iterateRdfStream(inputStream).buffered
    val inputOptions = frames.headOption.flatMap(_.getRows.asScala.headOption) match
      case Some(row) if row.hasOptions => row.getOptions
      case Some(_) => throw CriticalException("First input row is not an options row")
      case None => throw CriticalException("Empty input stream")
    val quads = inputOptions.getPhysicalType match
      case PhysicalStreamType.TRIPLES => false
      case PhysicalStreamType.QUADS | PhysicalStreamType.GRAPHS => true
      case t => throw CriticalException(s"Unsupported physical stream type: $t")
    val vars = if quads then StatementBinding.quadVars else StatementBinding.tripleVars

    // The decoder pushes the statements of one frame here, and the iterator takes them out
    var buffer = ArrayBuffer[Binding]()
    val handler = new AnyStatementHandler[Node] {
      override def handleTriple(s: Node, p: Node, o: Node): Unit =
        buffer += TripleBinding(s, p, o)

      override def handleQuad(s: Node, p: Node, o: Node, g: Node): Unit =
        if !quads then
          throw CriticalException(
            "Found a quad in a stream that started as a TRIPLES stream. " +
              "The result set only has the ?s ?p ?o variables.",
          )
        buffer += (if Quad.isDefaultGraph(g) then TripleBinding(s, p, o)
                   else QuadBinding(s, p, o, g))
    }
    val decoder = JenaConverterFactory.getInstance().anyStatementDecoder(
      handler,
      JellyOptions.DEFAULT_SUPPORTED_OPTIONS,
    )
    val bindings = frames.flatMap { (frame: RdfStreamFrame) =>
      // A new buffer for every frame, so that the iterator can take the old one as it is
      buffer = ArrayBuffer[Binding]()
      frame.getRows.forEach(decoder.ingestRow)
      buffer
    }
    (inputOptions, vars, bindings)

  /** Converts a Jelly-SPARQL stream to a Jelly-RDF stream, one statement per solution.
    *
    * A FLAT stream gives a stream with frames of `rowsPerFrame` rows. A PUNCTUATED stream gives one
    * frame per result set, with the logical type set to GRAPHS (TRIPLES output) or DATASETS
    * (otherwise), unless it was set already.
    *
    * @param outputOptions
    *   stream options of the output. The physical type may be unspecified.
    * @param rowsPerFrame
    *   target number of rows per output frame, for FLAT input
    * @param delimited
    *   whether the output should be delimited
    */
  def sparqlToRdf(
      inputStream: InputStream,
      outputStream: OutputStream,
      outputOptions: RdfStreamOptions,
      rowsPerFrame: Int,
      delimited: Boolean,
  ): Unit =
    SparqlJellyUtil.translateErrors {
      val resultSets = SparqlResultSetReader(SparqlJellyUtil.iterateSparqlStream(inputStream))
      if !resultSets.hasNext then throw CriticalException("Empty input stream")
      val first = selectResult(resultSets.next(), 0)
      val jellyOpt = withPhysicalType(outputOptions, first.variables.size == 4)
      if resultSets.isPunctuated then
        if jellyOpt.getLogicalType == LogicalStreamType.UNSPECIFIED then
          jellyOpt.setLogicalType(
            if jellyOpt.getPhysicalType == PhysicalStreamType.TRIPLES then LogicalStreamType.GRAPHS
            else LogicalStreamType.DATASETS,
          )
        val writer = JellyFrameWriter(jellyOpt, delimited, outputStream)
        writeStatements(first, writer, jellyOpt, Some(0))
        writer.endFrame()
        for (resultSet, i) <- resultSets.zipWithIndex do
          writeStatements(selectResult(resultSet, i + 1), writer, jellyOpt, Some(i + 1))
          writer.endFrame()
        writer.finish()
      else
        val writer = JellyWriterUtil.createWriter(
          jellyOpt,
          rowsPerFrame,
          enableNamespaceDeclarations = false,
          delimited,
          outputStream,
        )
        writer.start()
        writeStatements(first, writer, jellyOpt)
        writer.finish()
      outputStream.flush()
    }

  /** Checks that the result set is a solution sequence that can be made into statements. */
  private def selectResult(resultSet: SparqlResultSet, index: Int): SparqlResultSet.Select =
    resultSet match
      case _: SparqlResultSet.Ask =>
        throw CriticalException(
          (if index == 0 then "The input is" else s"Result set $index is") +
            " an ASK result, which cannot be converted to RDF",
        )
      case select: SparqlResultSet.Select =>
        val vars = select.variables
        if vars.size != 3 && vars.size != 4 then
          throw CriticalException(
            (if index == 0 then "The result set" else s"Result set $index") +
              s" must have 3 or 4 variables, but it has ${vars.size}: " + vars.mkString(", "),
          )
        select

  /** Writes the solutions of the result set as statements.
    *
    * @param resultSetIndex
    *   the index of the result set in a PUNCTUATED stream, for error messages
    */
  private def writeStatements(
      resultSet: SparqlResultSet.Select,
      writer: StreamRDF,
      jellyOpt: RdfStreamOptions,
      resultSetIndex: Option[Int] = None,
  ): Unit =
    val vars = resultSet.variables
    val quads = vars.size == 4
    val triples = jellyOpt.getPhysicalType == PhysicalStreamType.TRIPLES
    val where = resultSetIndex.map(i => s" of result set $i").getOrElse("")
    if quads && triples then
      throw CriticalException(
        s"Result set${resultSetIndex.map(" " + _).getOrElse("")} has 4 variables, but the output is a " +
          "TRIPLES stream. Use --opt.physical-type=QUADS or GRAPHS.",
      )
    var index = 0L
    for row <- resultSet.rows do
      def required(i: Int): Node =
        val node = row(i)
        if node == null then
          throw CriticalException(
            s"Solution $index$where has no value for ${vars(i)}, so it cannot be made into a " +
              "statement",
          )
        node
      val triple = Triple.create(required(0), required(1), required(2))
      if triples then
        checkStatement(
          triple.toString,
          StatementUtils.isGeneralized(triple),
          StatementUtils.hasTripleTerms(triple),
          s"$index$where",
          jellyOpt,
        )
        writer.triple(triple)
      else
        val graph = (if quads then Option(row(3)) else None).getOrElse(Quad.defaultGraphIRI)
        val quad = Quad.create(graph, triple)
        checkStatement(
          quad.toString,
          StatementUtils.isGeneralized(quad),
          StatementUtils.hasTripleTerms(quad),
          s"$index$where",
          jellyOpt,
        )
        writer.quad(quad)
      index += 1

  /** The output options, with the physical type set to match the number of variables, unless it was
    * set already.
    */
  private def withPhysicalType(
      options: RdfStreamOptions,
      quads: Boolean,
  ): RdfStreamOptions.Mutable =
    val jellyOpt = options.clone()
    jellyOpt.getPhysicalType match
      case PhysicalStreamType.UNSPECIFIED =>
        jellyOpt.setPhysicalType(
          if quads then PhysicalStreamType.QUADS else PhysicalStreamType.TRIPLES,
        )
      case PhysicalStreamType.TRIPLES if quads =>
        throw InvalidArgument(
          "--opt.physical-type",
          "TRIPLES",
          Some("The result set has 4 variables, so the output must be QUADS or GRAPHS"),
        )
      case _ => jellyOpt

  private def checkStatement(
      statement: String,
      generalized: Boolean,
      tripleTerms: Boolean,
      solution: String,
      opt: RdfStreamOptions,
  ): Unit =
    if !opt.getGeneralizedStatements && generalized then
      throw CriticalException(
        s"Solution $solution is not a valid RDF statement: $statement. " +
          "Use --opt.generalized-statements=true to allow generalized statements.",
      )
    if !opt.getRdfStar && tripleTerms then
      throw CriticalException(
        s"Solution $solution contains a triple term: $statement. " +
          "Use --opt.triple-terms=true to allow triple terms.",
      )
