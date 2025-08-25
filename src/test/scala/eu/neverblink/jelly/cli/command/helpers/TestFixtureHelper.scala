package eu.neverblink.jelly.cli.command.helpers

import eu.neverblink.jelly.cli.util.jena.JenaSystemOptions
import eu.neverblink.jelly.cli.util.jena.riot.CliRiot
import eu.neverblink.jelly.convert.jena.riot.{JellyFormatVariant, JellyLanguage}
import eu.neverblink.jelly.core.JellyOptions
import org.apache.jena.graph.Triple
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat, RDFLanguages, RDFWriter, RIOT}
import org.apache.jena.shared.impl.JenaParameters
import org.apache.jena.sparql.graph.GraphFactory
import org.apache.jena.sys.JenaSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec

import java.io.FileOutputStream
import java.nio.file.{Files, Path, Paths}
import java.util.UUID.randomUUID
import scala.util.Using

object TestFixtureHelper

trait TestFixtureHelper extends BeforeAndAfterAll:
  this: AnyWordSpec =>

  TestFixtureHelper.synchronized {
    JenaSystem.init()
    CliRiot.initialize()
  }

  private val specificTestDir: Path = Paths.get("src", "test", "resources")
  private val tmpDir: Path = Files.createTempDirectory("jelly-cli")

  /** The number of triples to generate for the tests
    */
  protected val testCardinality: Int

  private def getFileExtension(format: Lang = RDFLanguages.NQUADS): String =
    format.getFileExtensions.get(0)

  def withFullJenaFile(testCode: (String) => Any, jenaLang: Lang = RDFLanguages.NQUADS): Unit =
    val extension = getFileExtension(jenaLang)
    val tempFile = Files.createTempFile(tmpDir, randomUUID.toString, f".${extension}")
    val model = DataGenHelper.generateTripleModel(testCardinality)
    Using(new FileOutputStream(tempFile.toFile)) { fileOutputStream =>
      RDFDataMgr.write(fileOutputStream, model, jenaLang)
    }
    try {
      testCode(tempFile.toString)
    } finally { tempFile.toFile.delete() }

  def withEmptyJellyFile(testCode: (String) => Any): Unit =
    val extension = getFileExtension(JellyLanguage.JELLY)
    val tempFile = Files.createTempFile(tmpDir, randomUUID.toString, f".${extension}")
    try {
      testCode(tempFile.toString)
    } finally { tempFile.toFile.delete() }

  def withJenaFileOfContent[T](content: Seq[Triple], jenaLang: Lang = RDFLanguages.NQUADS)(
      testCode: String => T,
  ): T =
    val extension = getFileExtension(jenaLang)
    val tempFile = Files.createTempFile(tmpDir, randomUUID.toString, f".${extension}")
    val graph = GraphFactory.createGraphMem()
    content.foreach(graph.add)
    Using(new FileOutputStream(tempFile.toFile)) { fileOutputStream =>
      RDFDataMgr.write(fileOutputStream, graph, jenaLang)
    }
    try {
      testCode(tempFile.toString)
    } finally { tempFile.toFile.delete() }

  def withEmptyJellyTextFile(testCode: (String) => Any): Unit =
    val tempFile = Files.createTempFile(tmpDir, randomUUID.toString, ".jelly.txt")
    try {
      testCode(tempFile.toString)
    } finally { tempFile.toFile.delete() }

  def withFullJellyTextFile(testCode: (String) => Any): Unit =
    val tempFile = Files.createTempFile(tmpDir, randomUUID.toString, ".jelly.txt")
    val text = DataGenHelper.generateJellyText(testCardinality)
    Files.write(tempFile, text.getBytes)
    try {
      testCode(tempFile.toString)
    } finally { tempFile.toFile.delete() }

  def withEmptyRandomFile(testCode: (String) => Any): Unit =
    val tempFile = Files.createTempFile(tmpDir, randomUUID.toString, ".random")
    try {
      testCode(tempFile.toString)
    } finally { tempFile.toFile.delete() }

  def withSpecificJellyFile(
      testCode: (String) => Any,
      fileName: String,
  ): Unit = {
    val filePath = specificTestDir.resolve(fileName)
    if !Files.exists(filePath) then
      throw new IllegalArgumentException(s"File $fileName does not exist in $specificTestDir")
    else testCode(filePath.toString)
  }

  def withFullJellyFile(testCode: (String) => Any, frameSize: Int = 256): Unit =
    val extension = getFileExtension(JellyLanguage.JELLY)
    val tempFile = Files.createTempFile(tmpDir, randomUUID.toString, f".${extension}")
    val customFormat = new RDFFormat(
      JellyLanguage.JELLY,
      JellyFormatVariant.builder.frameSize(frameSize).build(),
    )

    val writerContext = RIOT.getContext.copy.set(
      JellyLanguage.SYMBOL_STREAM_OPTIONS,
      JellyOptions.SMALL_ALL_FEATURES.clone().setStreamName("Stream"),
    )
    val model = DataGenHelper.generateTripleModel(testCardinality)

    RDFWriter.create()
      .format(customFormat)
      .context(writerContext)
      .source(model)
      .build()
      .output(new FileOutputStream(tempFile.toFile))
    try {
      testCode(tempFile.toString)
    } finally { tempFile.toFile.delete() }

  def withEmptyJenaFile(testCode: (String) => Any, jenaLang: Lang = RDFLanguages.NQUADS): Unit =
    val extension = getFileExtension(jenaLang)
    val tempFile = Files.createTempFile(tmpDir, randomUUID.toString, f".${extension}")
    try {
      testCode(tempFile.toString)
    } finally { tempFile.toFile.delete() }

  override def afterAll(): Unit =
    Files.deleteIfExists(tmpDir)
    // Reset any Jena system options we might have changed during tests
    JenaSystemOptions.resetTermValidation()
    JenaParameters.enableEagerLiteralValidation = false
