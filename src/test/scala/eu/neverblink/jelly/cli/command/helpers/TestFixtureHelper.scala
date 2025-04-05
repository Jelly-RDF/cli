package eu.neverblink.jelly.cli.command.helpers

import eu.ostrzyciel.jelly.convert.jena.riot.{JellyFormatVariant, JellyLanguage}
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat, RDFLanguages}
import org.apache.jena.sys.JenaSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec

import java.io.FileOutputStream
import java.nio.file.{Files, Path}
import java.util.UUID.randomUUID
import scala.util.Using

object TestFixtureHelper

trait TestFixtureHelper extends BeforeAndAfterAll:
  this: AnyWordSpec =>

  TestFixtureHelper.synchronized {
    JenaSystem.init()
  }

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

  def withFullJellyFile(testCode: (String) => Any, frameSize: Int = 256): Unit =
    val extension = getFileExtension(JellyLanguage.JELLY)
    val tempFile = Files.createTempFile(tmpDir, randomUUID.toString, f".${extension}")
    val customFormat = new RDFFormat(
      JellyLanguage.JELLY,
      JellyFormatVariant(frameSize = frameSize),
    )
    val model = DataGenHelper.generateTripleModel(testCardinality)
    RDFDataMgr.write(new FileOutputStream(tempFile.toFile), model, customFormat)
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
