package eu.neverblink.jelly.cli.command.helpers

import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFLanguages}
import org.apache.jena.sys.JenaSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec

import java.io.FileOutputStream
import java.nio.file.{Files, Path}
import java.util.UUID.randomUUID

object TestFixtureHelper

trait TestFixtureHelper extends BeforeAndAfterAll:
  this: AnyWordSpec =>

  TestFixtureHelper.synchronized {
    JenaSystem.init()
  }

  private val tmpDir: Path = Files.createTempDirectory("jelly-cli")

  /** The number of triples to generate for the tests
    */
  protected val testCardinality: Integer

  private def getFileExtension(format: Lang = RDFLanguages.NQUADS): String =
    format.getFileExtensions.get(0)

  def withFullQuadFile(testCode: (String) => Any): Unit =
    val extension = getFileExtension(RDFLanguages.NQUADS)
    val tempFile = Files.createTempFile(tmpDir, randomUUID.toString, f".${extension}")
    val model = DataGenHelper.generateTripleModel(testCardinality)
    RDFDataMgr.write(new FileOutputStream(tempFile.toFile), model, RDFLanguages.NQUADS)
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

  def withEmptyRandomFile(testCode: (String) => Any): Unit =
    val tempFile = Files.createTempFile(tmpDir, randomUUID.toString, ".random")
    try {
      testCode(tempFile.toString)
    } finally { tempFile.toFile.delete() }

  def withFullJellyFile(testCode: (String) => Any): Unit =
    val extension = getFileExtension(JellyLanguage.JELLY)
    val tempFile = Files.createTempFile(tmpDir, randomUUID.toString, f".${extension}")
    val model = DataGenHelper.generateTripleModel(testCardinality)
    RDFDataMgr.write(new FileOutputStream(tempFile.toFile), model, JellyLanguage.JELLY)
    try {
      testCode(tempFile.toString)
    } finally { tempFile.toFile.delete() }

  def withEmptyQuadFile(testCode: (String) => Any): Unit =
    val extension = getFileExtension(RDFLanguages.NQUADS)
    val tempFile = Files.createTempFile(tmpDir, randomUUID.toString, f".${extension}")
    try {
      testCode(tempFile.toString)
    } finally { tempFile.toFile.delete() }

  override def afterAll(): Unit =
    Files.deleteIfExists(tmpDir)
