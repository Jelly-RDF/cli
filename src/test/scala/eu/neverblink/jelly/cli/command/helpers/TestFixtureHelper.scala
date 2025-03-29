package eu.neverblink.jelly.cli.command.helpers

import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFLanguages}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec

import java.io.FileOutputStream
import java.nio.file.{Files, Path}
import java.util.UUID.randomUUID

trait TestFixtureHelper extends BeforeAndAfterAll:
  this: AnyWordSpec =>

  protected val tmpDir: Path

  protected val testCardinality: Integer

  private def getFileExtension(format: Lang = RDFLanguages.NQUADS): String =
    format.getFileExtensions.get(0)

  def withFullQuadFile(testCode: (String) => Any): Unit = {
    val extension = getFileExtension(RDFLanguages.NQUADS)
    val tempFile = Files.createFile(tmpDir.resolve(f"${randomUUID}.${extension}"))
    val model = DataGenHelper.generateTripleModel(testCardinality)
    RDFDataMgr.write(new FileOutputStream(tempFile.toFile), model, RDFLanguages.NQUADS)
    try {
      testCode(tempFile.toString)
    } finally Files.deleteIfExists(tempFile)
  }

  def withEmptyJellyFile(testCode: (String) => Any): Unit = {
    val extension = getFileExtension(JellyLanguage.JELLY)
    val tempFile = Files.createFile(tmpDir.resolve(f"${randomUUID}.${extension}"))
    try {
      testCode(tempFile.toString)
    } finally Files.deleteIfExists(tempFile)
  }

  def withFullJellyFile(testCode: (String) => Any): Unit = {
    val extension = getFileExtension(JellyLanguage.JELLY)
    val tempFile = Files.createFile(tmpDir.resolve(f"${randomUUID}.${extension}"))
    val model = DataGenHelper.generateTripleModel(testCardinality)
    RDFDataMgr.write(new FileOutputStream(tempFile.toFile), model, JellyLanguage.JELLY)
    try {
      testCode(tempFile.toString)
    } finally Files.deleteIfExists(tempFile)
  }

  def withEmptyQuadFile(testCode: (String) => Any): Unit = {
    val extension = getFileExtension(RDFLanguages.NQUADS)
    val tempFile = Files.createFile(tmpDir.resolve(f"${randomUUID}.${extension}"))
    try {
      testCode(tempFile.toString)
    } finally Files.deleteIfExists(tempFile)
  }

  override def beforeAll(): Unit = {
    if !Files.exists(tmpDir) then Files.createDirectory(tmpDir)
  }

  override def afterAll(): Unit = {
    Files.deleteIfExists(tmpDir)
  }
