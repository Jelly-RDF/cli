package eu.neverblink.jelly.cli.command.helpers

import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFLanguages}

import java.io.FileOutputStream
import java.nio.file.Files

trait TestFixtureHelper:

  protected val dHelper: DataGenHelper

  val testCardinality: Integer = 33

  private def getFileExtension(format: Lang = RDFLanguages.NQUADS): String =
    format.getFileExtensions.get(0)

  def withFullQuadFile(testCode: (String) => Any): Unit = {
    val extension = getFileExtension(RDFLanguages.NQUADS)
    val tempFile = Files.createTempFile("test", extension)
    val model = dHelper.generateTripleModel(testCardinality)
    RDFDataMgr.write(new FileOutputStream(tempFile.toFile), model, RDFLanguages.NQUADS)
    try {
      testCode(tempFile.toString)
    } finally Files.deleteIfExists(tempFile)
  }

  def withEmptyJellyFile(testCode: (String) => Any): Unit = {
    val extension = getFileExtension(JellyLanguage.JELLY)
    val tempFile = Files.createTempFile("test", extension)
    try {
      testCode(tempFile.toString)
    } finally Files.deleteIfExists(tempFile)
  }
