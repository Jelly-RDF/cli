package eu.neverblink.jelly.cli.command.rdf

import java.io.{File, FileInputStream}

/*
 * Object for small, repeating operations with proper error handling
 */
object Ops:

  @throws[IllegalArgumentException]
  def readFile(fileName: String): FileInputStream =
    val file = File(fileName)
    FileInputStream(file)
