package eu.neverblink.jelly.cli.command.rdf

import eu.neverblink.jelly.cli.{
  InputFileInaccessible,
  InputFileNotFound,
  OutputFileCannotBeCreated,
  OutputFileExists,
}

import java.io.{File, FileInputStream, FileOutputStream}

/*
 * Object for small, repeating operations with proper error handling
 */
object Ops:

  /** Read input file and return FileInputStream
    * @param fileName
    * @throws InputFileNotFound
    * @throws InputFileInaccessible
    * @return
    */
  def readInputFile(fileName: String): FileInputStream =
    val file = File(fileName)
    if !file.exists then throw InputFileNotFound(fileName)
    if !file.canRead then throw InputFileInaccessible(fileName)
    FileInputStream(file)

  /** Create output stream with extra error handling
    * @param fileName
    * @throws OutputFileExists
    * @return
    */
  def createOutputStream(fileName: String): FileOutputStream =
    val file = File(fileName)
    if file.exists then throw OutputFileExists(fileName)
    if !file.getParentFile.canWrite then throw OutputFileCannotBeCreated(fileName)
    FileOutputStream(file)
