package eu.neverblink.jelly.cli.util.io

import eu.neverblink.jelly.cli.*

import java.io.{File, FileInputStream, FileOutputStream}

/** Object for small, repeating I/O operations
  */
object IoUtil:

  /** Read input file and return FileInputStream
    * @param fileName
    * @throws InputFileNotFound
    * @throws InputFileInaccessible
    * @return
    *   FileInputStream
    */
  def inputStream(fileName: String): FileInputStream =
    val file = File(fileName)
    if !file.exists then throw InputFileNotFound(fileName)
    if !file.canRead then throw InputFileInaccessible(fileName)
    FileInputStream(file)

  /** Create output stream with extra error handling. If the file exists, it will append to it.
    * @param fileName
    * @throws OutputFileExists
    * @return
    *   FileOutputStream
    */
  def outputStream(fileName: String): FileOutputStream =
    val file = File(fileName)
    val suppFile = file.getParentFile
    val parentFile = if (suppFile != null) suppFile else File(".")
    if !parentFile.canWrite || (file.exists() && !file.canWrite) then
      throw OutputFileCannotBeCreated(fileName)
    FileOutputStream(file, true)
