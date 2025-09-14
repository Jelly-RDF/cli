package eu.neverblink.jelly.cli.util.jena

import org.apache.jena.graph.impl.LiteralLabel

import scala.util.Try

object JenaSystemOptions:
  /** Enable faster parsing by disabling strict literal validation.
    * @return
    *   A Success if the operation was successful, or a Failure with the exception if not. The
    *   operation may fail in environments where reflection is not supported. The failure can be
    *   ignored, but parsing will be slower.
    */
  def disableTermValidation(): Try[Unit] =
    toggle(false)

  /** For use only in tests.
    */
  def resetTermValidation(): Try[Unit] =
    toggle(true)

  private def toggle(enable: Boolean): Try[Unit] =
    val valueMode =
      if enable then "EAGER"
      else "LAZY"

    // Disable/enable eager computation of literal values, which does strict checking.
    // This requires reflection as the field is private static final.
    Try {
      val f = classOf[LiteralLabel].getDeclaredField("valueMode")
      val valueModeClass =
        classOf[LiteralLabel].getDeclaredClasses.find(_.getSimpleName == "ValueMode").get
      val valueModeLazy = valueModeClass.getDeclaredField(valueMode)
      valueModeLazy.setAccessible(true)
      f.setAccessible(true)
      f.set(null, valueModeLazy.get(null))
    }
