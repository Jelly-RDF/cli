package eu.neverblink.jelly.cli.command

import caseapp.core.RemainingArgs
import eu.neverblink.jelly.cli.{CriticalException, ExitException, JellyCommand}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ErrorHandlerSpec extends AnyWordSpec, Matchers:
  def makeMockCommand(ex: Throwable): JellyCommand[VersionOptions] =
    val command = new JellyCommand[VersionOptions] {
      override def names: List[List[String]] = List(List("test"))
      override def doRun(options: VersionOptions, remainingArgs: RemainingArgs): Unit =
        throw ex
    }
    command.testMode(true)
    command

  "ErrorHandler" should {
    "not print stack trace for known exceptions if not in debug mode" in {
      val command = makeMockCommand(new CriticalException("Known error!"))
      intercept[ExitException] {
        command.main("test", Array())
      }
      val err = command.getErrString
      err should include("Known error!")
      err should include("Run with --debug to see the complete stack trace.")
    }

    "print stack trace for known exceptions in debug mode" in {
      val command = makeMockCommand(new CriticalException("Known error!"))
      intercept[ExitException] {
        command.main("test", Array("--debug"))
      }
      val err = command.getErrString
      err should include("Known error!")
      err should include("CriticalException")
      err should include("at eu.neverblink.jelly.cli.command.ErrorHandlerSpec")
    }

    "print stack trace for unknown exceptions if not in debug mode" in {
      val command = makeMockCommand(new RuntimeException("Unknown error!"))
      intercept[ExitException] {
        command.main("test", Array())
      }
      val err = command.getErrString
      err should include("Unknown error")
      err should include("RuntimeException")
      err should include("at eu.neverblink.jelly.cli.command.ErrorHandlerSpec")
    }

    "print stack trace for unknown exceptions in debug mode" in {
      val command = makeMockCommand(new RuntimeException("Unknown error!"))
      intercept[ExitException] {
        command.main("test", Array("--debug"))
      }
      val err = command.getErrString
      err should include("Unknown error")
      err should include("RuntimeException")
      err should include("at eu.neverblink.jelly.cli.command.ErrorHandlerSpec")
    }
  }
