package eu.neverblink.jelly.cli.util.args

import eu.neverblink.jelly.cli.InvalidArgument
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class IndexRangeSpec extends AnyWordSpec, Matchers:
  "IndexRange" should {
    "parse a single index" in {
      IndexRange("1") should be(IndexRange(Some(1), Some(2)))
    }

    "parse a range" in {
      IndexRange("1..3") should be(IndexRange(Some(1), Some(3)))
    }

    "parse a range with inclusive end" in {
      IndexRange("1..=3") should be(IndexRange(Some(1), Some(4)))
    }

    "parse a range without start" in {
      IndexRange("..3") should be(IndexRange(None, Some(3)))
    }

    "parse a range without end" in {
      IndexRange("1..") should be(IndexRange(Some(1), None))
    }

    "parse a range with empty start and end" in {
      IndexRange("..") should be(IndexRange(None, None))
    }

    "parse a range with empty start and end with equals" in {
      IndexRange("..=") should be(IndexRange(None, None))
    }

    "parse a range with empty start and end with equals and no end" in {
      IndexRange("..=3") should be(IndexRange(None, Some(4)))
    }

    "parse an empty string" in {
      IndexRange("") should be(IndexRange(None, None))
    }

    "not parse an invalid range (a..b)" in {
      val e = intercept[InvalidArgument] {
        IndexRange("a..b")
      }
      e.argument should be ("--range")
      e.argumentValue should be ("a..b")
      e.message.get should include ("Correct ranges are in the form")
    }

    "not parse an invalid range (123a)" in {
      val e = intercept[InvalidArgument] {
        IndexRange("123a")
      }
      e.argument should be ("--range")
      e.argumentValue should be ("123a")
      e.message.get should include ("Correct ranges are in the form")
    }

    "not parse an invalid range (asdad)" in {
      val e = intercept[InvalidArgument] {
        IndexRange("asdad")
      }
      e.argument should be ("--range")
      e.argumentValue should be ("asdad")
      e.message.get should include ("Correct ranges are in the form")
    }

    "slice a collection" in {
      val range = IndexRange(Some(1), Some(3))
      val collection = Seq(0, 1, 2, 3, 4)
      range.slice(collection) should be (Seq(1, 2))
    }
  }
