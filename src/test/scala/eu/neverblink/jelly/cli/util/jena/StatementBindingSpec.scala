package eu.neverblink.jelly.cli.util.jena

import eu.neverblink.jelly.cli.command.helpers.TestFixtureHelper
import eu.neverblink.jelly.cli.util.jena.StatementBinding.*
import org.apache.jena.graph.NodeFactory
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.{Binding, BindingFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.*

class StatementBindingSpec extends AnyWordSpec with TestFixtureHelper with Matchers:
  // Not used by these tests
  protected val testCardinality: Int = 0

  private val s = NodeFactory.createURI("http://example.org/s")
  private val p = NodeFactory.createURI("http://example.org/p")
  private val o = NodeFactory.createLiteralString("o")
  private val g = NodeFactory.createURI("http://example.org/g")

  private def forEachPairs(b: Binding) =
    val pairs = ArrayBuffer[(Var, AnyRef)]()
    b.forEach((v, n) => pairs += (v -> n))
    pairs.toSeq

  "TripleBinding" should {
    val binding = TripleBinding(s, p, o)

    "hold ?s ?p ?o, in that order" in {
      binding.vars().asScala.toSeq shouldBe tripleVars
      forEachPairs(binding) shouldBe Seq(subject -> s, predicate -> p, `object` -> o)
      binding.size() shouldBe 3
      binding.isEmpty shouldBe false
    }

    "find the values by variables that are not the same objects" in {
      binding.get(Var.alloc("s")) shouldBe s
      binding.get(Var.alloc("p")) shouldBe p
      binding.get(Var.alloc("o")) shouldBe o
    }

    "not hold ?g or any other variable" in {
      binding.get(graph) shouldBe null
      binding.contains(graph) shouldBe false
      binding.get(Var.alloc("x")) shouldBe null
    }

    "be equal to Jena's own binding" in {
      val jena = BindingFactory.binding(subject, s, predicate, p, `object`, o)
      binding shouldBe jena
      jena shouldBe binding
      binding.hashCode shouldBe jena.hashCode
      binding.detach() shouldBe jena
      binding.varsMentioned().asScala shouldBe jena.varsMentioned().asScala
      binding.toString shouldBe jena.toString
    }
  }

  "QuadBinding" should {
    val binding = QuadBinding(s, p, o, g)

    "hold ?s ?p ?o ?g, in that order" in {
      binding.vars().asScala.toSeq shouldBe quadVars
      forEachPairs(binding) shouldBe
        Seq(subject -> s, predicate -> p, `object` -> o, graph -> g)
      binding.size() shouldBe 4
    }

    "find the values by variables that are not the same objects" in {
      binding.get(Var.alloc("g")) shouldBe g
      binding.contains(Var.alloc("g")) shouldBe true
      binding.get(Var.alloc("x")) shouldBe null
    }

    "be equal to Jena's own binding" in {
      val jena = BindingFactory.binding(subject, s, predicate, p, `object`, o, graph, g)
      binding shouldBe jena
      jena shouldBe binding
      binding.hashCode shouldBe jena.hashCode
      binding.detach() shouldBe jena
      binding.varsMentioned().asScala shouldBe jena.varsMentioned().asScala
      binding.toString shouldBe jena.toString
    }
  }
