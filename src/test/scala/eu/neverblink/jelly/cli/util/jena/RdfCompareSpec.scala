package eu.neverblink.jelly.cli.util.jena

import eu.neverblink.jelly.cli.CriticalException
import eu.ostrzyciel.jelly.core.NamespaceDeclaration
import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.sparql.core.Quad
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RdfCompareSpec extends AnyWordSpec, Matchers:
  // Test triples and quads
  private val t_iri_1 = Triple.create(
    NodeFactory.createURI("http://example.com/s"),
    NodeFactory.createURI("http://example.com/p"),
    NodeFactory.createURI("http://example.com/o"),
  )
  private val t_iri_2 = Triple.create(
    NodeFactory.createURI("http://example.com/s2"),
    NodeFactory.createURI("http://example.com/p"),
    NodeFactory.createURI("http://example.com/o"),
  )
  private val t_bnode_1 = Triple.create(
    NodeFactory.createBlankNode("b1"),
    NodeFactory.createURI("http://example.com/p"),
    NodeFactory.createURI("http://example.com/o"),
  )
  private val t_bnode_2 = Triple.create(
    NodeFactory.createBlankNode("b2"),
    NodeFactory.createURI("http://example.com/p"),
    NodeFactory.createURI("http://example.com/o"),
  )
  private val q_bnode_1 = Quad.create(
    NodeFactory.createURI("http://example.com/g"),
    NodeFactory.createBlankNode("b1"),
    NodeFactory.createURI("http://example.com/p"),
    NodeFactory.createURI("http://example.com/o"),
  )
  private val q_bnode_2 = Quad.create(
    NodeFactory.createURI("http://example.com/g"),
    NodeFactory.createBlankNode("b2"),
    NodeFactory.createURI("http://example.com/p"),
    NodeFactory.createURI("http://example.com/o"),
  )
  private val q_bnode_3 = Quad.create(
    NodeFactory.createURI("http://example.com/g2"),
    NodeFactory.createBlankNode("b1"),
    NodeFactory.createURI("http://example.com/p"),
    NodeFactory.createURI("http://example.com/o"),
  )
  private val ns1 = NamespaceDeclaration("ex", "http://example.com/")
  private val ns2 = NamespaceDeclaration("ex2", "http://example.com/")

  val c1: StreamRdfCollector = StreamRdfCollector()
  c1.prefix(ns1.prefix, ns1.iri)
  c1.triple(t_iri_1)
  c1.triple(t_bnode_1)
  c1.quad(q_bnode_1)

  val c2: StreamRdfCollector = StreamRdfCollector()
  c2.prefix(ns1.prefix, ns1.iri)
  c2.triple(t_iri_1)
  c2.triple(t_bnode_2)
  c2.quad(q_bnode_2)

  "OrderedRdfCompare" should {
    "match identical streams (identity)" in {
      OrderedRdfCompare.compare(c1, c1)
    }

    "match identical streams (same data)" in {
      val c1a = StreamRdfCollector()
      c1a.prefix(ns1.prefix, ns1.iri)
      c1a.triple(t_iri_1)
      c1a.triple(t_bnode_1)
      c1a.quad(q_bnode_1)
      OrderedRdfCompare.compare(c1, c1a)
    }

    "match streams with differing blank node IDs" in {
      OrderedRdfCompare.compare(c1, c2)
    }

    "not match streams with different namespace declarations" in {
      val c3 = StreamRdfCollector()
      c3.prefix(ns2.prefix, ns2.iri)
      c3.triple(t_iri_1)
      c3.triple(t_bnode_1)
      c3.quad(q_bnode_1)
      val e = intercept[CriticalException] {
        OrderedRdfCompare.compare(c1, c3)
      }
      e.getMessage should include("RDF element 0 is different: expected")
    }

    "not match streams with missing namespace declarations" in {
      val c3 = StreamRdfCollector()
      c3.triple(t_iri_1)
      c3.triple(t_bnode_1)
      c3.quad(q_bnode_1)
      val e = intercept[CriticalException] {
        OrderedRdfCompare.compare(c1, c3)
      }
      e.getMessage should include("Expected 4 RDF elements, but got 3 elements")
    }

    "not match streams with reordered namespace declarations" in {
      val c3 = StreamRdfCollector()
      c3.triple(t_iri_1)
      c3.prefix(ns1.prefix, ns1.iri)
      c3.triple(t_bnode_1)
      c3.quad(q_bnode_1)
      val e = intercept[CriticalException] {
        OrderedRdfCompare.compare(c1, c3)
      }
      e.getMessage should include("RDF element 0 is of different type: expected")
    }

    "not match streams with conflicting blank node mappings" in {
      val c3 = StreamRdfCollector()
      c3.prefix(ns1.prefix, ns1.iri)
      c3.triple(t_iri_1)
      c3.triple(t_bnode_1)
      c3.quad(q_bnode_2)
      val e = intercept[CriticalException] {
        OrderedRdfCompare.compare(c1, c3)
      }
      e.getMessage should include("RDF element 3 is different in subject term: expected")
      e.getMessage should include("b1 is already mapped to b1")
    }

    "not match streams with different IRIs" in {
      val c3 = StreamRdfCollector()
      c3.prefix(ns1.prefix, ns1.iri)
      c3.triple(t_iri_2)
      c3.triple(t_bnode_1)
      c3.quad(q_bnode_1)
      val e = intercept[CriticalException] {
        OrderedRdfCompare.compare(c1, c3)
      }
      e.getMessage should include("RDF element 1 is different in subject term: expected")
    }
  }

  "UnorderedRdfCompare" should {
    "match identical streams (identity)" in {
      UnorderedRdfCompare.compare(c1, c1)
    }

    "match identical streams (same data)" in {
      val c1a = StreamRdfCollector()
      c1a.prefix(ns1.prefix, ns1.iri)
      c1a.triple(t_iri_1)
      c1a.triple(t_bnode_1)
      c1a.quad(q_bnode_1)
      UnorderedRdfCompare.compare(c1, c1a)
    }

    "match streams with differing blank node IDs" in {
      UnorderedRdfCompare.compare(c1, c2)
    }

    "not match streams with a missing named graph" in {
      val c3 = StreamRdfCollector()
      c3.prefix(ns1.prefix, ns1.iri)
      c3.triple(t_iri_1)
      c3.triple(t_bnode_1)
      val e = intercept[CriticalException] {
        UnorderedRdfCompare.compare(c1, c3)
      }
      e.getMessage should include(
        "Expected 1 named graph(s), but got 0",
      )
    }

    "not match streams with different graph names" in {
      val c3 = StreamRdfCollector()
      c3.prefix(ns1.prefix, ns1.iri)
      c3.triple(t_iri_1)
      c3.triple(t_bnode_1)
      c3.quad(q_bnode_3)
      val e = intercept[CriticalException] {
        UnorderedRdfCompare.compare(c1, c3)
      }
      e.getMessage should include(
        "Named graph http://example.com/g is missing in the actual dataset",
      )
    }

    "not match streams with a different default graph" in {
      val c3 = StreamRdfCollector()
      c3.prefix(ns1.prefix, ns1.iri)
      c3.triple(t_iri_1)
      c3.quad(q_bnode_2)
      val e = intercept[CriticalException] {
        UnorderedRdfCompare.compare(c1, c3)
      }
      e.getMessage should include(
        "Default graph is not isomorphic with the expected one",
      )
    }

    "not match streams with different contents of a named graph" in {
      val c3 = StreamRdfCollector()
      c3.prefix(ns1.prefix, ns1.iri)
      c3.triple(t_iri_1)
      c3.triple(t_bnode_1)
      c3.quad(q_bnode_1)
      c3.quad(q_bnode_2)
      val e = intercept[CriticalException] {
        UnorderedRdfCompare.compare(c1, c3)
      }
      e.getMessage should include(
        "Named graph http://example.com/g is not isomorphic with the expected one",
      )
    }
  }
