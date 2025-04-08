package eu.neverblink.jelly.cli.util.jena

import eu.ostrzyciel.jelly.core.NamespaceDeclaration
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.core.Quad

type RdfElement = Triple | Quad | NamespaceDeclaration
