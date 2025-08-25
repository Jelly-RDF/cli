package eu.neverblink.jelly.cli.graal

import org.apache.jena.graph.impl.LiteralLabel
import org.graalvm.nativeimage.hosted.{Feature, RuntimeReflection}

class JenaInternalsFeature extends Feature:
  import Feature.*

  override def getDescription: String =
    "Registers Jena internals for reflection. Needed for JenaSystemOptions to disable a few " +
      "checks during RDF parsing."

  override def beforeAnalysis(access: BeforeAnalysisAccess): Unit =
    val classes = classOf[LiteralLabel].getDeclaredClasses
    val valueModeClass = classes.find(_.getSimpleName == "ValueMode").get
    RuntimeReflection.register(valueModeClass)
    RuntimeReflection.register(valueModeClass.getDeclaredField("LAZY"))
