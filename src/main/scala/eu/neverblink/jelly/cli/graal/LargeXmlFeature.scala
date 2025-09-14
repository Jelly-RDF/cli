package eu.neverblink.jelly.cli.graal

import org.graalvm.nativeimage.hosted.{Feature, RuntimeSystemProperties}

class LargeXmlFeature extends Feature:
  import Feature.*

  override def getDescription: String =
    "Increases XML parsing limits to support large RDF/XML files."

  override def beforeAnalysis(access: BeforeAnalysisAccess): Unit =
    // Support arbitrarily large RDF/XML files – needed since JDK 24.
    // 0 indicates no limit.
    // Issue: https://github.com/Jelly-RDF/cli/issues/220
    RuntimeSystemProperties.register("jdk.xml.maxGeneralEntitySizeLimit", "0")
    RuntimeSystemProperties.register("jdk.xml.totalEntitySizeLimit", "0")
