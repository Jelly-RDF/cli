package eu.neverblink.jelly.cli.graal

import org.graalvm.nativeimage.hosted.{Feature, RuntimeReflection}
import org.reflections.Reflections
import org.reflections.scanners.Scanners

import scala.jdk.CollectionConverters.*

class ProtobufFeature extends Feature:
  import Feature.*

  override def getDescription: String =
    "Registers Google-style Protobuf classes for reflection. Needed for jelly-text support."

  override def beforeAnalysis(access: BeforeAnalysisAccess): Unit =
    val reflections = Reflections("eu.neverblink.jelly.core.proto.google.v1", Scanners.SubTypes)
    val classes = Seq(
      classOf[com.google.protobuf.DescriptorProtos.FieldOptions],
    ) ++
      reflections.getSubTypesOf(classOf[com.google.protobuf.GeneratedMessage]).asScala ++
      reflections.getSubTypesOf(classOf[com.google.protobuf.GeneratedMessage.Builder[?]]).asScala ++
      reflections.getSubTypesOf(classOf[com.google.protobuf.ProtocolMessageEnum]).asScala
    classes.foreach(clazz => {
      RuntimeReflection.register(clazz)
      RuntimeReflection.register(clazz.getDeclaredMethods*)
    })
