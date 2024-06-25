import java.nio.file.{Files, StandardCopyOption}
import sbt.util

val orgName = "io.unitycatalog"
val artifactNamePrefix = "unitycatalog"

lazy val commonSettings = Seq(
  organization := orgName,
  // Compilation configs
  initialize := {
    // Assert that the JVM is at least Java 11
    val _ = initialize.value  // ensure previous initializations are run
    assert(
      sys.props("java.specification.version").toDouble >= 11,
      "Java 11 or above is required to run this project.")
  },
  javacOptions ++= Seq(
    "-Xlint:deprecation",
    "-Xlint:unchecked",
    "-source", "1.8",
    "-target", "1.8",
    "-g:source,lines,vars"
  ),
  Compile / logLevel := util.Level.Warn,
  resolvers += Resolver.mavenLocal,
  autoScalaLibrary := false,
  crossPaths := false,  // No scala cross building
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  },

  // Test configs
  Test / testOptions  := Seq(Tests.Argument(TestFrameworks.JUnit, "-a", "-v", "-q"), Tests.Filter(name => !(name startsWith s"$orgName.server.base"))),
  Test / logLevel := util.Level.Info,
  Test / publishArtifact := false,
  fork := true,
  outputStrategy := Some(StdoutOutput),

  Compile / packageBin := {
    val packageFile = (Compile / packageBin).value
    generateClasspathFile(
      targetDir = packageFile.getParentFile,
      classpath = (Runtime / dependencyClasspath).value)
    packageFile
  }
)

def javaCheckstyleSettings(configLocation: File) = Seq(
  checkstyleConfigLocation := CheckstyleConfigLocation.File(configLocation.toString),
  checkstyleSeverityLevel := Some(CheckstyleSeverityLevel.Error),
  // (Compile / compile) := ((Compile / compile) dependsOn (Compile / checkstyle)).value,
  // (Test / test) := ((Test / test) dependsOn (Test / checkstyle)).value,
)

lazy val client = (project in file("clients/java"))
  .enablePlugins(OpenApiGeneratorPlugin)
  .settings(
    name := s"$artifactNamePrefix-client",
    commonSettings,
    libraryDependencies ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jacksonVersion,
      "org.openapitools" % "jackson-databind-nullable" % openApiToolsJacksonBindNullableVersion,
      "com.google.code.findbugs" % "jsr305" % "3.0.2",
      "jakarta.annotation" % "jakarta.annotation-api" % "1.3.5" % Provided,

      // Test dependencies
      "junit" %  "junit" % "4.13.2" % Test,
      "org.junit.jupiter" % "junit-jupiter" % "5.9.2" % Test,
      "net.aichler" % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test,
    ),

    // OpenAPI generation specs
    openApiInputSpec := (file(".") / "api" / "all.yaml").toString,
    openApiGeneratorName := "java",
    openApiOutputDir := (file("clients") / "java").toString,
    openApiApiPackage := s"$orgName.client.api",
    openApiModelPackage := s"$orgName.client.model",
    openApiAdditionalProperties := Map(
      "library" -> "native",
      "hideGenerationTimestamp" -> "true"),
    openApiGenerateApiTests := SettingDisabled,
    openApiGenerateModelTests := SettingDisabled,
    openApiGenerateApiDocumentation := SettingDisabled,
    openApiGenerateModelDocumentation := SettingDisabled,
    // Define the simple generate command to generate full client codes
    generate := {
      val _ = openApiGenerate.value
    }
  )

lazy val apiDocs = (project in file("api"))
  .enablePlugins(OpenApiGeneratorPlugin)
  .settings(
    name := s"$artifactNamePrefix-docs",

    // OpenAPI generation specs
    openApiInputSpec := (file("api") / "all.yaml").toString,
    openApiGeneratorName := "markdown",
    openApiOutputDir := (file("api")).toString,
    // Define the simple generate command to generate markdown docs
    generate := {
      val _ = openApiGenerate.value
    }
  )


lazy val server = (project in file("server"))
  .dependsOn(client % "test->test")
  .enablePlugins(OpenApiGeneratorPlugin)
  .settings (
    name := s"$artifactNamePrefix-server",
    commonSettings,
    javaCheckstyleSettings(file("dev") / "checkstyle-config.xml"),
    libraryDependencies ++= Seq(
      "com.linecorp.armeria" %  "armeria" % "1.28.4",
      "javax.annotation" %  "javax.annotation-api" % "1.3.2",
      // Jackson dependencies
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jacksonVersion,

      "com.google.code.findbugs" % "jsr305" % "3.0.2",
      "com.h2database" %  "h2" % "2.2.224",
      "org.hibernate.orm" % "hibernate-core" % "6.5.0.Final",
      "org.openapitools" % "jackson-databind-nullable" % openApiToolsJacksonBindNullableVersion,
      // logging
      "org.apache.logging.log4j" % "log4j-api" % "2.23.1",
      "org.apache.logging.log4j" % "log4j-core" % "2.23.1",
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.23.1",

      "jakarta.activation" % "jakarta.activation-api" % "2.1.3",
      "net.bytebuddy" % "byte-buddy" % "1.14.15",
      "org.projectlombok" % "lombok" % "1.18.32" % "provided",

      //For s3 access
      "com.amazonaws" % "aws-java-sdk-s3" % "1.12.728",
      "org.apache.httpcomponents" % "httpcore" % "4.4.16",
      "org.apache.httpcomponents" % "httpclient" % "4.5.14",

      // Iceberg REST Catalog dependencies
      "org.apache.iceberg" % "iceberg-core" % "1.5.2",
      "io.vertx" % "vertx-core" % "4.3.5",
      "io.vertx" % "vertx-web" % "4.3.5",
      "io.vertx" % "vertx-web-client" % "4.3.5",

      // Test dependencies
      "junit" %  "junit" % "4.13.2" % Test,
      "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
    ),

    Compile / compile / javacOptions ++= Seq(
      "-processor",
      "lombok.launch.AnnotationProcessorHider$AnnotationProcessor"
    ),

    Compile / sourceGenerators += Def.task {
      val file = (Compile / sourceManaged).value / "io" / "unitycatalog" / "server" / "utils" / "VersionUtils.java"
      IO.write(file,
        s"""package io.unitycatalog.server.utils;
           |
           |public class VersionUtils {
           |  public static String VERSION = "${version.value}";
           |}
           |""".stripMargin)
      Seq(file)
    },

    // OpenAPI generation configs for generating model codes from the spec
    openApiInputSpec := (file(".") / "api" / "all.yaml").toString,
    openApiGeneratorName := "java",
    openApiOutputDir := serverOpenApiGenerateTempDir.toString,
    openApiValidateSpec := SettingEnabled,
    openApiGenerateMetadata := SettingDisabled,
    openApiModelPackage := s"$orgName.server.model",
    openApiAdditionalProperties := Map(
      "library" -> "resteasy",  // resteasy generates the most minimal models
      "hideGenerationTimestamp" -> "true"),
    openApiGlobalProperties := Map("models" -> ""),

    // Define the simple generate command to generate model codes and copy them into the server dir
    generate := {
      val _ = openApiGenerate.value
      val srcDir = (file(serverOpenApiGenerateTempDir.toString) / "src" / "main" / "java" / "io" / "unitycatalog" / "server" / "model" )
      val dstDir = file("server").getAbsoluteFile / "src" / "main" / "java" / "io" / "unitycatalog" / "server" /"model"
      println(s"Copying model files from $srcDir to $dstDir")

      srcDir.listFiles().foreach { srcFile =>
        Files.copy(srcFile.toPath, (dstDir / srcFile.getName).toPath, StandardCopyOption.REPLACE_EXISTING)
      }
    }
  )

lazy val cli = (project in file("examples") / "cli")
  .dependsOn(server % "compile->compile;test->test")
  .dependsOn(client % "compile->compile;test->test")
  .settings(
    name := s"$artifactNamePrefix-cli",
    mainClass := Some(orgName + ".cli.UnityCatalogCli"),
    commonSettings,
    javaCheckstyleSettings(file("dev") / "checkstyle-config.xml"),
    Compile / logLevel := util.Level.Info,
    libraryDependencies ++= Seq(
      "commons-cli" % "commons-cli" % "1.7.0",
      "org.json" % "json" % "20240303",
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jacksonVersion,
      "org.openapitools" % "jackson-databind-nullable" % openApiToolsJacksonBindNullableVersion,
      "org.yaml" % "snakeyaml" % "2.2",
      // logging
      "org.apache.logging.log4j" % "log4j-api" % "2.23.1",
      "org.apache.logging.log4j" % "log4j-core" % "2.23.1",
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.23.1",

      "io.delta" % "delta-kernel-api" % "3.2.0",
      "io.delta" % "delta-kernel-defaults" % "3.2.0",
      "io.delta" % "delta-storage" % "3.2.0",
      "org.apache.hadoop" % "hadoop-client-api" % "3.4.0",
      "org.apache.hadoop" % "hadoop-client-runtime" % "3.4.0",
      "de.vandermeer" % "asciitable" % "0.3.2",
      // for s3 access
      "com.amazonaws" % "aws-java-sdk-core" % "1.12.728",
      "org.apache.hadoop" % "hadoop-aws" % "3.4.0",
      "com.google.guava" % "guava" % "31.0.1-jre",
      // Test dependencies
      "junit" %  "junit" % "4.13.2" % Test,
      "com.github.sbt" % "junit-interface" % "0.13.3" % Test,
    ),
  )

// Extra functionalities
lazy val serverOpenApiGenerateTempDir = {
  import java.nio.file.Files
  Files.createTempDirectory("some-prefix")
}

def generateClasspathFile(targetDir: File, classpath: Classpath): Unit = {
  // Generate a classpath file with the entire runtime class path.
  // This is used by the launcher scripts for launching CLI directly with JAR instead of SBT.
  val classpathFile = targetDir / "classpath"
  Files.write(classpathFile.toPath, classpath.files.mkString(":").getBytes)
  println(s"Generated classpath file '$classpathFile'")
}

val generate = taskKey[Unit]("generate code from APIs")

// Library versions
val jacksonVersion = "2.17.0"
val openApiToolsJacksonBindNullableVersion = "0.2.6"
