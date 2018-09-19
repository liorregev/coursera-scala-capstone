import java.io.{File, InputStream, OutputStream}
import java.util.zip.ZipInputStream

name := course.value ++ "-" ++ assignment.value

scalaVersion := "2.11.8"

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-encoding", "UTF-8",
  "-unchecked",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-value-discard",
  "-Xfuture",
  "-Xexperimental"
)

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)

libraryDependencies ++= Seq(
  "com.sksamuel.scrimage" %% "scrimage-core" % "2.1.6", // for visualization
  // You don’t *have to* use Spark, but in case you want to, we have added the dependency
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  // You don’t *have to* use akka-stream, but in case you want to, we have added the dependency
  "com.typesafe.akka" %% "akka-stream" % "2.4.12",
  "com.chuusai" %% "shapeless" % "2.3.3",
  "com.typesafe.akka" %% "akka-stream-testkit" % "2.4.12" % Test,
  // You don’t *have to* use Monix, but in case you want to, we have added the dependency
  "io.monix" %% "monix" % "2.1.1",
  // You don’t *have to* use fs2, but in case you want to, we have added the dependency
  "co.fs2" %% "fs2-io" % "0.9.2",
  "org.scalacheck" %% "scalacheck" % "1.12.1" % Test,
  "junit" % "junit" % "4.10" % Test
)

courseId := "PCO2sYdDEeW0iQ6RUMSWEQ"

parallelExecution in Test := false // So that tests are executed for each milestone, one after the other

def downloadResources(destDir: File): Seq[File] = {
  def transfer(in: InputStream, out: OutputStream): Unit = {
    val buffer = new Array[Byte](8192)
    def read()
    {
      val byteCount = in.read(buffer)
      if(byteCount >= 0)
      {
        out.write(buffer, 0, byteCount)
        read()
      }
    }
    read()
  }

  val resourcesFound = destDir.isDirectory && destDir.listFiles().length == 42
  if(!resourcesFound) {
    val dataUrl = new URL("http://alaska.epfl.ch/files/scala-capstone-data.zip")
    val inputStream = dataUrl.openStream()
    val zipStream = new ZipInputStream(inputStream)
    val newFiles = Stream
      .continually(Option(zipStream.getNextEntry))
      .takeWhile(_.isDefined)
      .flatten
      .map(_.getName)
      .filter(_.endsWith("csv"))
      .map(_.split('/').last)
      .map {
        entry =>
          val newFile = new File(destDir.getAbsolutePath + File.separator + entry)
          new File(newFile.getParent).mkdirs()
          Using.fileOutputStream()(newFile) { outputStream =>
            transfer(zipStream, outputStream)
          }
          zipStream.closeEntry()
          newFile
      }
        .force
    zipStream.close()
    inputStream.close()
    newFiles
  } else {
    Seq.empty[File]
  }
}

val resourceDownloader = taskKey[Seq[File]]("resourceDownloader")

resourceDownloader in Compile :=
  downloadResources( (unmanagedResourceDirectories in Compile).value.head)

resourceGenerators in Compile += (resourceDownloader in Compile).taskValue