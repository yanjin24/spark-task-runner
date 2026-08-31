package io.github.yanjin24.sparktaskrunner

import java.io.{File, PrintWriter}
import java.net.URLClassLoader
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import scala.tools.nsc.{Global, Settings}
import scala.tools.nsc.reporters.StoreReporter

/**
 * Scala 脚本运行器（动态文件 + 运行时编译方案）。
 *
 * 读取 .scala 脚本（本地路径或 HDFS URI），在 driver 端用 scala-compiler 运行时编译为 class，
 * 通过 Spark 的 REPL class server（spark.repl.class.outputDir）把类分发给 executor，再反射调用脚本主入口执行。
 *
 * 为何用 REPL class server 而非 sc.addJar：
 *   spark.udf.register 注册的 UDF 会被 Spark 包成 ScalaUDF.f（一个行解码 Function1），
 *   该包装是 invokedynamic lambda，序列化为 SerializedLambda 后在 executor 反序列化时
 *   需要能加载到 capturing class。sc.addJar 的 jar 在 executor 子 classloader、反序列化路径取不到，
 *   导致 ClassCastException: SerializedLambda -> Function1。
 *   REPL class server（spark-shell 用的同一套机制）让 executor 经 spark.repl.class.uri 拉取运行时类，
 *   从而正确反序列化。这也是 spark-shell 里能直接用 UDF 的原因。
 */
object SparkScalaRunner {

  /** 脚本被包装后的全限定类名（package runner，object Script）。 */
  private val ScriptClass = "runner.Script"

  /** 包装代码在脚本内容前加入的行数，用于把编译错误行号映射回脚本原文行号。 */
  private val WrapLineOffset = 3

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("用法: spark-submit --class io.github.yanjin24.sparktaskrunner.SparkScalaRunner <jar> <scala脚本路径> [参数...]")
      sys.exit(1)
    }
    val scriptPath = args(0)
    val scriptArgs: Array[String] = args.drop(1)

    // 运行时编译依赖 scala-compiler，需在集群 spark-jars 中可用，先做检测以便给出明确错误。
    try {
      Class.forName("scala.tools.nsc.Main")
    } catch {
      case _: ClassNotFoundException =>
        System.err.println(
          "找不到 scala-compiler (scala.tools.nsc.Main)。\n" +
            "本运行器在 driver 端运行时编译 Scala 脚本，依赖 scala-compiler。\n" +
            "请将 scala-compiler-2.13.x.jar（以及 scala-reflect、scala-library）加入集群的 spark-jars 后重新提交。\n" +
            "这些 jar 可在 Spark 发行版的 jars/ 目录中找到，版本需与 spark-submit 显示的 Scala 版本一致。"
        )
        sys.exit(1)
    }

    val outDir = Files.createTempDirectory("spark-script-out-").toFile
    val srcFile = Files.createTempFile("spark-script-", ".scala").toFile

    // 1. 先创建 SparkSession，开启 REPL class server 指向 outDir。
    //    SparkContext 检测到 spark.repl.class.outputDir 后会启动 class server 并设置
    //    spark.repl.class.uri，executor 通过该 uri 拉取运行时编译的类（含 UDF 闭包）。
    val spark = SparkSession.builder()
      .appName("Spark Scala Runner")
      .config("spark.repl.class.outputDir", outDir.getAbsolutePath)
      .getOrCreate()

    val replUri = spark.sparkContext.getConf.getOption("spark.repl.class.uri").getOrElse("(未设置)")
    println("REPL class outputDir: " + outDir.getAbsolutePath)
    println("REPL class uri: " + replUri)

    // 用 failed 标记代替在 catch 里 sys.exit，保证 finally 中的临时目录清理能执行。
    var failed = false

    try {
      // 2. 读取脚本。HDFS 读取走 Spark 的 hadoopConfiguration，继承 --conf / Kerberos 等配置。
      val source = ScriptFileReader.read(scriptPath, spark.sparkContext.hadoopConfiguration)
      println("Scala脚本: " + scriptPath)

      // 3. 包装 + 运行时编译到 outDir（直接构造 Global，显式 delambdafy=inline）
      val pw = new PrintWriter(srcFile, StandardCharsets.UTF_8.name)
      try pw.write(wrapScript(source)) finally pw.close()

      println("正在编译 Scala 脚本...")
      val classpath = System.getProperty("java.class.path")
      val settings = new Settings
      settings.classpath.value = classpath
      settings.outputDirs.setSingleOutput(outDir.getAbsolutePath)
      // 强制匿名类（$anonfun），让脚本里的 UDF 闭包以可加载的 class 形式存在。
      settings.Ydelambdafy.value = "inline"
      println("  delambdafy = " + settings.Ydelambdafy.value)
      // StoreReporter 收集诊断后统一打印，以便把行号映射回脚本原文（ConsoleReporter 会立即输出，无法映射）。
      val reporter = new StoreReporter
      val g = new Global(settings, reporter)
      val run = new g.Run
      run.compile(List(srcFile.getAbsolutePath))
      if (reporter.hasErrors) {
        printDiagnostics(reporter)
        failed = true
      } else {
        println("编译成功。")
        val classes = listClasses(outDir)
        println("编译产物 (" + classes.length + " 个 class): " + classes.mkString(", "))

        // 4. 反射调用 runner.Script.main(scriptArgs)（driver 端从 outDir 加载）
        println("执行脚本...")
        val loader = new URLClassLoader(Array(outDir.toURI.toURL), getClass.getClassLoader)
        val scriptClass = loader.loadClass(ScriptClass)
        val mainMethod = scriptClass.getMethod("main", classOf[Array[String]])
        mainMethod.invoke(null, scriptArgs.asInstanceOf[AnyRef])
        println("脚本执行完成！")
      }
    } catch {
      case e: java.lang.reflect.InvocationTargetException =>
        val cause = Option(e.getCause).getOrElse(e)
        System.err.println("脚本执行失败: " + cause.getMessage)
        cause.printStackTrace()
        failed = true
      case e: Throwable =>
        System.err.println("执行失败: " + e.getMessage)
        e.printStackTrace()
        failed = true
    } finally {
      spark.stop()
      // 临时文件清理：executor 已在执行期间从 REPL class server 拉取完类，driver 停止后可安全删除。
      srcFile.delete()
      deleteRecursively(outDir)
    }

    if (failed) sys.exit(1)
  }

  /**
   * 将脚本包进 object Script 的 main 方法。
   * Scala 允许在方法体内写 import，因此脚本顶部的 import 无需特殊处理。
   * 用拼接而非插值，避免脚本中的 $ 被误解析。
   * 注意：包装在脚本内容前加了 WrapLineOffset 行，编译错误行号需据此映射回脚本原文。
   */
  private def wrapScript(source: String): String = {
    "package runner\n" +
      "object Script {\n" +
      "  def main(args: Array[String]): Unit = {\n" +
      source + "\n" +
      "  }\n" +
      "}\n"
  }

  /** 打印编译诊断，行号映射回脚本原文（包装代码内的错误按原始行号展示）。 */
  private def printDiagnostics(reporter: StoreReporter): Unit = {
    System.err.println("脚本编译失败，错误信息如下（行号已映射回脚本原文）：")
    reporter.infos.foreach { info =>
      val loc =
        if (info.pos.isDefined && info.pos.line > WrapLineOffset) "脚本第 " + (info.pos.line - WrapLineOffset) + " 行"
        else if (info.pos.isDefined) "运行器包装代码第 " + info.pos.line + " 行"
        else "未知位置"
      System.err.println("[" + loc + "] " + info.severity + ": " + info.msg)
    }
  }

  /** 递归删除文件/目录。 */
  private def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles).foreach(_.foreach(deleteRecursively))
    f.delete()
  }

  /** 列出目录下所有 .class 文件的相对路径（调试用）。 */
  private def listClasses(dir: File): Array[String] = {
    val root = dir.getAbsolutePath
    val buf = scala.collection.mutable.ArrayBuffer.empty[String]
    def walk(f: File): Unit = {
      if (f.isFile && f.getName.endsWith(".class")) {
        buf += f.getAbsolutePath.substring(root.length + 1).replace('\\', '/')
      } else if (f.isDirectory) {
        val cs = f.listFiles()
        if (cs != null) cs.foreach(walk)
      }
    }
    walk(dir)
    buf.toArray
  }
}
