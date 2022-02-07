/*
 * Copyright (c) 2020 by Andrew Charneski.
 *
 * The author licenses this file to you under the
 * Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.simiacryptus.mindseye.art.examples

import java.io.PrintStream
import java.net.URI

import ammonite.Main
import ammonite.compiler.DefaultCodeWrapper
import ammonite.main.{Config, Defaults}
import ammonite.runtime.Storage
import ammonite.util.{Bind, Colors}
import com.amazonaws.services.s3.AmazonS3
import com.simiacryptus.aws.S3Util
import com.simiacryptus.mindseye.art.util._
import com.simiacryptus.notebook.NotebookOutput
import com.simiacryptus.sparkbook._
import com.simiacryptus.sparkbook.util.LocalRunner
import mainargs.Flag

object REPLNotebook extends REPLNotebook with LocalRunner[Object] with NotebookRunner[Object] {
  override def http_port: Int = 1081
}

class REPLNotebook extends ArtSetup[Object, REPLNotebook] {

  val s3bucket: String = "test.deepartist.org"

  override def indexStr = "000"

  override def description = <div>
    Interactive REPL Session, documented in a notebook
  </div>.toString.trim

  override def inputTimeoutSeconds = 3600

  override def postConfigure(log: NotebookOutput) = {
    implicit val l = log
    implicit val s3client: AmazonS3 = S3Util.getS3(log.getArchiveHome)
    // First, basic configuration so we publish to our s3 site
    if (Option(s3bucket).filter(!_.isEmpty).isDefined)
      log.setArchiveHome(URI.create(s"s3://$s3bucket/$className/${log.getId}/"))
    log.onComplete(() => upload(log): Unit)

    val cliConfig = Config(
      core = Config.Core(
        noDefaultPredef = Flag(false),
        silent = Flag(false),
        watch = Flag(false),
        bsp = Flag(false),
        thin = Flag(false),
        help = Flag(false),
        showVersion = Flag(false)
      ),
      predef = Config.Predef(
        noHomePredef = Flag(false)
      ),
      repl = Config.Repl(
        noRemoteLogging = Flag(false),
        classBased = Flag(false),
        banner = "DeepArtist.org MindsEye REPL"
      ))

    val storage = new Storage.Folder(Defaults.ammoniteHome)
    Main(
      cliConfig.predef.predefCode,
      cliConfig.core.predefFile,
      !cliConfig.core.noDefaultPredef.value,
      storage,
      wd = os.pwd,
      inputStream = System.in,
      outputStream = System.out,
      errorStream = System.err,
      welcomeBanner = cliConfig.repl.banner match {
        case "" => None
        case s => Some(s)
      },
      verboseOutput = !cliConfig.core.silent.value,
      remoteLogging = !cliConfig.repl.noRemoteLogging.value,
      colors = Colors.Default,
      replCodeWrapper = DefaultCodeWrapper,
      scriptCodeWrapper = DefaultCodeWrapper,
      parser = () => ammonite.compiler.Parsers,
      alreadyLoadedDependencies = Defaults.alreadyLoadedDependencies(),
      classPathWhitelist = ammonite.repl.Repl.getClassPathWhitelist(cliConfig.core.thin.value)
    ).run(
      Bind("log",log)
    )


    null
  }
}
