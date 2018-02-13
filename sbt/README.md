### Integrate sbt with Scala-IDE
#### Scenario: We use Scala-IDE to develop scala program, and we also need sbt to manage dependency of libraries, we process the following steps for this requirement:
1. create project folder and create build.sbt within project folder
2. use sbteclipse to create scala project for Scala-IDE and download related jars according to build.sbt 
3. open Scala-IDE and use `import -> General -> Project from Folder or Archive` to import sbt project
4. enjoy Scala-IDE...

#### How install and config
* sbt installation on ubuntu: https://www.scala-sbt.org/1.0/docs/zh-cn/Installing-sbt-on-Linux.html
* use `sbt about` or `sbt version` command to check version of sbt
* download Scala-IDE: http://scala-ide.org/
* config sbteclipse: https://github.com/sbt/sbteclipse
  * if sbt version is not 0.13, sbt will not load plugins from ~/.sbt/0.13/plugins/plugins.sbt, and `sbt eclipse` will cause error:
    <pre><code>
    [error] Not a valid command: eclipse (similar: client, help, alias)
    [error] Not a valid project ID: eclipse
    [error] Expected ':'
    [error] Not a valid key: eclipse (similar: deliver, licenses, clean)
    [error] eclipse
    [error]        ^
    </code></pre>
  * so we need to create plugins.sbt into project folder within scala project folder by following command:
    <pre><code> echo 'addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "5.2.4")' > project/plugins.sbt
           (if project folder not exist, create it first)</code></pre>
  * then use `sbt` command to enter sbt environment, and use `eclipse` command to create eclipse project 
 
