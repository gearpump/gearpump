### Intellij IDE Setup

1. In Intellij, download scala plugin.  We are using scala version {{SCALA_BINARY_VERSION}}
2. Open menu "File->Open" to open Gearpump root project, then choose the Gearpump source folder.
3. All set.

**NOTE:** Intellij Scala plugin is already bundled with sbt. If you have Scala plugin installed, please don't install additional sbt plugin. Check your settings at "Settings -> Plugins"
**NOTE:** If you are behind a proxy, to speed up the build, please set the proxy for sbt in "Settings -> Build Tools > SBT". in input field "VM parameters", add 

	:::bash
	-Dhttp.proxyHost=<proxy host>
	-Dhttp.proxyPort=<port like 911>
	-Dhttps.proxyHost=<proxy host>
	-Dhttps.proxyPort=<port like 911>
	

### Eclipse IDE Setup

I will show how to do this in eclipse LUNA.

There is a sbt-eclipse plugin to generate eclipse project files, but seems there are some bugs, and some manual fix is still required. Here is the steps that works for me:

1. Install latest version eclipse luna
2. Install latest scala-IDE http://scala-ide.org/download/current.html   I use update site address: http://download.scala-ide.org/sdk/lithium/e44/scala211/stable/site
3. Open a sbt shell under the root folder of Gearpump. enter "eclipse", then we get all eclipse project file generated.
4. Use eclipse import wizard. File->Import->Existing projects into Workspace, make sure to tick the option "Search for nested projects"
5. Then it may starts to complain about encoding error, like "IO error while decoding". You need to fix the eclipse default text encoding by changing configuration at "Window->Preference->General->Workspace->Text file encoding" to UTF-8.
6. Then the project gearpump-external-kafka may still cannot compile. The reason is that there is some dependencies missing in generated .classpath file by sbt-eclipse. We need to do some manual fix. Right click on project icon of gearpump-external-kafka in eclipse, then choose menu "Build Path->Configure Build Path". A window will popup. Under the tab "projects", click add, choose "gearpump-streaming"
7. All set. Now the project should compile OK in eclipse.
