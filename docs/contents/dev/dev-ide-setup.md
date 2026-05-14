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

Eclipse project generation through sbteclipse is no longer supported by the Gearpump build. Use the Intellij setup above for sbt-based development.
