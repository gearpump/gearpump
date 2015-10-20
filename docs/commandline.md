---
layout: global
displayTitle: Gearpump Client Commandline
title: Gearpump Client Commandline
description: Gearpump Client Commandline
---

The commands can be found at: "bin" folder of Gearpump binary.

**NOTE:** on MS Windows platform, please use window shell gear.bat script instead. bash script doesn't work well in cygwin/mingw.

### Submit an new application

You can use command `bin/gear app` to submit an application:

```bash
bin/gear app [-namePrefix <application name prefix>] -jar xx.jar MainClass <arg1> <arg2> ...
```

### List all running applications
To list all running applications:

```bash
gear info
```

### Kill a running application
To kill an application:

```bash
gear kill -appid <application id>
```

### To start Gearpump shell
To start a Gearpump shell

```bash
gear shell
```  
