lazy val root = project in file(".") aggregate(core, examples, rest)

lazy val core = project in file("core")

lazy val examples = project in file("examples") dependsOn core 

lazy val rest = project in file("rest") dependsOn core 
