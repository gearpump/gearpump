
parallelExecution in(ThisBuild, Test) := false

scalacOptions in(ThisBuild) ++= 
  // Show more detailed warnings.
  Seq("-unchecked", "-feature", "-deprecation",
    // Disable postfix, and other advanced scala features,
    "-language:-dynamics", "-language:-postfixOps", "-language:-reflectiveCalls",
    // "-Xsource:2.11",
    "-language:-existentials"
  )

