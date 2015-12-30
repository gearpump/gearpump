When you want to create a release tag, here is the process:

I use RELEASE_VERSION to represent the version to be released. 
The version must follow a format. For example, 0.2.3 is a valid release version, 0.2.3-SNAPSHOT is not a valid release version.

Step0: Function verification Checklist
===================
1. metrics Ui is updated correctly.
2. submit a jar by UI can work correctly, custom config is effective, clock is advancing. 
3. Dynamic DAG functions normally. 

Step1: Pre-release
===================
1. Modify CHANGELOG.md
2. Update version in docs/_config.yml
3. Bump the gearpump version in version.sbt 

   ```scala
   version in ThisBuild := "RELEASE_VERSION"
   ```
  
4. Follow https://github.com/gearpump/gearpump/blob/master/doc/CommitGuideline.md to submit a PR

Step2: Release
==================
1. Create a tag by ```git tag RELEASE_VERSION```
2. ```git remote add upstream https://github.com/gearpump/gearpump.git```
3. ```git push upstream RELEASE_VERSION```
4. Change the release notes under https://github.com/gearpump/gearpump/releases

Step3: Post-Release
==================
1. Bump the gearpump version in version.sbt
    
   ```scala
   version in ThisBuild := "NEXT_SNPASHOT_VERSION"
   ```
   where NEXT_SNPASHOT_VERSION must end with "-SNAPSHOT". For example, 0.2.3-SNPASHOT is a good snapshot version, 0.2.3 is NOT
2. Make PR by following https://github.com/gearpump/gearpump/blob/master/doc/CommitGuideline.md 


Done!!!
