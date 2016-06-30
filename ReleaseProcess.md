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
1. Modify CHANGELOG.md to add JIRA's
2. Update version in docs/_config.yml
3. Bump the gearpump version in version.sbt 

   ```scala
   version in ThisBuild := "RELEASE_VERSION"
   ```
  
4. Run dev-tools/create_apache_source_release.sh $GPG_KEY $GPG_PASSPHRASE
5. svn add gearpump-* to https://dist.apache.org/repos/dist/dev/incubator/gearpump/RELEASE_VERSION-incubating/RC[0-9]
6. svn add KEYS to https://dist.apache.org/repos/dist/dev/incubator/gearpump/

Step2: Release
==================
1. Create a tag by ```git tag RELEASE_VERSION```
2. ```git remote add upstream https://github.com/gearpump/gearpump.git```
3. ```git push upstream RELEASE_VERSION```

Step3: Post-Release
==================
1. Bump the gearpump version in version.sbt
    
   ```scala
   version in ThisBuild := "NEXT_SNAPSHOT_VERSION"
   ```
   where NEXT_SNAPSHOT_VERSION must end with "-SNAPSHOT". For example, 0.2.3-SNAPSHOT is a good snapshot version, 0.2.3 is NOT
2. Create JIRA for new release
3. Make PR with new release

