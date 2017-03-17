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
1. Review release notes in JIRA
2. Update version in docs/version.yml
3. Bump the gearpump version in version.sbt 
   ```scala
   version in ThisBuild := "RELEASE_VERSION"
   ```
4. Run dev-tools/dependencies.sh
   This will generate a LICENSE.dependencies file that lists all dependencies including Apache.
   Make sure this agrees with the LICENSE and license/* files.
   Eventually we'll have something like a verify option so the inspection isn't manual.
5. Run dev-tools/create_apache_source_release.sh $GPG_KEY $GPG_PASSPHRASE
   This will provide the source artifacts that need to be uploaded in step 6. below
6. Upload to svn 
   Run 'svn checkout https://dist.apache.org/repos/dist/dev/incubator/gearpump'
   Run 'svn mkdir RELEASE_VERSION-incubating'
   Run 'svn mkdir RELEASE_VERSION-incubating/RC[0-9]'
   cp the gearpump* files generated from 5. to RELEASE_VERSION-incubating/RC[0-9]
   Run 'svn add RELEASE_VERSION-incubating/RC[0-9]/gearpump*'
   Run 'svn commit'
7. Run dev-tools/create_apache_bin_release.sh $GPG_KEY $GPG_PASSPHRASE
   This will provide the binary artifacts that need to be uploaded in step 8. below
8. svn add gearpump-* to https://dist.apache.org/repos/dist/dev/incubator/gearpump/RELEASE_VERSION-incubating/RC[0-9]
9. svn add KEYS to https://dist.apache.org/repos/dist/dev/incubator/gearpump/
   This only needs to be done if we are adding new committers for this release
10.Create a tag for the RC release by ```git tag RELEASE_VERION-RC[0-9]```
11.Push this tag upstream and merge

Step2: Release
==================
1. Create a tag by ```git tag RELEASE_VERSION```
2. ```git remote add upstream https://github.com/apache/incubator-gearpump```
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

