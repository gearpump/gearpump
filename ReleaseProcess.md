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
1. Review release notes in the issue tracker
2. Update version in docs/version.yml
3. Bump the gearpump version in version.sbt 
   ```scala
   version in ThisBuild := "RELEASE_VERSION"
   ```
4. Run the license report tasks:
   ```bash
   sbt dumpLicenseReport dependencyLicenseInfo
   ```
   Make sure the generated dependency/license reports agree with the LICENSE and license/* files.
5. Build the release artifacts:
   ```bash
   sbt clean +assembly +packArchiveZip
   ```
6. Sign the release artifacts and generate checksums for the files you plan to publish.
7. Upload the signed source/binary artifacts to the project's current release channel.
   Do not upload release artifacts to Apache dist SVN.
8. Update KEYS if new signing keys are being used for the release, and publish it alongside the signed artifacts.
9. Create a tag for the RC release by ```git tag RELEASE_VERSION-RC[0-9]```
10. Push this tag upstream and announce the release candidate for validation.

Step2: Release
==================
1. Create a tag by ```git tag RELEASE_VERSION```
2. ```git remote add upstream https://github.com/gearpump/gearpump.git```
3. ```git push upstream RELEASE_VERSION```
4. Publish the final release artifacts in the project's current release channel.

Step3: Post-Release
==================
1. Bump the gearpump version in version.sbt
    
   ```scala
   version in ThisBuild := "NEXT_SNAPSHOT_VERSION"
   ```
   where NEXT_SNAPSHOT_VERSION must end with "-SNAPSHOT". For example, 0.2.3-SNAPSHOT is a good snapshot version, 0.2.3 is NOT
2. Create a tracking issue for the next release
3. Make PR with new release
