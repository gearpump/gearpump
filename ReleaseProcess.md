# Release process

Gearpump releases use a release commit followed by a development-version commit. The release
tag points at the release commit; the default branch ends at the development-version commit.

The examples below use `0.10.0` as the release and `0.11.0-SNAPSHOT` as the next development
version. Run release builds with JDK 17, the same Java version used by CI.

## 1. Prepare the release commit

1. Start from an up-to-date, clean `master` and create a release branch.
2. Confirm that the release tag does not already exist locally or on GitHub.
3. Set `ThisBuild / version` in `version.sbt` to the release version without a `-SNAPSHOT`
   suffix.
4. Review the changes since the previous release and draft the GitHub release notes.
5. Generate and review the dependency-license report:

   ```bash
   sbt dumpLicenseReportAggregate
   ```

   Reconcile the report with `LICENSE`, `LICENSE.bin`, and the files under `licenses/`.
6. Run the same clean compile gate as CI, then build the binary distribution:

   ```bash
   sbt -Dsbt.log.noformat=true clean compile test:compile
   sbt +assembly 'gearpump-pack / packArchiveZip'
   ```

   Run `git diff --check` as a source-tree hygiene check. Run scalastyle on changed Scala
   sources. Record the results of any broader test or scalastyle runs separately; neither is a
   repository-wide release gate until the existing failures on `master` have been resolved.

7. Commit the release-version and release-procedure changes, then merge them through a pull
   request. A release manager may push the linear release sequence directly when intentionally
   using the repository administrator exemption.

## 2. Tag and publish

1. Create an annotated tag on the release commit and push it:

   ```bash
   git tag -a 0.10.0 -m "Release Gearpump 0.10.0"
   git push origin 0.10.0
   ```

2. Build the artifacts from the tagged commit in a clean worktree. Create a source archive from
   the tag and SHA-512 checksums for every uploaded artifact:

   ```bash
   git archive --format=tar.gz --prefix=gearpump-0.10.0/ \
     -o gearpump-0.10.0-source.tar.gz 0.10.0
   shasum -a 512 gearpump-0.10.0-source.tar.gz gearpump-2.13-0.10.0.zip \
     > gearpump-0.10.0.sha512
   ```

3. When a project release key is configured, also create detached armored signatures and publish
   the public key if it is not already in `KEYS`:

   ```bash
   gpg --armor --detach-sign gearpump-0.10.0-source.tar.gz
   gpg --armor --detach-sign gearpump-2.13-0.10.0.zip
   ```

4. Create a GitHub release for the existing tag and upload the source archive, binary ZIP,
   checksum file, and any signatures:

   ```bash
   gh release create 0.10.0 --verify-tag --title "Release Gearpump 0.10.0" \
     --generate-notes gearpump-0.10.0-source.tar.gz gearpump-2.13-0.10.0.zip \
     gearpump-0.10.0.sha512
   ```

5. Maven Central publishing is optional and requires a Central Portal token and a GPG key. With
   `SONATYPE_USERNAME`, `SONATYPE_PASSWORD`, and the sbt-pgp credentials configured, stage and
   release the cross-built artifacts using sbt's Central Portal support:

   ```bash
   sbt +publishSigned sonaRelease
   ```

   Do not publish with the retired OSSRH endpoints.

## 3. Start the next development cycle

1. Set `ThisBuild / version` in `version.sbt` to `0.11.0-SNAPSHOT`.
2. Commit and publish the development-version change.
3. Verify that:
   - tag `0.10.0` still points to the release commit;
   - the GitHub release is public and its assets can be downloaded;
   - `master` reports `0.11.0-SNAPSHOT`; and
   - CI passes for the final `master` commit.
