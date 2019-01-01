# Developer documentation

This document summarizes the information relevant to Gearpump committers and contributors.  It includes information about
the development processes and policies as well as the tools we use to facilitate those.

---

Table of Contents
* <a href="#welcome">Welcome!</a>
* <a href="#workflow">Contribution workflow</a>
    * <a href="#report-bug">Report a bug</a>
    * <a href="#request-feature">Request a new feature</a>
    * <a href="#contribute-code">Contribute code/document by creating a Pull Request</a>
    * <a href="code-review">Code Review</a>
* <a href="#build-and-test">Build the code and run the tests</a>
    * <a href="#local-copy">Make a local copy of Gearpump</a>
    * <a href="#build">How to build</a>
    * <a href="#test">How to test</a>
    * <a href="#build-doc">How to build document</a>
    * <a href="#ide-setup">IDE setup</a>
    * <a href="#code-style">Code style</a>
    * <a href="#write-unittest">How to write unit test</a>
    * <a href="#write-integrationtest">How to write integration test</a>
    * <a href="#write-doc">How to write document</a>
* <a href="#committer-work">Committer section</a>
    * <a href="#approve-pull-request">Approve a pull request</a>
    * <a href="#merge-pull-request">Merge a pull request or patch</a>
    * <a href="#release">How to make a release</a>    

---

<a name="welcome"></a>

# Welcome!

If you are reading this document then you are interested in contributing to the Gearpump project -- many thanks for that!
All contributions are welcome: ideas, documentation, code, patches, bug reports, feature requests, etc. 


<a name="workflow"></a>
# Contribution workflow

This section explains how to make a contribution. 


<a name="report-bug"></a>

## Report a bug

To report a bug you should [open an issue](https://issues.apache.org/jira/browse/GEARPUMP) in our issue tracker that
summarizes the bug.  Set the form field "Issue type" to "Bug".  If you have not used the issue tracker before you will
need to register an account (free), log in, and then click on the red "Create Issue" button in the top navigation bar.

In order to help us understand and fix the bug it would be great if you could provide us with:

1. The steps to reproduce the bug.  This includes information about e.g. the Gearpump version you are using, the deployment model, etc.
2. The expected behavior.
3. The actual, incorrect behavior.

Feel free to search the issue tracker for existing issues (aka tickets) that already describe the problem;  if there is
such a ticket please add your information as a comment.

**If you want to provide a patch along with your bug report:**
That is great!  In this case please send us a pull request as described in section [Create a pull request](#create-pr) below.
You can also opt to attach a patch file to the issue ticket, but we prefer pull requests because they are easier to work
with.


<a name="request-feature"></a>

## Request a new feature

To request a new feature you should [open an issue](https://issues.apache.org/jira/browse/GEARPUMP) in our issue tracker
and summarize the desired functionality.  Set the form field "Issue type" to "New feature".  If you have not used the
issue tracker before you will need to register an account (free), log in, and then click on the red "Create Issue"
button in the top navigation bar.


<a name="contribute-code"></a>

## Contribute code/document by creating Pull Request
Before you set out to contribute code we recommend that you familiarize yourself with the Gearpump codebase and corresponding development document at Gearpump website.

_If you are interested in contributing code to Gearpump but do not know where to begin:_
In this case you should
[browse our issue tracker for open issues and tasks](https://issues.apache.org/jira/browse/GEARPUMP/?selectedTab=com.atlassian.jira.jira-projects-plugin:issues-panel).

Contributions to the Gearpump codebase should be sent as GitHub pull requests.  See section [Create a pull request](#create-pr) below
for details.  If there is any problem with the pull request we can iterate on it using the commenting features of
GitHub.

* For _small patches_, feel free to submit pull requests directly for those patches.
* For _larger code contributions_, please use the following process. The idea behind this process is to prevent any
  wasted work and catch design issues early on.

    1. [Open an issue](https://issues.apache.org/jira/browse/GEARPUMP) on our issue tracker if a similar issue does not
       exist already.  If a similar issue does exist, then you may consider participating in the work on the existing
       issue.
    2. Comment on the issue with your plan for implementing the issue.  Explain what pieces of the codebase you are
       going to touch and how everything is going to fit together.
    3. Gearpump committers will iterate with you on the design to make sure you are on the right track.
    4. Implement your issue, create a pull request (see below), and iterate from there.


### Contribution Guideline
1.	For all commits, an issue id must be created.
2.	For all commit log messages, they must contain issue id. Like this: "fix #issueId, comments".
3.	For all PRs (pull request), the title must contains issue Id.
4.	We use rebase and squash instead of merge to ensure the log message is clean. Check section "Pull Request merge process for Gearpump"
5.	Every commit (except doc) must have 1 committer to review before commit.

<a name="create-pr"></a>
### Create a Pull Request
Before working on code contribution, you need to prepare your [development environment](#build-and-test).

To work on a code contribution, following process is suggested:

1. You need to create a working branch (before that, please make sure your local master is synced with **upstream** master.)

  ```
  git branch branch_issueId
  git checkout branch_issueId
  ```

  Work on the branch, make changes, then push to [your forked Gearpump repo](https://github.com/your_username/gearpump).

  ```
  git push origin branch_issueId
  ```

2. When there are changes in upstream/master, rebase your work on upstream/master. 

   ```bash
   git checkout branch_issueId
   git fetch upstream
   git rebase -i upstream/master
   ```

   You can also use this when you want to squash(merge) multiple commits into one.
   ```git rebase -i``` will pop up a window, which allows you to squash(merge) multiple commits into one commit.
   For example I might have 12 commits in my branch. ```git rebase -i upstream/master``` opens a nice editor where you can mark some commits to be squashed(merged) into prior commits, and make 1 big commit (or several) out of it. In this way, I can tidy up what will be committed to the project master's history since otherwise my commit messages are like "not working" or "got it working" or "more fix".

3. If there is conflict, resolve the conflict, and then 

  ```bash
   git rebase --continue  
  ```

  After the code is successfully rebased, a window will pop up to edit the commit log, edit it then save and exit.
4. After rebase, now you have a clean log history. push to your remote working branch

  ```
  git push origin branch_issueId
  ```

  If commits have already been pushed to your forked repository on GitHub, you will have to "git push --force" to overwrite them with squashed commits.

  ```
  git push origin -f branch_issueId
  ```

5. Ensure all the unit tests and integration tests are passed, check [Test](#test) for details.
6. Open a Pull Request, which is a one-click thing in github.com; it knows you likely are opening a PR against upstream master. [Guide](https://help.github.com/articles/creating-a-pull-request) is here.

<a name="code-review"></a>
# Code Review
Committer will review your code periodically. When there is any comment/feedback from committer(s), it's contributor's duty to update the pull request correspondingly. 

When the merge is done by committer, you can optionally [delete your PR branch](https://help.github.com/articles/deleting-unused-branches).


<a name="build-and-test"></a>

# Build and test
Though without a development environment setup, you can still contribute to Gearpump by reporting ideas, documentations, bugs and feature requests.
It is highly recommended to set up a development environment to make any code contribution.

<a name="local-copy"></a>
## Clone Gearpump repository and make a local copy
If you just want to study Gearpump source code, it is optional to perform following steps. 
But, if you plan to contribute to Gearpump's code base, it is necessary to perform following steps:

1. [Fork](https://help.github.com/articles/fork-a-repo) https://github.com/gearpump/gearpump to  your own repo at https://github.com/your_username/gearpump.

2. Clone the forked repo at your computer.

  ```bash
  git clone https://github.com/your_username/gearpump
  cd gearpump
  ```

3. Add gearpump/gearpump as an external repo 'upstream' by following the [guide](https://help.github.com/articles/configuring-a-remote-for-a-fork/).

  ```bash
  git remote add upstream https://github.com/gearpump/gearpump.git
  ```

4. In local master branch, periodically sync the forked master with the upstream master with 
 
  ```bash
   git pull --rebase upstream  master
   git push origin master
  ``` 

Another way to do this is to 

 ```bash
   git checkout master
   git fetch upstream
   git rebase upstream/master
 ```

No development work should ever be done in the forked master. 

<a name="build"></a>
## How to build
To make a compilation of Gearpump, you can execute:
```bash
  sbt compile pack
```

To build a Gearpump package, you can execute following commands:

```bash
  ## The target package path: output/target/gearpump-${version}.zip
  sbt clean +assembly +packArchiveZip
```

  After the build, there will be a package file gearpump-${version}.zip generated under output/target/ folder.

  **NOTE:**
The build requires network connection. If you are behind a proxy, make sure you have set the proxy in your env before running the build commands.

For Windows:

```bash
set HTTP_PROXY=http://host:port
set HTTPS_PROXY= http://host:port
```

For Linux:

```bash
export HTTP_PROXY=http://host:port
export HTTPS_PROXY= http://host:port
```
<a name="test"></a>
## How to test
Unit tests and Integration tests are an essential part of code contributions.

To run unit test, you can run
```bash
  sbt test
```

Gearpump has an integration test system which is based on Docker. Please check [the instructions](integrationtest/README.md).

<a name="build-doc"></a>
## How to build Gearpump documentation
To build Gearpump document use
```bash
   ## the 2nd argument indicates whether to build API doc or not
   ## for release, we need to use '1' to build it (time consuming)
   docs/build_doc.sh  2.11  0 
```  

<a name="ide-setup"></a>
## IDE setup
IDE environment can be set up on either Windows, Linux and Mac platform. You can choose the one you prefer. 
The IDE setup guide can be found at [Gearpump website](http://gearpump.apache.org/releases/latest/dev/dev-ide-setup/index.html).

It is highly recommended to perform [package build](#build) before IDE setup.

<a name="code-style"></a>
## Gearpump code style

Gearpump follows the standard Scala style, just like other Scala projects. This style is:

1. Mainly, we follow the [Spark Scala style](https://github.com/databricks/scala-style-guide).

2. We allow some exceptions: e.g. allowing using !, ? for Akka to send actor message.

Before submitting a PR, you should always run style check first:
```
  ## Run style check for compile, test, and integration test.
  sbt scalastyle test:scalastyle it:scalastyle
```

<a name="write-unittest"></a>
## How to write unit test
TBD

<a name="write-integrationtest"></a>
## How to write integration test
TBD

<a name="write-doc"></a>
## How to write document
Documentation contributions are very welcome!

You can contribute documentation by pull request, same as code contribution.
Main directory is ```docs/```, and you can refer to docs/README.md for how to build / test documentation.

NOTE: this documentation is the release documentation for Gearpump, not the website documentation at Apache website, which is maintained by another repository.


<a name="committer-work"></a>
# Committer section
_This section applies to committers only._

<a name="approve-pull-request"></a>
## Approve pull request
It's committer's duty to review pull requests from contributors. 

Any PR ready to merge shall have at least one +1(s) and no -1(s) from other than the one who authored this PR. And any merge shall wait another 24 hours after the first +1 received to wait for potential comments.
Only committer has the right to perform PR merge to Apache upstream.


<a name="merge-pull-request"></a>
## Merge pull request

All merges should be done using the `dev-tools/merge_gearpump_pr.py` script. To use this script, you will need to add a git remote called "apache" at `https://git-wip-us.apache.org/repos/asf/incubator-gearpump.git`, as well as one called "apache-github" at `git://github.com/apache/incubator-gearpump`. For the "apache" repo, you can authenticate using your ASF username and password. 

The script is fairly self explanatory and walks you through following steps and options interactively.

1. squashes the pull request's changes into one commit and merge into master
2. push to "apache" repo (automitically close GitHub pull request)
3. optionally cherry-pick the commit on to another branch
5. clean up and resolve the related JIRA issue

If you want to amend a commit before merging – which should be used for trivial touch-ups – then simply let the script wait at the point where it asks you if you want to push to Apache. Then, in a separate window, modify the code and push a commit. Run "git rebase -i HEAD~2" and "squash" your new commit. Edit the commit message just after to remove your commit message. You can verify the result is one change with "git log". Then resume the script in the other window.
Also, please remember to set Assignee on JIRAs where applicable when they are resolved. The script can't do this automatically.

<a name="release"></a>
## How to make a release
Follow [release process](ReleaseProcess.md) for making a release.
