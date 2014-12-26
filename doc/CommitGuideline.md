This documents are for commitors which have direct write permission to intel-hadoop/gearpump

Commit Guideline
========================
1.	For all commit(except doc), must create an issue id.
2.	For all commit log message, must contain the issue id, Like this: fix #issueId, comments.
3.	For all PR, the title must contains issue Id.
4.	We use rebase and squash instead of merge to ensure the log message is clean. Check section "Pull Request merge process for Gearpump"
5.	Every commit (except doc) must have 1 guy to review before commit.

Pull Request merge process for Gearpump
========================
1. Fork in github to create a /gearpump repo, as branches can't be created in intel-hadoop/gearpump. After fork, you will have a new repo at http://github.com/<git-user_id>/gearpump.
2. Add intel-hadoop/gearpump as an external repo 'upstream' by following the [guide](https://help.github.com/articles/configuring-a-remote-for-a-fork/).

  ```bash
  git remote add upstream https://github.com/intel-hadoop/gearpump.git
  ```

3. In local master branch, periodically sync the forked master with the main master with 
 
  ```
   git pull --rebase upstream  master
   git push origin master
  ``` 
No work should ever be done in the forked master. Another way to do this is to 

 ```
 git checkout master
 git fetch upstream
 git rebase upstream/master
 ```

4. Create a working branch

  ```
  git branch branch_issueId
  git checkout branch_issusId
  ```

  Work on the branch, make changes, then push to your forked gearpump repo http://github.com/<git-user_id>/gearpump

  ```
  git push origin branch_issueId
  ```

5. When there is changes in upstream/master, rebase your work on upstream/master. with

   ```bash
   git checkout branch_issueId
   git rebase -i upstream/master
   ```

   You can also use this when you want to squash(merge) multiple commits into one.
   ```git rebase -i``` will popup a window, which allow you to squash(merge) multiple commits into one commit.
   For example I might have 12 commits in my branch. "rebase -i upstream/master" opens a nice editor where you can mark some commits to be squashed(merged) into prior commits, and make 1 big commit (or several) out of it. In this way, I can tidy up what will be committed to the project master's history since otherwise my commit messages are like "not working" or "got it working" or "more fix" or "merged <git-user-id>/gearpump to master".

6. If there is conflict, resolve the conflict, and then 

  ```bash
   git rebase --continue  
  ```

  After the code is successfully rebased, a window will pop up to edit the commit log, edit it then save and exit.
7. After rebase, now you have a clean log history. push to your remote working branch

  ```
  git push origin branch_issueId
  ```

  If commits have already been pushed to <git-user-id>/gearpump fork on github, you will have to "git push --force" to overwrite them with squashed commits.

  ```
  git push origin -f branch_issueId
  ```

8. Ensure all the unit tests are passed by running command "sbt test".
6. Open a PR, which is a one-click thing in github.com; it knows you likely are opening a PR against upstream master. [Guide](https://help.github.com/articles/creating-a-pull-request) is here.
7. [Merge PR](https://help.github.com/articles/merging-a-pull-request), [delete branch](https://help.github.com/articles/deleting-unused-branches).


You can skip using branches in your fork if desired, and just work out of your master. Less overhead, but, you can't pursue different unrelated changes then.

The key here is using rebase, not merge. The downstream forks never need to use merge unless the work is a long-lived collaboration with someone else who is pulling from your fork.

More details on Squash before pull request
---------------------------
More about squash, please check 

http://gitready.com/advanced/2009/02/10/squashing-commits-with-rebase.html 

https://github.com/edx/edx-platform/wiki/How-to-Rebase-a-Pull-Request
