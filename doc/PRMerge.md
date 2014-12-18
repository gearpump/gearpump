Git PR merge process for Gearpump
------------------------------------

1. Fork to create a <git-user-id>/gearpump repo, as branches can't be created in intel-hadoop/gearpump.
2. Add intel-hadoop/gearpump as an external repo 'upstream' by following the [guide](https://help.github.com/articles/configuring-a-remote-for-a-fork/).
3. Periodically sync the forked master with the main master with "git pull --rebase upstream master"
 and "git push origin master". No work should ever be done in the forked master.
4. Create a branch on <git-user_id>/gearpump and do some work in it.
5. Ensure all the unit tests are passed by running command "sbt test".
6. Open a PR, which is a one-click thing in github.com; it knows you likely are opening a PR against upstream master. [Guide](https://help.github.com/articles/creating-a-pull-request) is here.
7. [Merge PR](https://help.github.com/articles/merging-a-pull-request), [delete branch](https://help.github.com/articles/deleting-unused-branches).

You can skip using branches in your fork if desired, and just work out
of your master. Less overhead, but, you can't pursue different
unrelated changes then.

The key here is using rebase, not merge. The downstream forks never
need to use merge unless the work is a long-lived collaboration with
someone else who is pulling from your fork.

Squash my commits before a PR
------------------------------

For example I might have 12 commits in my branch. "git rebase -i
HEAD~12" opens a nice editor where you can mark some commits to be
squashed into prior commits, and make 1 big commit (or several) out of
it. I use this to tidy up what will be committed to the project
master's history since otherwise my commit messages are like "not
working" or "got it working" or "more fix" or "merged <git-user-id>/gearpump to master".

If commits have already been pushed to <git-user-id>/gearpump fork on github, you will have to
"git push --force" to overwrite them with squashed commits.

About how to squash several commits into one, please check http://gitready.com/advanced/2009/02/10/squashing-commits-with-rebase.html 
or https://github.com/edx/edx-platform/wiki/How-to-Rebase-a-Pull-Request
