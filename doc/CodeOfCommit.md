Code of Commit
========================
1.	For all commit(except doc), must create an issue id.
2.	For all commit log message, must contain the issue id,
Like this: fix #issueId, comments.
3.	For all PR, the title must contains issue Id.
4.	We use rebase instead of merge to ensure the log message is clean.
5.	If there are multiple commits in the PR, must squash all them into one. Follow guide here: https://github.com/intel-hadoop/gearpump/blob/master/doc/PRMerge.md
6.	Every commit (except doc) must have 1 guy to review before commit.


Steps:
========================
1. git remote add upstream https://github.com/intel-hadoop/gearpump.git
2. Create a working branch, and sumit a PR. 
3. keep sync with github by running 
  ```  
   git fetch upstream
  ```
4. When there is changes in upstream/master, rebase your work on upstream/master. With
   ```
   git rebase -i upstream/master
   ```
5. If there is conflict, resolve the conflict, and then 
  ```
   git rebase --continue  
  ```
6. When there are multiple commits on your working branch, try to squash them.
  For guide, please follow: https://github.com/intel-hadoop/gearpump/blob/master/doc/PRMerge.md
7. Now you have a clean log history. push to your remote working branch
  ```
  git push origin
  ```
  
  
   



