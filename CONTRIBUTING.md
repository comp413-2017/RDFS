# Standard Contribution Guidelines

These contribution guidelines exist to streamline the process of merging everyone's patches into the repository while keeping Git history as clean as possible.

### General Guidelines

* **Do not push directly to `develop`.** Merge your changes by submitting a pull request.
* PRs should be approved by at least one person before merging. **Do not** merge an unapproved PR unless the change is an emergency fix.
* Use the squash-and-merge merge strategy for pull requests.
    * This packages changes into `develop` as single, clean commits.
    * The incremental commits from the original change can still be audited from the PR itself.
* **Do not** force push unless you are deliberately rewriting history. If you need to revert a change, use `git revert` to keep the commit tree history linear.

### Contribution Procedure

1. Create a new branch from `develop`. See the relevant wiki page: [Github Conventions](https://github.com/comp413-2017/RDFS/wiki/Github-Conventions)
2. Do work, make incremental commits, push, repeat.
3. When you are ready to merge your changes, rebase your local branch against `origin/develop` and address all conflicts, if necessary, followed by pushing again.
    * This keeps Git history clean and linear by replaying your commits on top of any remote changes that may have been introduced after you branched from `develop`.
    * To do this, `git fetch --all && git rebase origin/develop`, then follow the interactive instructions.
    * [Tutorial](https://www.atlassian.com/git/tutorials/rewriting-history/git-rebase), if it helps.
3. Go to the repository home page and create a pull request.
    * Base fork: `comp413-2017/RDFS/develop`.
    * HEAD fork: `<your branch name>`.
    * This will merge the tip (HEAD) of your feature branch into the base branch.
4. Tag reviewers.
    * Not sure who to tag? `git blame` is usually a good indicator of relevant stakeholders.
5. After your changes have been approved by at least one person, close the PR by merging using the **Squash and merge** strategy. Finally, delete the remote feature branch (you can do this from the Github UI).

### Pull Request Summary

* Your PR summary/description alone should provide enough context that anyone can review the code change.
* For non-trivial changes, the following details should be included:
    1. Motivation (what is the purpose of this change?)
    2. Changeset (what did you actually change?)
    3. Testing (how do you know it works?)

### Code Review

* Why code review?
    * Second set of eyes over a change to check for bugs or other problems the author might have missed
    * Enforces consistency of changes across multiple authors
* As an author, tag at least one person as a reviewer for every PR you make
* If you are tagged as a reviewer on a PR, respond to the review as quickly as you can
    * Keep in mind: if you're responsible for reviewing a change, **you are blocking the author on making any further progress until you approve or request changes**.
* If you haven't done a code review on Github before, check the [documentation](https://help.github.com/articles/about-pull-request-reviews/) for a how-to
