# autobuilder

#### Utility to manage incremental builds of monorepo projects

This utility will scan the repository it's being used on for Dockerfile(s), identifying packages, then it will 
find differences introduced in the latest commit, find impacts on the packages (including indirect ones, through 
dependencies) and decide which packages  need to be built.

Finally it will build such packages.

## Requirements:
* Python 3.7
* pip
* pipenv

## Usage:
```bash
pipenv install
pipenv run autobuilder/build_project.py -b {[feature]|stage|prod|tag} -r {[go]|node}
```
**NOTE:** To perform an automated build, but force it to build all packages, regardless of diffs from previous commits. Use the environment variable `GITLAB_FORCE_BUILD`

```bash
pipenv install
GITLAB_FORCE_BUILD=True pipenv run autobuilder/build_project.py -b {[feature]|stage|prod|tag} -r {[go]|node}
```

#### Adviced usage:
Add previous commands to Gitlab CI stage that usually builds the project, effectively substituting current commands 

**NOTE:** When used in a Gitlab CI, you can force it to build all packages, regardless of diffs from previous commits by creating a pipeline with the environment variable `GITLAB_FORCE_BUILD` set to any valie