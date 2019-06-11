#!/usr/bin/env python
import os
import re
import sys
import subprocess
import gitlab
from github import Github


class MergeRequestIDNotFoundException(Exception):
    pass


GITHUB_PULL_REQUEST_COMMIT_REGEX = r'(^Merge pull request #|[\w*\s*\(]#)(\d+)'
GITLAB_MERGE_REQUEST_COMMIT_REGEX = r'(\S*\/\S*!)(\d+)'
COMMIT_MESSAGE = "Auto-bumping project to version {} (skip-build)"
TRAVIS_EMAIL = "build@travis-ci.com"
TRAVIS_NAME = "Travis CI"


def git(*args):
    return subprocess.check_output(["git"] + list(args))


def verify_env_var_presence(name):
    if name not in os.environ:
        raise Exception(u"Expected the following environment variable to be set: {}".format(name))


def extract_gitlab_url_from_project_url():
    project_url = os.environ['CI_PROJECT_URL']
    project_path = os.environ['CI_PROJECT_PATH']

    return project_url.split(u"/{}".format(project_path), 1)[0]


def extract_merge_request_id_from_commit(message, regex):
    # matches commit messages that terminate with something like "See merge request XYZ/repo!<MERGE_REQUEST_ID>"
    matches = re.search(regex, message, re.M | re.I)

    if matches is None:
        raise MergeRequestIDNotFoundException(
            u"Unable to extract merge request from commit message: {}".format(message)
        )
    print("Found merge request ID", matches.group(2))
    return matches.group(2)


def retrieve_labels_from_merge_request(merge_request_id):
    project_id = os.environ['CI_PROJECT_ID']
    gitlab_private_token = os.environ['NPA_PASSWORD']
    gl = gitlab.Gitlab(extract_gitlab_url_from_project_url(), private_token=gitlab_private_token)
    gl.auth()

    project = gl.projects.get(project_id)
    merge_request = project.mergerequests.get(merge_request_id)

    return merge_request.labels


def get_gitlab_labels():
    # get last commit message, it will be used later
    message = git("log", "-1", "--pretty=%B").decode("utf-8")

    try:
        merge_request_id = extract_merge_request_id_from_commit(message, GITLAB_MERGE_REQUEST_COMMIT_REGEX)
    except MergeRequestIDNotFoundException as mridnf:
        print(mridnf)
        return []
    else:
        return retrieve_labels_from_merge_request(merge_request_id)


def get_github_labels():
    try:
        pr_number = int(extract_merge_request_id_from_commit(
            os.getenv("TRAVIS_COMMIT_MESSAGE"),
            GITHUB_PULL_REQUEST_COMMIT_REGEX,
        ))
    except MergeRequestIDNotFoundException as mridnf:
        print(mridnf)
        return []
    else:
        g = Github(os.getenv("GITHUBKEY"))
        repo = g.get_repo(os.getenv("TRAVIS_REPO_SLUG"))

        issue = repo.get_issue(pr_number)
        return [label.name for label in issue.get_labels()]


def bump(labels=None):
    labels = labels or []
    if "bump-minor" in labels:
        bump_part = 'minor'
    elif "bump-major" in labels:
        bump_part = 'major'
    else:
        bump_part = 'patch'

    subprocess.check_output(["bumpversion", bump_part])

    with open('VERSION') as f:
        version = f.readline()

    print("Committing files changed by version bumping")
    # commit files changed by version bumping
    print(git("commit", "-a", "-m", COMMIT_MESSAGE.format(version)))

    return version


def tag_repo(tag):
    print("Creating tag number", tag)
    # create a tag from new version and push it
    git("tag", tag, '-a', '-m', COMMIT_MESSAGE.format(tag))


def main():
    branch_name = os.getenv('TRAVIS_BRANCH') or 'master'
    print('Using branch ', branch_name)
    is_travis_ci = os.getenv('TRAVIS') == 'true'
    push_commands_list = ["push", "origin", branch_name]

    if is_travis_ci:
        env_list = ["GH_TOKEN", "TRAVIS_REPO_SLUG", "TRAVIS_COMMIT_MESSAGE"]
        [verify_env_var_presence(e) for e in env_list]

        email = TRAVIS_EMAIL
        name = TRAVIS_EMAIL

        labels = get_github_labels()
        push_url = "https://{}@github.com/{}.git".format(os.getenv('GH_TOKEN'), os.getenv('TRAVIS_REPO_SLUG'))
    else:
        env_list = ["CI_REPOSITORY_URL", "CI_PROJECT_ID", "CI_PROJECT_URL", "CI_PROJECT_PATH", "NPA_USERNAME",
                    "NPA_PASSWORD"]
        [verify_env_var_presence(e) for e in env_list]

        labels = get_gitlab_labels()

        repository_url = os.environ["CI_REPOSITORY_URL"]
        username = os.environ["NPA_USERNAME"]
        password = os.environ["NPA_PASSWORD"]
        email = os.environ["NPA_EMAIL"]
        name = os.environ["NPA_NAME"]

        push_url = re.sub(r'([a-z]+://)[^@]*(@.*)', r'\g<1>{}:{}\g<2>'.format(username, password), repository_url)
        push_commands_list.extend(["-o", "ci.skip"])

    # update repo URL
    print("Switching push URL to authenticated one")
    git("remote", "set-url", "--push", "origin", push_url)

    git('checkout', branch_name)

    tag = bump(labels=labels)

    # configure git identity
    git("config", "user.email", email)
    git("config", "user.name", name)
    
    tag_repo(tag)
    # push commit with the option to skip the CI or it will trigger same job that called this script!
    print(git(*push_commands_list))
    # push tags
    print(git("push", "origin", branch_name, "--tags",))

    return 0


if __name__ == "__main__":
    sys.exit(main())
