#!/usr/bin/env python
import os
import re
import sys
import subprocess
import gitlab


class MergeRequestIDNotFoundException(Exception):
    pass


COMMIT_MESSAGE = "Auto-bumping project to version {}"


def git(*args):
    return subprocess.check_output(["git"] + list(args))


def verify_env_var_presence(name):
    if name not in os.environ:
        raise Exception(u"Expected the following environment variable to be set: {}".format(name))


def extract_gitlab_url_from_project_url():
    project_url = os.environ['CI_PROJECT_URL']
    project_path = os.environ['CI_PROJECT_PATH']

    return project_url.split(u"/{}".format(project_path), 1)[0]


def extract_merge_request_id_from_commit(message):
    # matches commit messages that terminate with something like "See merge request XYZ/repo!<MERGE_REQUEST_ID>"
    matches = re.search(r'(\S*\/\S*!)(\d+)', message, re.M | re.I)

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


def bump():
    # get last commit message, it will be used later
    message = git("log", "-1", "--pretty=%B").decode("utf-8")

    # check out master branch, because we'll need to commit to it
    git('checkout', 'master')

    try:
        merge_request_id = extract_merge_request_id_from_commit(message)
    except MergeRequestIDNotFoundException as mridnf:
        print(mridnf)
        labels = []
    else:
        labels = retrieve_labels_from_merge_request(merge_request_id)

    if "bump-minor" in labels:
        bump_part = 'minor'
    elif "bump-major" in labels:
        bump_part = 'major'
    else:
        bump_part = 'patch'

    subprocess.check_output(["bumpversion", bump_part])

    with open('VERSION') as f:
        version = f.readline()

    print(COMMIT_MESSAGE.format(version))

    # commit files changed by version bumping and make the commit clean by amending previous one
    git("commit", "-a", "-m", COMMIT_MESSAGE.format(version))
    # push amended commit with the option to skip the CI or it will trigger same job that called this script!
    # '--force' option is needed because previous commit has been amended
    git("push", "--force", "-o", "ci.skip")

    return version


def tag_repo():
    repository_url = os.environ["CI_REPOSITORY_URL"]
    username = os.environ["NPA_USERNAME"]
    password = os.environ["NPA_PASSWORD"]
    email = os.environ["NPA_EMAIL"]
    name = os.environ["NPA_NAME"]

    push_url = re.sub(r'([a-z]+://)[^@]*(@.*)', r'\g<1>{}:{}\g<2>'.format(username, password), repository_url)

    # update repo URL
    git("remote", "set-url", "--push", "origin", push_url)
    # configure git identity
    git("config", "user.email", email)
    git("config", "user.name", name)

    tag = bump()
    print("Creating tag number", tag)
    # create a tag from new version and push it
    git("tag", tag)
    git("push", "origin", tag)


def main():
    print(os.environ)
    if verify_env_var_presence("TRAVIS"):
        print("RUNNING ON TRAVIS, BUMP DISABLED FOR NOW")
        return 0

    env_list = ["CI_REPOSITORY_URL", "CI_PROJECT_ID", "CI_PROJECT_URL", "CI_PROJECT_PATH", "NPA_USERNAME",
                "NPA_PASSWORD"]
    [verify_env_var_presence(e) for e in env_list]

    tag_repo()

    return 0


if __name__ == "__main__":
    sys.exit(main())
