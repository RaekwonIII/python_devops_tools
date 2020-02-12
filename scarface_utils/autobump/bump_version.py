#!/usr/bin/env python
import configparser
import logging
import gitlab
import os
import re
import sys
import subprocess

from argparse import ArgumentParser
from bumpversion.cli import main as bumpversion_main
from github import Github

CHANGELOG_FILENAME = 'CHANGELOG.md'
CHANGELOG_TAG_ENTRY = '## [{tag}]({url}/-/tags/{tag})\n'
CHANGELOG_CHANGE_ENTRY_TEMPLATE = '- {change}\n'
GITLAB_MERGE_REQUEST_COMMIT_REGEX = r'(\S*\/\S*!)(\d+)'
GITHUB_PULL_REQUEST_COMMIT_REGEX = r'(^Merge pull request #|[\w*\s*\(]#)(\d+)'
COMMIT_MESSAGE = "Auto-bumping project to version {} (skip-build)"
DEFAULT_CONFIG_FILE = '.bumpversion.cfg'
TRAVIS_EMAIL = "build@travis-ci.com"
TRAVIS_NAME = "Travis CI"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger("AutoBuilder")


class MergeRequestIDNotFoundException(Exception):
    pass


def git(*git_args):
    return subprocess.check_output(["git"] + list(git_args))


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
    logger.info("Found merge request ID %s", matches.group(2))
    return matches.group(2)


def get_gitlab_client():
    gitlab_private_token = os.environ['NPA_GITLAB_TOKEN']
    gl = gitlab.Gitlab(extract_gitlab_url_from_project_url(), private_token=gitlab_private_token)
    gl.auth()
    return gl


def get_merge_request_from_id(merge_request_id):
    gl = get_gitlab_client()
    project = gl.projects.get(os.environ['CI_PROJECT_ID'])
    merge_request = project.mergerequests.get(merge_request_id)

    return merge_request


def get_gitlab_merge_request():
    # get last commit message, it will be used later
    message = git("log", "-1", "--pretty=%B").decode("utf-8")

    try:
        merge_request_id = extract_merge_request_id_from_commit(message, GITLAB_MERGE_REQUEST_COMMIT_REGEX)
    except MergeRequestIDNotFoundException as mridnf:
        logger.warning(mridnf)
        return None
    else:
        return get_merge_request_from_id(merge_request_id)


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


def bump(project_type: str = None, config_file: str = None, labels: list = None) -> str:
    """
    Method uses labels parameter to establish what part of semantic versioning to increase and calls external library
    'bumpversion' to increase version number of the project.
    If 'project_type' parameter is equal to 'android' the library is called a second time to increase the project's
    version_code.
    :param project_type: parameter indicating the type of project. Only 'android' value is interesting to this function
    :param config_file: name of the config file to be used. Useful in combination with project_type = 'android'
    :param labels: a list of labels of the project's merge request
    :return: the new version created, after bump
    """

    labels = labels or []
    if "bump-minor" in labels:
        bump_part = 'minor'
    elif "bump-major" in labels:
        bump_part = 'major'
    else:
        bump_part = 'patch'
    bumpversion_main([bump_part])

    if project_type == 'android':
        bumpversion_main(['--config-file', config_file, '--allow-dirty', 'major'])

    cfp = configparser.ConfigParser()
    try:
        with open(DEFAULT_CONFIG_FILE) as cfg:
            cfp.read_file(cfg)
            version = cfp.get('bumpversion', 'current_version')
    except FileNotFoundError:
        if config_file:
            with open(config_file) as cfg:
                cfp.read_file(cfg)
                version = cfp.get('bumpversion', 'current_version')
        else:
            version = '1.0.0'
            logger.error("Could not read current version, using default")

    logger.info('Generated new tag version %s', version)

    return version


def tag_repo(tag):
    logger.info("Creating tag number %s", tag)
    # create a tag from new version and push it
    git("tag", tag, '-a', '-m', COMMIT_MESSAGE.format(tag))


def get_changes_list(merge_request):
    # Collect the titles of merge requests converging to this release, if found
    if merge_request:
        # Fallback: use this merge request title. This is, for example, the case of continuous deployment on a master
        # branch or if this comes from a direct commit to master
        titles = [merge_request.attributes.get('title')]
        # Collect all commits for this merge requests that are a merge request themselves.
        # This covers the scenario of a GitFlow development, where a release merge request is composed of merge requests
        # to the `development` branch.
        merge_request_ids = [
            re.search(GITLAB_MERGE_REQUEST_COMMIT_REGEX, commit.attributes.get('message'), re.M | re.I).group(2)
            for commit in merge_request.commits()
            if commit.attributes.get('title').startswith('Merge branch ')
        ]
        gl = get_gitlab_client()
        project = gl.projects.get(os.environ['CI_PROJECT_ID'])
        # reassign the variable if we have values to fill it with, or reassign it to itself as fallback (no nasty ifs)
        titles = [project.mergerequests.get(mr_id).attributes.get('title') for mr_id in merge_request_ids] or titles
    else:
        # use this commit's message
        titles = [git("log", "-1", "--pretty=%B").decode("utf-8")]

    logger.info("List of changes:\n%s", '\n'.join(titles))
    return titles


def get_changelog_fp_lines_index():
    f = lines = index = None
    try:
        # To write at the top of the file, it needs to be read first
        f = open(CHANGELOG_FILENAME, 'r+')
        # lines are preserved
        lines = f.readlines()
        # in case of empty file, start at the top, otherwise leave one line from the top for spacing
        index = 0
        if len(lines) > 0:
            index = 1
    except FileNotFoundError:
        logger.info("%s does not exist, will be created", CHANGELOG_FILENAME)
        # if the file does not exist, it needs to be created and the lines are
        f = open(CHANGELOG_FILENAME, 'w+')
        lines = []
        index = 0
        logger.info("Adding %s to git changelist", CHANGELOG_FILENAME)
        git("add", CHANGELOG_FILENAME)
    finally:
        return f, lines, index


def update_changelog(merge_request, tag):
    logger.info("Getting list of changes to update Changelog")
    titles = get_changes_list(merge_request)
    logger.info("Found %s changes since last tag", len(titles))

    f, lines, index = get_changelog_fp_lines_index()

    res = False
    if f:
        # Changes are added at the beginning, in reverse order, so first is added last and stays at the top
        for title in sorted(titles, reverse=True):
            lines.insert(index, CHANGELOG_CHANGE_ENTRY_TEMPLATE.format(change=title))
        project_url = os.environ['CI_PROJECT_URL']
        lines.insert(index, CHANGELOG_TAG_ENTRY.format(tag=tag, url=project_url))
        lines.insert(index, '\n')
        f.seek(0)
        f.writelines(lines)
        f.close()
        res = True
        logger.info("Changelog updated")
    return res


def main(project_type, config_file, no_git_flow=False):
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
        env_list = [
            "CI_REPOSITORY_URL", "CI_PROJECT_ID", "CI_PROJECT_URL", "CI_PROJECT_PATH", "CI_PROJECT_NAME",
            "NPA_GITLAB_TOKEN", "NPA_EMAIL",
        ]
        [verify_env_var_presence(e) for e in env_list]

        merge_request = get_gitlab_merge_request()
        labels = merge_request.labels if merge_request else None

        repository_url = os.environ["CI_REPOSITORY_URL"]
        name = os.environ["CI_PROJECT_NAME"]
        password = os.environ["NPA_GITLAB_TOKEN"]
        email = os.environ["NPA_EMAIL"]
        push_url = re.sub(r'([a-z]+://)[^@]*(@.*)', r'\g<1>{}:{}\g<2>'.format('oauth2', password), repository_url)

    # update repo URL
    logger.info("Switching push URL to authenticated one")
    git("remote", "set-url", "--push", "origin", push_url)

    git('checkout', branch_name)
    git('pull')

    logger.info("Configuring Git email and username")
    # configure git identity
    git("config", "user.email", email)
    git("config", "user.name", name)

    tag = bump(
        project_type=project_type,
        config_file=config_file,
        labels=labels
    )
    # TODO need to implement changelog functionality for github
    if not is_travis_ci:
        update_changelog(merge_request, tag)

    logger.info("Committing files changed by version bumping")
    # log files changed by version bumping
    logger.info(git("status"))
    # commit files changed by version bumping
    logger.info(git("commit", "-a", "-m", COMMIT_MESSAGE.format(tag)))
    # push commit with the option to skip the CI or it will trigger same job that called this script!
    logger.info(git("push", "origin", branch_name, "-o", "ci.skip"))

    tag_repo(tag)
    # push tags
    logger.info(git("push", "origin", branch_name, "--tags"))

    if not no_git_flow:
        # checkout develop branch
        logger.info(git("checkout", "develop"))
        # have to merge back the changes committed to master into develop branch
        logger.info(git("merge", branch_name))
        # push commit with the option to skip the CI or it will trigger same job that called this script!
        logger.info(git("push", "origin", "develop", "-o", "ci.skip"))

    return 0


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "-p", "--project", help="type of project(android)", required=False
    )
    parser.add_argument(
        "-c", "--config_file", help="name of the bumpversion config file", required=False
    )
    parser.add_argument('--no-git-flow', action='store_true')

    args = parser.parse_args()
    sys.exit(main(project_type=args.project, config_file=args.config_file, no_git_flow=args.no_git_flow))
