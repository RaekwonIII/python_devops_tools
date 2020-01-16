#!/usr/bin/env python

import os
import sys

import docker
import gitlab
import subprocess
import logging

from argparse import ArgumentParser
from docker import errors as docker_errors
from glob import glob
from urllib.parse import urlparse

GIT_DIFF_COMMAND = "git --no-pager diff --name-only {compare_commit}"

GET_GO_DEPENDENCIES = 'go list -deps=true'

GET_NODE_DEPENDENCIES = 'lerna ls --scope {package} --include-filtered-dependencies'

DOCKER_PULL_COMMAND = 'docker pull {ci_registry_image}/{package_name}:{version}'

DOCKER_BUILD_COMMAND = 'docker build --cache-from {ci_registry_image}/{package_name}:{version} --build-arg ' \
                       'GIT_ACCESS_TOKEN={ci_job_token} --build-arg DEP_VERSION={dep_version} --pull -t ' \
                       '{ci_registry_image}/{package_name}:{version} . -f {package_path}/Dockerfile'

DOCKER_TAG_COMMAND = 'docker tag {ci_registry_image}/{package_name}:{version} {ci_registry_image}/{' \
                     'package_name}:{tag_version}'

DOCKER_PUSH_COMMAND = 'docker push {ci_registry_image}/{package_name}:{version}'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger("AutoBuilder")


class RepoTypeNotSetException(Exception):
    pass


class GitCommandException(Exception):
    pass


def extract_gitlab_url_from_project_url():
    project_url = os.environ['CI_PROJECT_URL']
    project_path = os.environ['CI_PROJECT_PATH']

    return project_url.split(u"/{}".format(project_path), 1)[0]


def detect_changes(build_type):
    """
    Function uses git command line to assess and return a set of changed directories in the latest commit
    :param build_type: type of build (feature|stage|prod)
    :return: a set of directories changed in the latest commit
    """

    compare_commit = 'HEAD~1'
    try:
        gitlab_token = os.environ['NPA_GITLAB_TOKEN']
        project_id = os.environ['CI_PROJECT_ID']
        gl = gitlab.Gitlab(extract_gitlab_url_from_project_url(), gitlab_token)
        gl.auth()
        proj = gl.projects.get(project_id)
        current_pipeline = proj.pipelines.get(os.environ['CI_PIPELINE_ID'])
        # list of all project pipelines related to current branch which are older than current one
        pipelines = [
            p for p in proj.pipelines.list()
            if p.attributes.get('ref') == current_pipeline.attributes.get('ref')
            and p.attributes.get('status') != 'failed'
            and p.get_id() < current_pipeline.get_id()
        ]
        # if at least one pipeline is found, then the commit to compare to comes from the latest found
        if pipelines:
            prev_pipeline = pipelines.pop(0)
            logger.info("Using commit from pipeline #%s to compare to.", prev_pipeline.get_id())
            compare_commit = prev_pipeline.attributes.get('sha')
        elif build_type == 'feature':
            compare_commit = 'develop'
            logger.info('No pipelines found, comparing to %s', compare_commit)
        elif build_type == 'stage':
            compare_commit = 'master'
            logger.info('No pipelines found, comparing to %s', compare_commit)
        elif build_type == 'prod':
            logger.info('No pipelines found, comparing to %s', compare_commit)
    except KeyError as ke:
        logger.warning("Could not fetch commit of previous pipeline because of missing environment vars\n%s", str(ke))
    except gitlab.GitlabAuthenticationError as gae:
        logger.warning("Could not fetch commit of previous pipeline because of Authentication Error\n%s", str(gae))

    logger.info("Running command:\n%s", GIT_DIFF_COMMAND.format(compare_commit=compare_commit))
    p = subprocess.Popen(
        GIT_DIFF_COMMAND.format(compare_commit=compare_commit).split(' '),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    o, e = p.communicate()
    if p.returncode != 0:
        raise GitCommandException("command failed with code %d\n%s\n%s" % (p.returncode, o.decode(), e.decode()))

    changes = o.decode('ascii').split('\n')

    dirs = {os.path.dirname(c) for c in changes if os.path.dirname(c)}
    logger.info("Found %s changed directories: \n%s", len(dirs), '\n'.join(dirs))
    return dirs


def find_packages():
    """
    Function recursively searches for Dockerfiles in the current folder, to identify packages that might need rebuilding
    :return: a dictionary of packages in the current path, in the structure {package_name: package_value}
    """
    source_folders = glob('**/Dockerfile', recursive=True)
    packages = dict()
    for folder in source_folders:
        package_path = os.path.dirname(folder)
        packages[os.path.basename(package_path) or os.getenv('CI_PROJECT_NAME')] = package_path or '.'
    logger.info("Found %s packages: \n%s", len(packages), '\n'.join(packages.keys()))
    return packages


def get_private_dependencies(package_name, package_path, repo_type, prefix):
    """
    Function finds a set of dependencies of a given package, and filters for the ones that are part of the package
    itself. I.e.: private dependencies.
    :param package_name: name of the package currently being assessed
    :param package_path: the path of the package to assess
    :param repo_type: type of repository (go|node)
    :param prefix: prefix of package names, used to clean up dependencies names found (depends on repo_type)
    :return: a set of private dependencies for the given package
    """
    deps = set()
    if repo_type == 'go':
        pwd = os.getcwd()
        os.chdir(package_path)
        # `go list` always installs dependencies, so they need to be installed first, to avoid polluting the output
        p = subprocess.Popen(GET_GO_DEPENDENCIES.split(' '), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        o, e = p.communicate()
        if p.returncode != 0:
            raise Exception("command failed with code %d\n%s\n%s" % (p.returncode, o.decode(), e.decode()))
        p = subprocess.Popen(GET_GO_DEPENDENCIES.split(' '), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        o, e = p.communicate()
        if p.returncode != 0:
            raise Exception("command failed with code %d\n%s\n%s" % (p.returncode, o.decode(), e.decode()))
        logger.debug(o.decode())
        os.chdir(pwd)
        deps.update([dep.split(prefix).pop() for dep in o.decode('ascii').split('\n') if dep.startswith(prefix)])
    elif repo_type == 'node':
        package_path_prefix = package_path.rstrip(package_name)
        lerna_package_name = '{}{}'.format(prefix, package_name)
        p = subprocess.Popen(
            GET_NODE_DEPENDENCIES.format(package=lerna_package_name).split(' '),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        o, e = p.communicate()
        if p.returncode != 0:
            raise Exception("Command failed with code %d\n%s\n%s" % (p.returncode, o.decode(), e.decode()))
        logger.debug(o.decode())
        # lerna ls returns a list of packages in the format @project/package, but we need their path
        deps.update({
            '{}{}'.format(package_path_prefix, dep.split(prefix).pop())
            for dep in o.decode('ascii').split('\n') if dep.startswith(prefix)
        })

    logger.info("Found %s private dependencies for package %s: \n%s", len(deps), package_path, '\n'.join(deps))
    return deps


def find_changes_impact(packages, changes, repo_type):
    """
    Function to find the packages impacted (directly or *indirectly*, through dependencies) by the list of changes
    provided
    :param packages: a dictionary of packages, in the format {package_name: package_path}
    :param changes: a list of relative paths of changes made to the repository
    :param repo_type: type of the repository this script is assessing. It will impact how to find dependencies
    :return: a set of packages that need rebuilding
    """
    if repo_type == 'go':
        parsed_url = urlparse(os.getenv('CI_PROJECT_URL'))
        prefix = '{}{}/'.format(parsed_url.netloc, parsed_url.path)
    elif repo_type == 'node':
        prefix = '@{}/'.format(os.getenv('CI_PROJECT_NAME'))
    elif not repo_type:
        raise RepoTypeNotSetException("Repository type not set, cannot find impacting changes")
    logger.debug('Finding dependencies with prefix=%s', prefix)
    # find packages directly impacted by the list of changes provided
    impacts = {package_name for package_name, package in packages.items() if
               any(change.startswith(package) for change in changes)}
    # iterate over the packages, get their dependencies and see if any of the changes impact such dependencies
    for package_name, package_path in packages.items():
        dependencies = get_private_dependencies(package_name, package_path, repo_type, prefix)
        if any(any(change.startswith(dependency) for change in changes) for dependency in dependencies):
            impacts.add(package_name)
    logger.info("Found %s packages impacted by changes: \n%s", len(impacts), '\n'.join(impacts))
    return impacts


def _get_image_tag_name(package_name, package_path, version):
    ci_registry_image = os.getenv('CI_REGISTRY_IMAGE')
    image_tag_monorepo = '{repo}/{package_name}:{version}'
    image_tag_singlerepo = '{repo}:{version}'
    if package_path == '.':
        tag_name = image_tag_singlerepo.format(
            repo=ci_registry_image, version=version,
        )
    else:
        tag_name = image_tag_monorepo.format(
            repo=ci_registry_image, package_name=package_name, version=version,
        )
    return tag_name


def build_package(package_name, package_path, build_type):
    """
    Function builds a package, given its name, its path and the build type (either prod, stage or simple feature)
    Depending on the build type it builds a tag and changes the version name of the build.
    :param package_name: the name of the package
    :param package_path: the relative path of the package
    :param build_type: the build type
    :return: 0 if everything executed correctly
    """
    docker_cli = docker.from_env()
    version = os.getenv("CI_COMMIT_REF_SLUG")
    tag_version = None
    target_image = None
    if build_type == 'stage':
        version = os.getenv("CI_COMMIT_SHA")
        tag_version = 'stage'
    elif build_type == 'prod':
        version = os.getenv("CI_COMMIT_SHA")
        tag_version = 'latest'
    elif build_type == 'tag':
        version = "latest"
        tag_version = os.getenv("CI_COMMIT_TAG")
        image_tag_name = _get_image_tag_name(package_name=package_name, package_path=package_path, version=version)
        logger.info('Pulling image: %s', image_tag_name)
        target_image = docker_cli.images.pull(image_tag_name)

    # not performing docker build on tags, as it's just a re-tagging of a pre-built image
    if build_type != 'tag':
        dep_version = os.getenv('DEP_VERSION')
        ci_job_token = os.getenv('CI_JOB_TOKEN')
        image_tag_name = _get_image_tag_name(package_name=package_name, package_path=package_path, version=version)

        logger.info("Building tag %s of package %s", image_tag_name, package_name)
        try:
            target_image, build_logs = docker_cli.images.build(
                buildargs={'GIT_ACCESS_TOKEN': ci_job_token, 'DEP_VERSION': dep_version}, pull=True, path='.',
                dockerfile='{}/Dockerfile'.format(package_path),
                tag=image_tag_name,
                quiet=True,
            )
        except docker_errors.BuildError as be:
            logger.error(
                'Impossible to build image, an error occurred during the build process. Reason:\n%s\nLog:\n',
                be.msg,
            )
            for line in be.build_log:
                logger.error(line)
            return True
        logger.info("Pushing package %s", package_name)
        _ = docker_cli.images.push(image_tag_name)
        logger.info("Push complete")

    if tag_version:
        image_tag_name = _get_image_tag_name(package_name=package_name, package_path=package_path, version=tag_version)
        # target_image is either built in build step or pulled if it's a tag build_type
        logger.info("Generating tag %s for package %s", image_tag_name, package_name)
        res = target_image.tag(repository=image_tag_name)
        logger.info('Successful: %s', res)
        _ = docker_cli.images.push(image_tag_name)
        logger.info("Push complete")
    logger.info("Done")

    return False


def find_and_build_packages(build_type='feature', repo_type=None):
    """
    Function utilizes other functions in this module to find packages to build, as a result of changes in the repository
    and builds them according to the parameters passed.
    :param build_type: the type of build to perform (prod, stage, feature). Default: feature
    :param repo_type: the type of repository this script is running on (at the moment only go and node)
    :return: the result of the build: 0 if all executed correctly, 1 if something failed
    """
    logger.info("Building %s project for %s build, repo type: %s", os.getenv('CI_PROJECT_NAME'), build_type, repo_type)
    packages = find_packages()
    # in case of a forced build, simply "build" everything, same in case of tag, which means create a docker tag,
    # from tag commit
    if build_type == 'tag' or os.getenv('GITLAB_FORCE_BUILD'):
        logger.info("Forced or tag build, going to build entire project")
        packages_to_build = packages.keys()
    else:
        try:
            changes = detect_changes(build_type)
            packages_to_build = find_changes_impact(packages, changes, repo_type)
        except (RepoTypeNotSetException, GitCommandException) as e:
            logger.warning(e)
            logger.info("Building all packages found instead")
            packages_to_build = packages.keys()
    res = 0
    for package_name in packages_to_build:
        res += build_package(package_name, packages.get(package_name), build_type)

    logger.info("Finished building %s packages for this project", len(packages_to_build))

    return res


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "-b", "--build", help="type of build to perform (feature|stage|prod|tag)", default='feature', required=False
    )
    parser.add_argument(
        "-r", "--repo_type", help="type of repository, or language (go|node)", required=False
    )

    args = parser.parse_args()
    build = args.build
    repo = args.repo_type

    sys.exit(find_and_build_packages(build_type=build, repo_type=repo))
