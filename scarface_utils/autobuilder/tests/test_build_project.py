import unittest
from unittest.mock import patch, MagicMock

import gitlab

from autobuilder.build_project import detect_changes, find_packages, get_private_dependencies, find_changes_impact, \
    build_package


class TestBuildProject(unittest.TestCase):
    @patch.dict('os.environ', {}, clear=True)
    @patch('subprocess.Popen')
    def test_detect_changes_no_env(self, mocked_popen):
        test_args = ['git', '--no-pager', 'diff', '--name-only', 'HEAD~1']
        response_mock = MagicMock(**{
            'communicate.return_value': (b'output', b'error'),
            'returncode': 0
        })
        mocked_popen.return_value = response_mock

        detect_changes('feature')

        mocked_popen.assert_called_with(test_args, stderr=-1, stdout=-1)

    @patch.dict('os.environ', {
        'CI_PROJECT_URL': 'http://gitlab.com/my_project', 'CI_PROJECT_PATH': 'my_project', 'CI_PROJECT_ID': '1',
        'NPA_GITLAB_TOKEN': 't0k3n', 'CI_PIPELINE_ID': '1',
    })
    @patch('gitlab.Gitlab')
    @patch('subprocess.Popen')
    def test_detect_changes_with_gitlab(self, mocked_popen, mocked_gitlab):
        commit_sha = 'xyz123'
        prev_unrelated__pipeline_mock = MagicMock(
            **{'attributes': {'sha': commit_sha, 'ref': 'another_branch'}, 'get_id.return_value': 0}
        )
        prev_pipeline_mock = MagicMock(
            **{'attributes': {'sha': commit_sha, 'ref': 'current_branch'}, 'get_id.return_value': 1}
        )
        curr_pipeline_mock = MagicMock(
            **{'attributes': {'sha': commit_sha, 'ref': 'current_branch'}, 'get_id.return_value': 2}
        )
        pipeline_list = [curr_pipeline_mock, prev_pipeline_mock, prev_unrelated__pipeline_mock]
        project_mock = MagicMock(
                    **{
                        'pipelines.list.return_value': pipeline_list,
                        'pipelines.get.return_value': curr_pipeline_mock
                    }
                )
        gitlab_mock = MagicMock(
            **{
                'auth.return_value': None,
                'projects.get.return_value': project_mock
            }
        )
        mocked_gitlab.return_value = gitlab_mock
        test_args = ['git', '--no-pager', 'diff', '--name-only', commit_sha]
        response_mock = MagicMock(**{
            'communicate.return_value': (b'output', b'error'),
            'returncode': 0
        })
        mocked_popen.return_value = response_mock

        detect_changes('feature')
        mocked_popen.assert_called_with(test_args, stderr=-1, stdout=-1)

        gitlab_mock = MagicMock(
            **{
                'auth.side_effect':  gitlab.GitlabAuthenticationError,
            }
        )
        mocked_gitlab.return_value = gitlab_mock
        test_args = ['git', '--no-pager', 'diff', '--name-only', 'HEAD~1']

        detect_changes('feature')
        mocked_popen.assert_called_with(test_args, stderr=-1, stdout=-1)

    @patch('autobuilder.build_project.glob')
    def test_find_packages(self, mocked_glob):
        glob_mock = ['folder1/Dockerfile', 'folder2/subfolder1/Dockerfile']
        mocked_glob.return_value = glob_mock

        result = find_packages()

        self.assertDictEqual({'folder1': 'folder1', 'subfolder1': 'folder2/subfolder1'}, result)

    @patch('autobuilder.build_project.os')
    @patch('subprocess.Popen')
    def test_get_private_dependencies_go(self, mocked_popen, mocked_os):
        response_mock = MagicMock(**{
            'communicate.return_value': (
                b'gitlab.idruide.tech/group/package1/dep1\ngitlab.idruide.tech/group/package2/dep2', b'error'
            ),
            'returncode': 0
        })
        mocked_popen.return_value = response_mock

        attrs = {
            "chdir.return_value": 0,
            "getcwd.return_value": '/home/user',
        }
        mocked_os.configure_mock(**attrs)
        res = get_private_dependencies(
            package_name='proj',
            package_path='services/proj',
            repo_type='go',
            prefix='gitlab.idruide.tech/group/'
        )
        self.assertSetEqual({'package1/dep1', 'package2/dep2'}, res)

    @patch('autobuilder.build_project.os')
    @patch('subprocess.Popen')
    def test_get_private_dependencies_node(self, mocked_popen, mocked_os):
        response_mock = MagicMock(**{
            'communicate.return_value': (b'@project/dep1\n@project/dep2', b'error'),
            'returncode': 0
        })
        mocked_popen.return_value = response_mock

        attrs = {
            "chdir.return_value": 0,
            "getcwd.return_value": '/home/user',
        }
        mocked_os.configure_mock(**attrs)
        res = get_private_dependencies(
            package_name='package',
            package_path='packages/package',
            repo_type='node',
            prefix='@project/'
        )
        self.assertSetEqual({'packages/dep1', 'packages/dep2'}, res)

    @patch('autobuilder.build_project.os')
    @patch('subprocess.Popen')
    def test_find_changes_impact(self, mocked_popen, mocked_os):
        response_mock = MagicMock(**{
            'communicate.return_value': (
                b'gitlab.idruide.tech/group/package/dep1\ngitlab.idruide.tech/group/package/dep2', b'error'
            ),
            'returncode': 0
        })
        mocked_popen.return_value = response_mock

        attrs = {
            "chdir.return_value": 0,
            "getcwd.return_value": '/home/user',
            "getenv.return_value": 'gitlab.idruide.tech/group/package/',
        }
        mocked_os.configure_mock(**attrs)

        packages = {'folder1': 'folder1', 'dep2': 'folder2/dep2'}
        changes = ['folder2/dep2']

        res = find_changes_impact(packages, changes, 'go')

        self.assertSetEqual({'dep2'}, res)

        response_mock = MagicMock(**{
            'communicate.return_value': (b'@project/dep1\n@project/dep2', b'error'),
            'returncode': 0
        })
        mocked_popen.return_value = response_mock

        attrs = {
            "chdir.return_value": 0,
            "getcwd.return_value": '/home/user',
        }
        mocked_os.configure_mock(**attrs)

        res = find_changes_impact(packages, changes, 'node')

        self.assertSetEqual({'dep2'}, res)

    @patch.dict('os.environ', {'CI_COMMIT_TAG': '0.1'}, clear=True)
    @patch('docker.from_env')
    def test_build_package(self, mocked_docker):

        tag_mock = MagicMock(return_value=True)
        image_mock = MagicMock(**{
            'tag': tag_mock
        })
        build_mock = MagicMock(**{
            'return_value': (image_mock, None)
        })
        client_mock = MagicMock(**{
            'images.build': build_mock,
            'images.pull.return_value': image_mock,
            'images.push.return_value': True,
        })
        mocked_docker.return_value = client_mock

        # test feature build
        build_package('folder1', 'folder1/subfolder2', 'feature')
        build_mock.assert_called_with(
            buildargs={'GIT_ACCESS_TOKEN': None, 'DEP_VERSION': None}, dockerfile='folder1/subfolder2/Dockerfile',
            path='.', pull=True, quiet=True, tag='None/folder1:None'

        )
        build_mock.reset_mock()

        # test stage build
        build_package('folder1', 'folder1/subfolder2', 'stage')
        build_mock.assert_called_with(
            buildargs={'GIT_ACCESS_TOKEN': None, 'DEP_VERSION': None}, dockerfile='folder1/subfolder2/Dockerfile',
            path='.', pull=True, quiet=True, tag='None/folder1:None'

        )
        tag_mock.assert_called_with(repository='None/folder1:stage')
        tag_mock.reset_mock()
        build_mock.reset_mock()

        # test prod build
        build_package('folder1', 'folder1/subfolder2', 'prod')
        build_mock.assert_called_with(
            buildargs={'GIT_ACCESS_TOKEN': None, 'DEP_VERSION': None}, dockerfile='folder1/subfolder2/Dockerfile',
            path='.', pull=True, quiet=True, tag='None/folder1:None'

        )
        tag_mock.assert_called_with(repository='None/folder1:latest')
        tag_mock.reset_mock()
        build_mock.reset_mock()

        # test tag build
        build_package('folder1', 'folder1/subfolder2', 'tag')
        build_mock.assert_not_called()
        tag_mock.assert_called_with(repository='None/folder1:0.1')


if __name__ == '__main__':
    unittest.main()
