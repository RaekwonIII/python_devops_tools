from unittest import main, TestCase

from unittest.mock import patch, MagicMock
from scarface_utils.common import bump_version
from scarface_utils.common.bump_version import MergeRequestIDNotFoundException, GITLAB_MERGE_REQUEST_COMMIT_REGEX


class BumpVersionTest(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.env_list = ["CI_REPOSITORY_URL", "CI_PROJECT_ID", "CI_PROJECT_URL", "CI_PROJECT_PATH", "NPA_USERNAME",
                        "NPA_PASSWORD"]

    def setUp(self):
        pass

    @patch('subprocess.check_output')
    def test_git(self, mocked_check_output):
        test_args = ['log', '-1']
        response_mock = MagicMock()
        mocked_check_output.return_value = response_mock
        bump_version.git(*test_args)
        mocked_check_output.assert_called_with(['git'] + test_args)

    @patch.dict('os.environ', {'CI_PROJECT_URL': 'http://gitlab.com/my_project', 'CI_PROJECT_PATH': 'my_project'})
    def test_extract_gitlab_url(self):
        env_list = ['CI_PROJECT_URL', 'CI_PROJECT_PATH', 'env_var3']

        with self.assertRaises(Exception) as e:
            [bump_version.verify_env_var_presence(e) for e in env_list]
        self.assertTrue(
            u"Expected the following environment variable to be set: env_var3" in str(e.exception)
        )
        url = bump_version.extract_gitlab_url_from_project_url()
        self.assertEqual(url, 'http://gitlab.com')

    def test_extract_merge_request_id(self):
        expected_id = 10
        message = u"Real Commit message 123\n\nSee merge request XYZ/repo!{}".format(expected_id)
        merge_id = bump_version.extract_merge_request_id_from_commit(message, GITLAB_MERGE_REQUEST_COMMIT_REGEX)
        self.assertEqual(merge_id, str(expected_id))

        new_message = u"Simple Commit Msg"
        with self.assertRaises(MergeRequestIDNotFoundException) as mridnf:
            bump_version.extract_merge_request_id_from_commit(new_message, GITLAB_MERGE_REQUEST_COMMIT_REGEX)
        self.assertTrue(
            u"Unable to extract merge request from commit message: {}".format(new_message) in str(mridnf.exception)
        )

    @patch('gitlab.Gitlab')
    @patch.dict('os.environ', {
        'CI_PROJECT_ID': '20', 'NPA_PASSWORD': 'XyZ',
        'CI_PROJECT_URL': 'http://gitlab.com/my_project',
        'CI_PROJECT_PATH': 'my_project'
    })
    def test_retrieve_labels(self, mocked_gitlab):
        labels_list = ['label_one', 'bump_minor']
        merge_request_mock = MagicMock(**{'labels': labels_list})
        project_mock = MagicMock(
                    **{
                        'mergerequests.get.return_value': merge_request_mock
                    }
                )
        gitlab_mock = MagicMock(
            **{
                'auth.return_value': None,
                'projects.get.return_value': project_mock
            }
        )
        mocked_gitlab.return_value = gitlab_mock
        labels = bump_version.retrieve_labels_from_merge_request('10')
        self.assertEqual(labels, labels_list)


if __name__ == '__main__':
    main()
