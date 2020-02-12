from io import StringIO
from unittest import main, TestCase

from unittest.mock import patch, MagicMock, mock_open, call
from autobump import bump_version


class BumpVersionTest(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.env_list = ["CI_REPOSITORY_URL", "CI_PROJECT_ID", "CI_PROJECT_URL", "CI_PROJECT_PATH", "NPA_GITLAB_TOKEN"]

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
        merge_id = bump_version.extract_merge_request_id_from_commit(
            message, bump_version.GITLAB_MERGE_REQUEST_COMMIT_REGEX
        )
        self.assertEqual(merge_id, str(expected_id))

        new_message = u"Simple Commit Msg"
        with self.assertRaises(bump_version.MergeRequestIDNotFoundException) as mridnf:
            bump_version.extract_merge_request_id_from_commit(
                new_message, bump_version.GITLAB_MERGE_REQUEST_COMMIT_REGEX
            )
        self.assertTrue(
            u"Unable to extract merge request from commit message: {}".format(new_message) in str(mridnf.exception)
        )

    @patch('gitlab.Gitlab')
    @patch.dict('os.environ', {
        'CI_PROJECT_ID': '20', 'NPA_GITLAB_TOKEN': 'XyZ',
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
        mr = bump_version.get_merge_request_from_id('10')
        self.assertEqual(mr.labels, labels_list)

    @patch('subprocess.check_output')
    @patch('gitlab.Gitlab')
    @patch.dict('os.environ', {
        'CI_PROJECT_ID': '20', 'NPA_GITLAB_TOKEN': 'XyZ',
        'CI_PROJECT_URL': 'http://gitlab.com/my_project',
        'CI_PROJECT_PATH': 'my_project'
    })
    def test_get_merge_request(self, mocked_gitlab, mocked_check_output):
        test_args = ["log", "-1", "--pretty=%B"]
        labels_list = ['label_one', 'bump_minor']

        good_message = b"Real Commit message 123\n\nSee merge request XYZ/repo!10"
        bad_message = b"Simple Commit Msg"

        mocked_check_output.side_effect = [good_message, bad_message]

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
        mr = bump_version.get_gitlab_merge_request()
        mocked_check_output.assert_called_with(['git'] + test_args)
        self.assertEqual(mr.labels, labels_list)

        mr = bump_version.get_gitlab_merge_request()
        self.assertIsNone(mr)

    @patch('autobump.bump_version.bumpversion_main')
    @patch('builtins.open', new_callable=mock_open)
    def test_bump(self, mocked_open, mocked_bumpversion_main):
        version = '1.0.0'
        config_file = '[bumpversion]\ncurrent_version = {}'.format(version)
        input_outputs = [
            (['label_one', 'bump-minor'], 'minor'),
            (['label_one', 'bump-major'], 'major'),
            (['label_one'], 'patch')
        ]
        mocked_open.side_effect = [StringIO(config_file) for _ in input_outputs]

        for input_labels, output in input_outputs:
            new_version = bump_version.bump(labels=input_labels)
            print('test')
            mocked_bumpversion_main.assert_called_with([output])
            self.assertEqual(version, new_version)

    @patch('autobump.bump_version.bumpversion_main')
    @patch('builtins.open', new_callable=mock_open)
    def test_bump_android(self, mocked_open, mocked_bumpversion_main):
        version = '1.0.0'
        config_file = '[bumpversion]\ncurrent_version = {}'.format(version)
        input_outputs = [
            (['label_one', 'bump-minor'], 'minor'),
            (['label_one', 'bump-major'], 'major'),
            (['label_one'], 'patch')
        ]
        mocked_open.side_effect = [StringIO(config_file) for _ in input_outputs]

        for input_labels, output in input_outputs:
            config_file = 'fake_config_file.cfg'
            new_version = bump_version.bump(project_type='android', config_file=config_file, labels=input_labels)
            print('test')
            mocked_bumpversion_main.assert_has_calls([
                call([output]),
                call(['--config-file', config_file, '--allow-dirty', 'major'])
            ])
            self.assertEqual(version, new_version)


if __name__ == '__main__':
    main()
