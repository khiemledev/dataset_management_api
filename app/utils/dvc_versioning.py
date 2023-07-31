
import datetime
import io
import os
import subprocess
import tempfile
import zipfile
from pathlib import Path

from dvc.api import DVCFileSystem
from git import Repo
from utils.config_utils import get_config
from utils.logger_utils import get_logger

logger = get_logger()
config = get_config()


class DVCVersioning:
    ROOT = Path(config.storage.root)
    DATASET = ROOT / 'datasets'
    REMOTE = Path(config.storage.remote)

    def __init__(self) -> None:
        self.DATASET.mkdir(parents=True, exist_ok=True)

    def _init_repo(self, dataset_name: str):
        repo_path = str(self.DATASET / dataset_name)
        if not (Path(repo_path) / '.git').exists():
            subprocess.check_call(['git', 'init'], cwd=repo_path)
        if not (Path(repo_path) / '.dvc').exists():
            subprocess.check_call(['dvc', 'init'], cwd=repo_path)

    def _config_dvc_remote(self, dataset_name: str):
        repo_path = str(self.DATASET / dataset_name)
        remote_path = str(self.REMOTE / dataset_name)
        try:
            subprocess.check_call(
                ['dvc', 'remote', 'add', '-d', 'remote_storage', remote_path], cwd=repo_path)
        except subprocess.CalledProcessError:
            print("DVC remote storage already configured")

    def add_dataset(self, dataset_name: str):
        dataset_path = self.DATASET / dataset_name
        dataset_path.mkdir(parents=True, exist_ok=True)

        # Init the repo
        self._init_repo(dataset_name)
        self._config_dvc_remote(dataset_name)

        if not dataset_path.exists():
            print(f"Dataset path {dataset_path} does not exist")
            return

        repo_path = str(self.DATASET / dataset_name)

        subprocess.check_call(['dvc', 'add', 'dataset'],
                              cwd=repo_path)
        subprocess.check_call(
            ['git', 'add', f'dataset.dvc', '.gitignore'], cwd=repo_path)
        try:
            subprocess.check_call(
                ['git', 'commit', '-m', f'Add {dataset_name} to DVC'], cwd=repo_path)
        except subprocess.CalledProcessError:
            print("Nothing to commit")
        subprocess.check_call(['dvc', 'push'], cwd=repo_path,
                              stdout=subprocess.DEVNULL)

    def list_datasets(self):
        files = self.DATASET.glob('**/*.dvc')
        dataset_dvc_files = [f for f in files if f.name != '.dvc']
        return [f.parts[-2] for f in dataset_dvc_files]

    def list_versions(self, dataset_name: str):
        dvc_name = f'dataset.dvc'
        dvc_path = self.DATASET / dataset_name / dvc_name
        if not dvc_path.exists():
            raise FileNotFoundError(f'DVC file {dataset_name} not found')

        repo = Repo(str(self.DATASET / dataset_name))
        commits = list(repo.iter_commits(paths=dvc_name))

        _commits = []
        for commit in commits:
            _commits.append({
                'hash': commit.hexsha,
                'message': commit.message,
                'committed_date': datetime.datetime.fromtimestamp(commit.committed_date).isoformat(),
            })
        return _commits

    def list_untracked_changes(self, dataset_name: str):
        repo_path = str(self.DATASET / dataset_name)

        # Get the diff of the dataset
        diff = subprocess.check_output(
            ['dvc', 'diff', '--target', 'dataset'], cwd=repo_path).decode('utf-8')

        # Parse the diff to get the untracked changes
        untracked_changes = {}
        change_type = None
        for idx, line in enumerate(diff.splitlines()):
            if line.startswith('Added:'):
                change_type = 'added'
            elif line.startswith('Modified:'):
                change_type = 'modified'
            elif line.startswith('Deleted:'):
                change_type = 'deleted'
            elif change_type:
                if change_type not in untracked_changes:
                    untracked_changes[change_type] = []

                line = line.strip()
                if line == "":
                    continue

                if "summary" in line:
                    continue

                untracked_changes[change_type].append(line)

        return untracked_changes

    def download_dataset(self, dataset_name: str, commit_hash: str):
        repo_path = str(self.DATASET / dataset_name)

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)

            # Download data into the temporary directory
            subprocess.check_call(
                ['dvc', 'get', repo_path, 'dataset', '--rev', commit_hash], cwd=str(temp_dir))

            # Create a BytesIO object to hold the zip file
            zip_data = io.BytesIO()

            # Create a zip file and add the data files
            with zipfile.ZipFile(zip_data, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                for file in temp_dir.glob('**/*'):
                    print(file)
                    if file.is_file():
                        zip_file.write(
                            file,
                            arcname=file.relative_to(temp_dir),
                        )

        # Return the zip file as bytes
        return zip_data.getvalue()

    def commit_changes(self, dataset_name: str, message: str):
        repo_path = str(self.DATASET / dataset_name)

        subprocess.check_call(['dvc', 'add', 'dataset'], cwd=repo_path)
        subprocess.check_call(
            ['git', 'add', f'dataset.dvc'], cwd=repo_path)
        try:
            subprocess.check_call(
                ['git', 'commit', '-m', message], cwd=repo_path)
        except subprocess.CalledProcessError:
            print("Nothing to commit")
        subprocess.check_call(['dvc', 'push'], cwd=repo_path)

    def remove_dataset(self, dataset_name: str):
        dataset_path = self.DATASET / dataset_name
        repo_path = str(self.DATASET / dataset_name)

        # # Remove the DVC-tracked file
        # subprocess.check_call(['dvc', 'remove', f'dataset.dvc'],
        #                       cwd=repo_path)

        # Optionally remove the actual data file
        if dataset_path.exists():
            subprocess.check_call(['rm', '-rf', dataset_path])

        # # Commit the changes
        # subprocess.check_call(
        #     ['git', 'add', f'{dataset_name}.dvc', '.gitignore'], cwd=repo_path)
        # try:
        #     subprocess.check_call(
        #         ['git', 'commit', '-m', f'Remove {dataset_name} from DVC'], cwd=repo_path)
        # except subprocess.CalledProcessError:
        #     print("Nothing to commit")
