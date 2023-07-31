from pathlib import Path

from utils.config_utils import get_config
from utils.logger_utils import get_logger

logger = get_logger()
config = get_config()


class StorageWrapper:
    ROOT = Path(config.storage.root)
    DATASET = ROOT / 'datasets'

    def __init__(self) -> None:
        pass

    def save_file(
        self,
        file: bytes,
        path: str | Path,
        dataset: str,
    ) -> Path:
        path = self.DATASET / dataset / 'dataset' / path
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'wb') as f:
            f.write(file)

        return path

    def check_dataset_exists(self, dataset: str) -> bool:
        dataset_path = self.DATASET / dataset
        return dataset_path.exists()

    def list_files(self, dataset: str) -> list[Path]:
        dataset_path = self.DATASET / dataset / 'dataset'
        if not dataset_path.exists():
            raise FileNotFoundError(f'Dataset {dataset} not found')

        files = dataset_path.glob('**/*')
        # Remove root directory
        files = [f.relative_to(dataset_path) for f in files if f.is_file()]
        return files

    def delete_file(self, path: str, dataset: str):
        path = self.DATASET / dataset / 'dataset' / path
        if not path.exists():
            raise FileNotFoundError(f'File {path} not found')

        path.unlink()
