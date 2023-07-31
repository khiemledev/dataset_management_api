import io

from fastapi import APIRouter, File, Form, UploadFile
from fastapi.responses import StreamingResponse
from utils.dvc_versioning import DVCVersioning
from utils.storage_wrapper import StorageWrapper

storage = StorageWrapper()
versioning = DVCVersioning()

router = APIRouter()


@router.post('/create')
def create_dataset(
    dataset_name: str = Form(...),
    files: list[UploadFile] = File(...),
):
    if storage.check_dataset_exists(dataset_name):
        return {
            'message': f'Dataset {dataset_name} already exists',
        }

    for file in files:
        storage.save_file(
            file.file.read(),
            file.filename,
            dataset=dataset_name,
        )
    versioning.add_dataset(dataset_name)

    return {
        'message': 'Dataset created successfully',
    }


@router.get('/list')
def list_datasets():
    return versioning.list_datasets()


@router.get('/list_versions')
def list_dataset_versions(
    dataset_name: str,
):
    if not storage.check_dataset_exists(dataset_name):
        return {
            'message': f'Dataset {dataset_name} does not exist',
        }

    return versioning.list_versions(
        dataset_name=dataset_name,
    )


@router.post('/upload_files')
def upload_files(
    dataset_name: str = Form(...),
    files: list[UploadFile] = File(...),
):
    if not storage.check_dataset_exists(dataset_name):
        return {
            'message': f'Dataset {dataset_name} does not exist',
        }

    for file in files:
        storage.save_file(
            file.file.read(),
            file.filename,
            dataset=dataset_name,
        )

    return {
        'message': 'Files uploaded successfully',
    }


@router.get('/list_untracked_changes')
def list_untracked_changes(
    dataset_name: str,
):
    if not storage.check_dataset_exists(dataset_name):
        return {
            'message': f'Dataset {dataset_name} does not exist',
        }

    return versioning.list_untracked_changes(
        dataset_name=dataset_name,
    )


@router.get('/list_files')
def list_files(
    dataset_name: str,
):
    if not storage.check_dataset_exists(dataset_name):
        return {
            'message': f'Dataset {dataset_name} does not exist',
        }

    return storage.list_files(dataset_name)


@router.get('/download')
def download_dataset(
    dataset_name: str,
    commit_hash: str,
):
    if not storage.check_dataset_exists(dataset_name):
        return {
            'message': f'Dataset {dataset_name} does not exist',
        }

    file_bytes = versioning.download_dataset(
        dataset_name=dataset_name,
        commit_hash=commit_hash,
    )
    return StreamingResponse(
        io.BytesIO(file_bytes),
        media_type='application/zip',
        headers={
            'Content-Disposition': f'attachment; filename={dataset_name}_{commit_hash}.zip',
        }
    )


@router.delete('/delete_file')
def delete_file(
    dataset_name: str,
    file_path: str,
):
    if not storage.check_dataset_exists(dataset_name):
        return {
            'message': f'Dataset {dataset_name} does not exist',
        }

    try:
        storage.delete_file(
            path=file_path,
            dataset=dataset_name,
        )
    except FileNotFoundError:
        return {
            'message': f'File {file_path} not found',
        }

    return {
        'message': 'File deleted successfully',
    }


# route to commit changes in dataset_name
@router.post('/commit')
def commit_changes(
    dataset_name: str = Form(...),
    commit_message: str = Form(...),
):
    if not storage.check_dataset_exists(dataset_name):
        return {
            'message': f'Dataset {dataset_name} does not exist',
        }

    versioning.commit_changes(
        dataset_name=dataset_name,
        message=commit_message,
    )

    return {
        'message': 'Changes committed successfully',
    }


@router.delete('/delete_dataset')
def delete_dataset(
    dataset_name: str,
):
    if not storage.check_dataset_exists(dataset_name):
        return {
            'message': f'Dataset {dataset_name} does not exist',
        }

    versioning.remove_dataset(dataset_name)

    return {
        'message': 'Dataset deleted successfully',
    }
