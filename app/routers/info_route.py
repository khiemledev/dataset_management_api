from fastapi import APIRouter


router = APIRouter()


@router.get("/")
def get_info():
    return {
        "service_name": "Service name here",
        "verion": "0.1",
    }
