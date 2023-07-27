import dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import dataset_route, info_route
from utils.config_utils import get_config
from utils.logger_utils import get_logger

dotenv.load_dotenv()
config = get_config()
logger = get_logger()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.app.cors.origins,
    allow_credentials=True,
    allow_methods=config.app.cors.methods,
    allow_headers=config.app.cors.headers,
)

app.include_router(
    info_route.router,
    prefix="/info",
)
app.include_router(
    dataset_route.router,
    prefix="/dataset",
)


@app.get("/")
def root():
    return {
        "message": "Hello World!",
    }


@app.get("/healh_check")
def healh_check():
    logger.info("Health check")
    logger.info(config.env_name_here)
    return {
        "status": "Running (Healthy)",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host=config.app.host,
        port=config.app.port,
        workers=1,
    )
