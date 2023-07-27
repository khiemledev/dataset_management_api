FROM python:3.10.10
# Using nvidia/cuda image when you want to get GPU support

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt

RUN pip install -r requirements.txt

COPY ./app /app

CMD [ "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1" ]
