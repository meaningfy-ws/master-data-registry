FROM docker.io/python:3.8-slim-buster
EXPOSE 8000

WORKDIR /usr/src/api/
COPY . /usr/src/api/

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

ENTRYPOINT uvicorn --host 0.0.0.0 --port 8000 master_data_registry.entrypoints.api.main:app
