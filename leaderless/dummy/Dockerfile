FROM python:3.9.12-slim

WORKDIR /code

RUN apt-get update \
    && apt-get -y install libpq-dev gcc musl-dev

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY . /code
ADD ./database.docker.ini /code/database.ini 

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"]