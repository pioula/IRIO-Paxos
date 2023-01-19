FROM python:3.11.1-slim

WORKDIR /code

RUN apt-get update \
    && apt-get -y install libpq-dev gcc musl-dev

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY . /code
ADD ./database.docker.ini /code/database.ini 

CMD ["python3", "app/main.py"]