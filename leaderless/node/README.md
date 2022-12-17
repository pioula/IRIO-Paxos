<h1 align="center">Welcome to Agent 👋</h1>
<p>
</p>

## Install
This project was written using Python 3.11.1. To install required libraries create venv and run:
```sh
pip3 install -r requirements.txt
```

## Usage
To run a single instance you have to setup database.ini file using database.ini.template. Table has structure:
```
CREATE TABLE accounts (
  id VARCHAR2 PRIMARY KEY,
  balance INTEGER NOT NULL
)
```
To start a single instance run:
```sh
uvicorn main:app --reload
```
To run multiple instances run:
```sh
docker compose up --build
```
> WARNING: It can take up to 6 minutes to run this command for the first time
To terminate containers and delete them run:
```sh
docker compose down
```
> WARNING: Database is ephermal meaning if you delete the container with db all data will be lost.
