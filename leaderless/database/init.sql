CREATE DATABASE bank1;
CREATE DATABASE bank2;
CREATE DATABASE bank3;
CREATE DATABASE bank4;
CREATE DATABASE bank5;

GRANT ALL PRIVILEGES ON DATABASE bank1 TO postgres;
GRANT ALL PRIVILEGES ON DATABASE bank2 TO postgres;
GRANT ALL PRIVILEGES ON DATABASE bank3 TO postgres;
GRANT ALL PRIVILEGES ON DATABASE bank4 TO postgres;
GRANT ALL PRIVILEGES ON DATABASE bank5 TO postgres;

\c bank1
CREATE TABLE accounts (
  id VARCHAR (50) PRIMARY KEY,
  balance INTEGER NOT NULL
);
\c bank2
CREATE TABLE accounts (
  id VARCHAR (50) PRIMARY KEY,
  balance INTEGER NOT NULL
);
\c bank3
CREATE TABLE accounts (
  id VARCHAR (50) PRIMARY KEY,
  balance INTEGER NOT NULL
);
\c bank4
CREATE TABLE accounts (
  id VARCHAR (50) PRIMARY KEY,
  balance INTEGER NOT NULL
);
\c bank5
CREATE TABLE accounts (
  id VARCHAR (50) PRIMARY KEY,
  balance INTEGER NOT NULL
);

