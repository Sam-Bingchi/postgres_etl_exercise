--
-- PostgreSQL port of the MySQL "dev_db" database.
--
-- CREATE USER dev_user;
CREATE DATABASE dev_db;
GRANT ALL PRIVILEGES ON DATABASE dev_db TO dev_user;
ALTER USER dev_user with PASSWORD 'dev_password';

\c dev_db

CREATE TABLE IF NOT EXISTS customers(
    customerId text NOT NULL,
    transactionDate text NOT NULL, 
    PRIMARY KEY(customerId)
);


CREATE TABLE IF NOT EXISTS transactions(
    transactionId text NOT NULL,
    transactionDate DATE NOT NULL, 
    sourceDate timestamp NOT NULL, 
    merchantId integer NOT NULL, 
    categoryId integer NOT NULL, 
    currency text NOT NULL, 
    amount float NOT NULL, 
    description text NOT NULL, 
    customerId text NOT NULL,
    PRIMARY KEY(transactionId, customerId),
    CONSTRAINT fk_customer FOREIGN KEY(customerId) REFERENCES customers (customerId)
);

CREATE TABLE IF NOT EXISTS error_log(
    customerId text NOT NULL,
    transactionId text NOT NULL,
    transactionDate text NOT NULL, 
    sourceDate text NOT NULL, 
    merchantId text NOT NULL, 
    categoryId text NOT NULL, 
    currency text NOT NULL, 
    amount text NOT NULL, 
    description text NOT NULL, 
    PRIMARY KEY(customerId, transactionId, transactionDate, sourceDate, merchantId, categoryId, currency, amount, description)
);