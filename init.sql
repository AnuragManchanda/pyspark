CREATE DATABASE IF NOT EXISTS pyspark_db;
USE pyspark_db;
CREATE TABLE people (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    age INT
);
INSERT INTO people (id, name, age)
VALUES (1, 'John Doe', 28);
INSERT INTO people (id, name, age)
VALUES (2, 'Jane Smith', 34);
INSERT INTO people (id, name, age)
VALUES (3, 'Bob Johnson', 45);