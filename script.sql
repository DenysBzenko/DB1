DROP TABLE accounts;


CREATE TABLE accounts (
    id INT AUTO_INCREMENT,
    name VARCHAR(100),
    balance DECIMAL(10, 2),
    PRIMARY KEY(id)
);

INSERT INTO accounts (name, balance) VALUES ('Alice', 1000.00), ('Bob', 1500.00);