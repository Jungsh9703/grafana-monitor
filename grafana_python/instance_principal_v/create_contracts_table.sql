CREATE TABLE contracts (
    id INT NOT NULL AUTO_INCREMENT,
    contract_start DATE NOT NULL,
    contract_end DATE NOT NULL,
    amount_krw DECIMAL(18,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);