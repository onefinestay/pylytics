CREATE TABLE dim_location (
    id INT AUTO_INCREMENT COMMENT 'surrogate key',
    code CHAR(3) COMMENT 'natural key',
    name VARCHAR(40),
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                             ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
) CHARSET=utf8
