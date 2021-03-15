CREATE TABLE `pos`
(
    table_name VARCHAR(64) NOT NULL,
    line_cnt   INT(10)     NOT NULL DEFAULT 0,
    PRIMARY KEY (table_name)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;