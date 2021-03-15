CREATE TABLE `user`
(
    uid   INT(10)     NOT NULL,
    score INT(10)     NOT NULL,
    name  VARCHAR(64) NOT NULL,
    phone VARCHAR(64) NOT NULL,
    PRIMARY KEY (uid)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;