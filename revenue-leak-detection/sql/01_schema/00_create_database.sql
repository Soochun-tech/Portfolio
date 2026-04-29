CREATE DATABASE IF NOT EXISTS portfolio
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

CREATE USER IF NOT EXISTS 'analyst'@'localhost' IDENTIFIED BY 'analystpw';
CREATE USER IF NOT EXISTS 'analyst'@'%'         IDENTIFIED BY 'analystpw';

GRANT ALL PRIVILEGES ON portfolio.* TO 'analyst'@'localhost';
GRANT ALL PRIVILEGES ON portfolio.* TO 'analyst'@'%';

USE portfolio;
