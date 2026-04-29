-- =============================================================
-- 00_create_database.sql
-- Create the `portfolio` database and a dedicated `analyst` user.
-- Self-contained: works on a fresh local MySQL install OR via Docker.
-- Run this FIRST in MySQL Workbench (as root or admin user).
-- =============================================================

CREATE DATABASE IF NOT EXISTS portfolio
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

-- Create the analyst user used by Python / Dagster scripts.
-- (CREATE USER IF NOT EXISTS lets this script be re-run safely.)
CREATE USER IF NOT EXISTS 'analyst'@'localhost' IDENTIFIED BY 'analystpw';
CREATE USER IF NOT EXISTS 'analyst'@'%'         IDENTIFIED BY 'analystpw';

GRANT ALL PRIVILEGES ON portfolio.* TO 'analyst'@'localhost';
GRANT ALL PRIVILEGES ON portfolio.* TO 'analyst'@'%';
-- Note: FLUSH PRIVILEGES is unnecessary after GRANT in MySQL 8.0+
-- and is deprecated in MySQL 8.4+, so we omit it.

USE portfolio;
