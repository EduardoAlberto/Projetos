-- ============================================
-- CRIAÇÃO DO DATABASE
-- ============================================

CREATE DATABASE data_lake;


-- ============================================
-- CONECTAR NO DATABASE
-- ============================================

\c data_lake


-- ============================================
-- CRIAÇÃO DOS SCHEMAS
-- ============================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;


-- ============================================
-- VERIFICAR OS SCHEMAS
-- ============================================

\dn