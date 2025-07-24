
-- Table de dimension date
DROP TABLE IF EXISTS dim_date CASCADE;
CREATE TABLE dim_date (
  id_date        SERIAL PRIMARY KEY,
  full_date      DATE    NOT NULL UNIQUE,
  jour           SMALLINT NOT NULL,
  mois           SMALLINT NOT NULL,
  trimestre      SMALLINT NOT NULL,
  annee          SMALLINT NOT NULL,
  jour_semaine   SMALLINT NOT NULL
);

-- Table de dimension source
DROP TABLE IF EXISTS dim_source CASCADE;
CREATE TABLE dim_source (
  id_source SERIAL PRIMARY KEY,
  via       TEXT    NOT NULL UNIQUE
);

-- Table de dimension contrat
DROP TABLE IF EXISTS dim_contrat CASCADE;
CREATE TABLE dim_contrat (
  id_contrat SERIAL PRIMARY KEY,
  contrat    TEXT    NOT NULL UNIQUE
);

-- Table de dimension titre
DROP TABLE IF EXISTS dim_titre CASCADE;
CREATE TABLE dim_titre (
  id_titre SERIAL PRIMARY KEY,
  titre    TEXT    NOT NULL UNIQUE
);

-- Table de dimension compagnie
DROP TABLE IF EXISTS dim_compagnie CASCADE;
CREATE TABLE dim_compagnie (
  id_compagnie SERIAL PRIMARY KEY,
  compagnie    TEXT    NOT NULL UNIQUE,
  secteur       TEXT
);

-- Table de dimension niveau d'études
DROP TABLE IF EXISTS dim_niveau_etudes CASCADE;
CREATE TABLE dim_niveau_etudes (
  id_niveau_etudes SERIAL PRIMARY KEY,
  niveau_etudes    TEXT    NOT NULL UNIQUE
);

-- Table de dimension niveau d'expérience
DROP TABLE IF EXISTS dim_niveau_experience CASCADE;
CREATE TABLE dim_niveau_experience (
  id_niveau_experience SERIAL PRIMARY KEY,
  niveau_experience    TEXT    NOT NULL UNIQUE
);

-- Table de dimension skills (hard & soft)
DROP TABLE IF EXISTS dim_skill CASCADE;
CREATE TABLE dim_skill (
  id_skill    SERIAL PRIMARY KEY,
  nom         TEXT    NOT NULL UNIQUE,
  type_skill  TEXT    NOT NULL CHECK(type_skill IN ('hard','soft'))
);

-- Table de faits des offres
DROP TABLE IF EXISTS fact_offre CASCADE;
CREATE TABLE fact_offre (
  id_offer              SERIAL PRIMARY KEY,
  job_url               TEXT    NOT NULL UNIQUE,
  id_date_publication   INT     NOT NULL REFERENCES dim_date(id_date),
  id_source             INT     NOT NULL REFERENCES dim_source(id_source),
  id_contrat            INT     NOT NULL REFERENCES dim_contrat(id_contrat),
  id_titre              INT     NOT NULL REFERENCES dim_titre(id_titre),
  id_compagnie          INT     NOT NULL REFERENCES dim_compagnie(id_compagnie),
  id_niveau_etudes      INT     NOT NULL REFERENCES dim_niveau_etudes(id_niveau_etudes),
  id_niveau_experience  INT     REFERENCES dim_niveau_experience(id_niveau_experience),
  description           TEXT,
  competences           TEXT,
  secteur               TEXT
);

-- Table de liaison offre ↔ skill
DROP TABLE IF EXISTS offre_skill CASCADE;
CREATE TABLE offre_skill (
  id_offer  INT NOT NULL REFERENCES fact_offre(id_offer),
  id_skill  INT NOT NULL REFERENCES dim_skill(id_skill),
  PRIMARY KEY (id_offer, id_skill)
);

-- Indexes recommandés
CREATE INDEX idx_fact_offre_date     ON fact_offre(id_date_publication);
CREATE INDEX idx_fact_offre_source   ON fact_offre(id_source);
CREATE INDEX idx_fact_offre_contrat  ON fact_offre(id_contrat);
CREATE INDEX idx_fact_offre_titre    ON fact_offre(id_titre);
CREATE INDEX idx_fact_offre_company  ON fact_offre(id_compagnie);
