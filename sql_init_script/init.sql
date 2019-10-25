-- PostgreSQL `bioactivities` table init
-- `resource_uri` is added to ensure the uniques of each row

CREATE TABLE "bioactivities" (
    "compound_name" character varying(4000),
    "pubmed_id" bigint,
    "authors" character varying(4000),
    "target_organism"  character varying(250),
    "target_pref_name" character varying(250),
    "gene_name" character varying(800),
    "resource_uri" character varying(250) UNIQUE
);