/*
    Script to create the GenVarDB database instance 'genvardb'
    and the associated table; annotations.
    
    The table has one main partition key; 'chromosome' and 
    three clustering keys columns; 'position', 'reference' and 'alternate'.
    
    All defaults are used except for the compaction strategy and the compression algorithm.
    This is more efficient to reduce the size of data on disk.

    Note: Some numerical columns are created as text to increase efficiency and
    then converted into numerical type while navigating the UI.

    Author : Muhammad Ashraf
    GitHub : https://github.com/muhammadash717/
*/


-- The first block creates the database itself.
-- All settings are set to default.
CREATE KEYSPACE IF NOT EXISTS genvardb
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': '1'}
AND durable_writes = true;


-- This block creates the annotations table.
-- All settings are set to default except for the compaction and compression.
CREATE TABLE IF NOT EXISTS genvardb.annotations
(
    -- Primary Key Columns
    chromosome text,
    position int,
    reference text,
    alternate text,
    
    -- Gene-based data
    effect text,
    gene text,
    consequence text,
    amino_acid text,
    dbsnp text,
    
    -- Clinical data
    clinvar_diseases text,
    clinvar_dbs text,
    clinvar_significance text,
    onco_diseases text,
    onco_dbs text,
    onco_significance text,
    somatic_diseases text,
    somatic_dbs text,
    somatic_significance text,
    intervar text,
    
    -- Population Frequency (WGS)
    gnomad_genome_all text,
    gnomad_genome_afr text,
    gnomad_genome_amr text,
    gnomad_genome_eas text,
    gnomad_genome_mid text,
    gnomad_genome_eur text,
    gnomad_genome_sas text,
    
    -- Population Frequency (WES)
    gnomad_exome_all text,
    gnomad_exome_afr text,
    gnomad_exome_amr text,
    gnomad_exome_eas text,
    gnomad_exome_mid text,
    gnomad_exome_eur text,
    gnomad_exome_sas text,
    
    -- 1000 Genomes Project
    all_1kg text,
    afr_1kg text,
    amr_1kg text,
    eas_1kg text,
    eur_1kg text,
    sas_1kg text,

    -- Great Middle East
    gme_all text,

    -- Prediction Tools for Variants Interpretation
    sift_score text,
    sift_pred text,
    polyphen2_hdiv_score text,
    polyphen2_hdiv_pred text,
    polyphen2_hvar_score text,
    polyphen2_hvar_pred text,
    mutationtaster_score text,
    mutationtaster_pred text,
    m_cap_score text,
    m_cap_pred text,
    revel_score text,
    cadd_phred text,
    gerp_rs text,
    phylop100way_vertebrate text,
    phylop470way_mammalian text,
    phylop17way_primate text,
    interpro_domain text,
    
    -- Samples data (your samples)
    variant_count int,
    variant_homozygous int,
    variant_heterozygous int,
    variant_samples text,

    PRIMARY KEY (chromosome, position, reference, alternate)
)
WITH CLUSTERING ORDER BY (position ASC, reference ASC, alternate ASC)
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.ZstdCompressor'}
    AND compaction = { 'class': 'UnifiedCompactionStrategy'};

-- Indices for faster queries...
CREATE INDEX IF NOT EXISTS annot_dbsnp_sai_idx ON genvardb.annotations (dbsnp) USING 'sai' WITH OPTIONS = {'case_sensitive': 'true', 'normalize': 'true', 'ascii': 'true'};
CREATE INDEX IF NOT EXISTS annot_effect_sai_idx ON genvardb.annotations (effect) USING 'sai' WITH OPTIONS = {'case_sensitive': 'true', 'normalize': 'true', 'ascii': 'true'};
CREATE INDEX IF NOT EXISTS annot_consequence_sai_idx ON genvardb.annotations (consequence) USING 'sai' WITH OPTIONS = {'case_sensitive': 'true', 'normalize': 'true', 'ascii': 'true'};
CREATE INDEX IF NOT EXISTS annot_intervar_sai_idx ON genvardb.annotations (intervar) USING 'sai' WITH OPTIONS = {'case_sensitive': 'false', 'normalize': 'true', 'ascii': 'true'};
CREATE INDEX IF NOT EXISTS annot_variant_count_sai_idx ON genvardb.annotations (variant_count) USING 'sai';

-- To check the progress of index creation...
-- SELECT keyspace_name,table_name,column_name,index_name,is_building,is_queryable FROM system_views.sai_column_indexes ;

