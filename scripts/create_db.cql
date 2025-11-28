/*
    Script to create the GenVarDB database instance 'genvardb'
    and the associated table; annotations.
    
    The table has one main partition key; 'chr' and 
    three clustering keys columns; 'pos', 'ref' and 'alt'.
    
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
    chr text,
    pos int,
    ref text,
    alt text,

    -- Variant data
    effect text,
    hgvs_c text,
    hgvs_p text,
    dbsnp text,
    
    -- Gene data
    gene_symbol text,
    gene_hgnc_id text,
    mane_select text,
    exon_rank text,
    exon_count text,
    intron_rank text,

    -- Clinical data
    clinvar_disease text,
    clinvar_classification text,
    clinvar_review_status text,
    clinvar_submissions_summary text,
    acmg_classification text,
    omim text,
    omim_ids text,
    matched_hpo_count text,
    matched_hpo_terms text,
    acmg_score text,
    acmg_criteria text,
    acmg_by_gene text,

    -- Prediction Tools for Variants Interpretation
    revel_score text,
    revel_prediction text,
    alphamissense_score text,
    alphamissense_prediction text,
    bayesdelnoaf_score text,
    bayesdelnoaf_prediction text,
    phylop100way_score text,
    phylop100way_prediction text,
    spliceai_max_score text,
    spliceai_max_prediction text,
    dbscsnv_ada_score text,
    dbscsnv_ada_prediction text,
    apogee2_score text,
    apogee2_prediction text,

    -- Population Frequency
    gnomad_exomes_af text,
    gnomad_genomes_af text,

    -- Others
    phenotype_combined text,
    pathogenicity_classification_combined text,
    ncbi_gene text,
    omim_gene text,
    aa_ref text,
    aa_alt text,
    aa_length text,
    aa_start text,
    canonical text,
    cdna_length text,
    cdna_start text,
    cds_length text,
    cds_start text,
    computational_prediction_selected text,
    computational_score_selected text,
    computational_source_selected text,
    allele_count_reference_population text,
    frequency_reference_population text,
    hom_count_reference_population text,
    gnomad_exomes_ac text,
    gnomad_exomes_homalt text,
    gnomad_genomes_ac text,
    gnomad_genomes_homalt text,
    gnomad_mito_heteroplasmic text,
    gnomad_mito_homoplasmic text,
    mitotip_prediction text,
    mitotip_score text,
    protein_coding text,
    protein_id text,
    splice_prediction_selected text,
    splice_score_selected text,
    splice_source_selected text,
    strand text,
    transcript_support_level text,
    diseases_description text,
    hpo_ids text,
    hpo_terms text,
    gene_description text,

    -- Samples data (your samples)
    variant_count int,
    variant_homozygous int,
    variant_heterozygous int,
    variant_samples text,

    PRIMARY KEY (chr, pos, ref, alt)
)
WITH CLUSTERING ORDER BY (pos ASC, ref ASC, alt ASC)
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.ZstdCompressor'}
    AND compaction = { 'class': 'UnifiedCompactionStrategy'};

-- Indices for faster queries...
CREATE INDEX IF NOT EXISTS annot_dbsnp_sai_idx ON genvardb.annotations (dbsnp) USING 'sai' WITH OPTIONS = {'case_sensitive': 'true', 'normalize': 'true', 'ascii': 'true'};
CREATE INDEX IF NOT EXISTS annot_effect_sai_idx ON genvardb.annotations (effect) USING 'sai' WITH OPTIONS = {'case_sensitive': 'true', 'normalize': 'true', 'ascii': 'true'};
CREATE INDEX IF NOT EXISTS annot_acmg_sai_idx ON genvardb.annotations (acmg_classification) USING 'sai' WITH OPTIONS = {'case_sensitive': 'false', 'normalize': 'true', 'ascii': 'true'};
CREATE INDEX IF NOT EXISTS annot_variant_count_sai_idx ON genvardb.annotations (variant_count) USING 'sai';

-- To check the progress of index creation...
-- SELECT keyspace_name,table_name,column_name,index_name,is_building,is_queryable FROM system_views.sai_column_indexes;

