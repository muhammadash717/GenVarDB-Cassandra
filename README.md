# GenVarDB-Cassandra
A scalable Apache Cassandra database with a structured schema and integrated frontend for exploring genomic variant datasets.

# **1. Introduction**

Storing genetic variants, such as single nucleotide variants (SNVs), insertions, and deletions, within a structured and reliable database is a critical step in advancing genetic research. Genetic variants play a pivotal role in understanding the genetic basis of diseases, uncovering population-specific traits, and driving the development of personalized medicine. As large-scale sequencing projects generate massive amounts of data, managing and analyzing this information becomes increasingly complex. A dedicated and efficient database is essential for organizing and leveraging this genetic data to ensure it is accessible for further analysis and interpretation. The GenVarDB is designed to address these needs by storing and managing genetic variants identified through such projects. This database not only catalogs the specific variants but also stores the genotypes associated with each variant in samples (vcf files). By offering a well-structured system, it enables seamless data management, while facilitating analyses and clinical interpretations.  
This guide is intended to walk you through the complete process of setting up, maintaining, and optimizing a database tailored to handle genetic variant data. From initial configuration and indexing strategies to query optimization and troubleshooting, this document will provide researchers, data scientists, and clinicians with the tools to efficiently store, retrieve, and analyze genetic variant information. Whether you’re focusing on population genetics, disease associations, or personalized medicine, this database will serve as the foundation for managing vast genetic datasets with precision and scalability. In the rapidly evolving field of genomics, the ability to store and query large datasets efficiently can significantly enhance research outcomes and clinical applications. By using this guide, you will be equipped to manage genetics variants data with the rigor and reliability required for high-quality genomic research and healthcare integration.

# **2. Schema Design**

The database comprises one main table; annotations, that stores all information about genetic variants. The genetic variants are defined by their genomic location (chromosome and position), reference and alternative alleles. The chromosome is the main partition column based on which the data is partitioned (divided) in separate groups to facilitate table query and data retrieval. Since the chromosome must be specified in every query, all other chromosomes are discarded while searching, enormously accelerating the query process. Within each partition, the data are clustered (sorted) in ascending order based on the value in the position column, followed by the reference allele, and then the alternate. The presence of variants in individual samples are represented in 4 columns showing the variant count in all samples, number of homozygous/heterozygous occurrences of the variant, and the samples containing the variant as well as the corresponding genotype. Gene-based data (from RefSeq) are shown in 5 columns for the variant unique identifier, gene name, variant effect (downstream, exonic, intergenic, intronic, ncRNA, splicing, upstream, UTR3, UTR5), consequence for exonic variants (frameshift\_deletion, frameshift\_insertion, nonframeshift\_deletion, nonframeshift\_insertion, nonsynonymous\_SNV, startloss, stopgain, stoploss, synonymous\_SNV), and amino acids change. Clinical annotations are shown in 4 columns for the diseases involving the variant (germline, oncogenic, somatic), variant hits in clinical databases (such as OMIM, MedGen, Orphanet, …), variant significance on ClinVar, and InterVar prediction based on the ACMG guidelines (Benign, Likely\_benign, Likely\_pathogenic, Pathogenic, Uncertain\_significance). Variant frequencies from various databases are also available in multiple columns. Population frequency databases are gnomAD genome (7 columns), gnomAD exome (7 columns), 1000 genomes (6 columns), and the Great Middle East (1 column). In addition, a number of computational tools for variant interpretation, pathogenicity prediction, and conservation are also included in multiple columns. Some of these tools are SIFT, PolyPhen, GERP, and CADD.  

# **3. Software & Tools**

To set up the database, you will need the following software and tools:

- JAVA version 11 (version17 is also supported)  
- Python 3  
- Apache Cassandra (version  5.0.0)
- Perl  
- PIP package manager  
- ANNOVAR

Firstly, Setup up JAVA, PIP and the required python modules (with PIP):  
```
sudo apt update && sudo apt upgrade -y
sudo apt install openjdk-11-jre-headless python3-pip -y
pip3 install cassandra-driver flask
```

Secondly, you need to download the tarball files and extract them for Cassandra and ANNOVAR:

1. Apache Cassandra
```
wget https://archive.apache.org/dist/cassandra/5.0.0/apache-cassandra-5.0.0-bin.tar.gz  
tar -xzf apache-cassandra-5.0.0-bin.tar.gz  
```
All the database core files, logs, and tables are stored in the downloaded tarball file.

2. ANNOVAR
```
wget http://www.openbioinformatics.org/annovar/download/0wgxR2rIVP/annovar.latest.tar.gz  
tar -xzf annovar.latest.tar.gz
```

Lastly, Download and Install the necessary database files required to annotate with ANNOVAR.  
```
### Download necessary database files.
for database in refGeneWithVer,avsnp151,clinvar_20240917,intervar_20180118,gnomad41_genome,gnomad41_exome,dbnsfp47a; do perl annovar/annotate_variation.pl -buildver hg38 -downdb -webfrom annovar ${database} annovar/humandb/; done
```
All databases are downloaded (along with their indexes) using the `annotate_variation.pl` script provided by ANNOVAR. But to speed up the processes downstream, the databases need to be reindexed using another script, `index_annovar.pl`, downloaded from an external github repository ([link](https://gist.github.com/fo40225/f135b50b3e47d0997098264c3d28e590)). The best indexing parameter for all databases is 1000 except for gnomad_genome (500), gnomad_exome (100), and intervar (100). Note that for InterVar, it's better to select the first 6 columns only since they are the only ones required.


# **4. Database Installation & Configs**

To provide a flexible and scalable environment for enhanced data storage and retrieval while maintaining system resources usage, some modifications must be made in the two configuration files; `cassandra.yaml` which is the main configuration file for Cassandra and `cassandra-env.sh` where the JAVA environment variables can be set. The configuration files can be found in the conf directory within the tarball install location. All you need to do is to replace the original conf directory with the modified one in this repository.  
You can launch the database server by executing the bin file `cassandra` that can be found in the bin directory within the tarball install location. This command operates in the background so you won’t get back the bash terminal prompt unless you press enter. You can know the server launched successfully when you get the last two lines from the previous command something like this:
```
INFO [main] 2024-10-26 08:35:09,423 StorageService.java:3220 - Node localhost/127.0.0.1:7000 state jump to NORMAL  
INFO [main] 2024-10-26 08:35:09,487 CassandraDaemon.java:450 - Prewarming of auth caches is disabled  
```
To further check if everything works fine, open the CQL shell by executing the bin file `cqlsh` (in the bin directory). To exit the CQL shell, simply press `Ctrl+D`.  
To create the GenVarDB Variants Database instance (keyspace) and the annotations table, simply run the following command.
`apache-cassandra-5.0.0/bin/cqlsh -f create_db.cql`
The `create_db.cql` script contains instructions for creating the database, the annotations table with the necessary parameters, and the column indexes to facilitate certain queries. All default options are used except for the compaction strategy (use the Unified Compaction Strategy) and the compression algorithm (use the Zstd compressor). The database server can be terminated by running
`apache-cassandra-5.0.0/bin/nodetool stopdaemon`

# **5. Data Importing**

The data are prepared and imported after the analysis of a sequenced sample is completed and the plain VCF files are produced. These VCF files serve as the starting point for the annotation and data import processes. The workflow can be launched by running the script `pipeline.sh` (in the scripts directory) with two parameters; the sample VCF file and the output directory. The steps of the workflow are described below.  
Firstly, the VCF file is converted to another VCF file with the multiallelic variants splitted into separate records by a bcftools norm command and the INFO and FORMAT fields are removed (keeping genotypes only) by bcftools annotate.  
The resulting VCF is annotated using ANNOVAR software. This step produces three output files; avinput file, a tsv file with the annotations, and an annotated VCF file. The first two are not needed and, thus, removed along with the VCF file produced from the first step. The annotated VCF file is compressed and indexed with bgzip and tabix tools for downstream analysis.  
Both annotations and genotypes data are parsed from the VCF file using a bcftools query command into a TSV file per chromosome that passes through some processes which are:

1) remove an additional comma and space in samples dictionary  
2) replace "\x3b" with ";" (since semicolons are not allowed within VCF fields)  
3) replace NA and "." with empty strings for the database numerical columns.

This outputs 25 files called (${sample}\_${chr}.tsv), each is passed to `generate_genotypes.py` script because the samples-related data need to be calculated first since each variant should be checked first using a conventional SELECT statement if it occurred before to increment its count by new total and append the new sample to the pre-existing list of samples or if it is novel to add the first sample. This outputs another 25 files called (${sample}\_${chr}.tsv.updated). To save up space, the original TSV (${sample}\_${chr}.tsv) is removed.  
Regarding the data import, columns are imported from the (${sample}\_${chr}.tsv.updated) file with the `dsbulk_annotations.sh` script in a way to handle the data model of each data type in the most efficient way.  
The final TSVs (${sample}\_${chr}.tsv.updated) are concatenated and piped to `gzip -9` to reduce the file size.

# **6. Modeling and Query Optimization**

The rationale behind the chosen data model is to provide a flexible and scalable structure for storing genetic variant data while maintaining data integrity and minimizing redundancy. The current data model used in the created table is as follows:

1) The genetic variants data in 4 columns (construct the primary key):  
   1) chromosome (chr1, chr2, chr3, …, chrX, chrY, chrM): main partition column that MUST be included in any query.  
   2) position (integer): for the location of altered bases (as in the VCF). (clustering column 1\)  
   3) reference: for the reference base in the VCF file. (clustering column 2\)  
   4) alternate: for the alternate base in the VCF file. (clustering column 3\)  
2) Variant data in samples is represented in 3 *integer* columns (variant\_count, variant\_homozygous, variant\_heterozygous).  
3) Sample IDs and genotypes are represented in the *text* column (variant\_samples) but parsed as a python3 dictionary while importing a new sample.  
4) Gene-based data are stored in *text* based columns for gene name, variant effect (exonic, intronic, …), consequence (synonyms, stopgain, …), and so on.  
5) Population frequency columns  (gnomAD, 1kGP, and GME) have the *text (can be double)* data type.  
6) Clinical-related annotations are represented in *text* columns.  
7) Computational tools for variant interpretations are shown in multiple columns of various types according to the output type of each tool (e.g, SIFT, PolyPhen, CADD, …).

The schema of the database allows searching tables by their PRIMARY KEYS only since their definitions show how the data is partitioned and clustered among nodes. The PK columns are the chromosome, position, reference, and alternate, to allow searching by a certain variant and also by a genomic region to return all variants present in this region.  
To allow searching by other columns that are not included in the PK definition, an SAI index can be created for this certain column using a CQL command:  
`cqlsh>>> CREATE INDEX col_sai_idx ON keyspace_name.table_name (column_name) USING 'sai';`  
Currently, there are 5 columns indexed with this strategy (dbsnp, effect, consequence, intervar, and variant_count). It MUST be noted that indexing can add extra overhead, cause performance issues, and lead to inconsistent data if not used properly, especially on highly diverse and frequently updated columns.

# **7. Web App Development**

For the ease of access of the data, a simple web application was created using a micro web framework written in Python called Flask that can be initiated by navigating to the directory of the flask project db_app/ and running the python script `app.py`. The structure of the flask project directory is as follows:  
```
db_app		# main Flask application directory  
├── app.py	# Flask application core script  
├── genes.py	# human gene names and coordinates  
├── static	# logos used in the web pages  
│   ├── *****.png  
│   ├── *****.png  
│   ├── *****.png  
│   ├── *****.png  
│   └── *****.png
├── templates		# html pages (with their built-in javascripts and css styles)  
│   ├── invalid_credentials.html	# in case of entering invalid username/password  
│   ├── login.html		# login webpage  
│   ├── no_results.html	# in case of no matches found for the given search  
│   ├── results.html	# webpage to view the results table  
│   ├── search.html		# search input page  
│   └── select_keyspace.html	# to select the keyspace  
└── usr_dt.py			# users and their login credentials
```

# **8. Data Access**

The available user interface to navigate the database can be accessed through the link "http://10.10.100.171:5000" or the IP of remote server. Through this interface, users can perform queries by entering the search keyword in the given textbox, select the columns to display and then press the Search button. The following is a more comprehensive guide on how to utilize the interface.  
Typically, users can get a single specific variant by specifying the genomic coordinates and the base alterations (such as chr1-11,213,687-A-C). The chromosome and position can be entered without base alterations (chr1:10,747,852) to return all variants found in a single position. In addition, querying genomic regions is also available, in which users can enter the chromosome, start position and end position (chr1:10,000-20,000) to return all variants present within this region. Note that commas are allowed in numbers only (123456 or 123,456). Gene names are also allowed by writing gene={GeneName}, for example gene=BRCA2.   
For queries that don’t involve the PK columns, users can write the column name followed by an equal sign and then the search term (Case Sensitive), for example consequence=stopgain or effect=splicing. Users can also combine more than one column by using the ‘&&’ symbols. For example, if you want to get all known pathogenic variants in the BRCA2 gene, you can enter gene=BRCA2 && intervar=Pathogenic in the search box. Please note that when users perform a query that doesn’t involve the PK columns, the server automatically searches for each partition (chromosome) separately and then aggregates the results which takes more time and utilizes more resources. 

| Search Term | Explanation |
| :---: | :---: |
| **chr1-11,213,687-A-C** | return specific variant by coordinates and base alterations |
| **chr1:10,747,852** | return all variants found in a single position |
| **chr1:10,000-20,000** | return all variants present within this region |
| **gene=BRCA2** | Search specific columns (Case Sensitive) (same for dbsnp, effect,consequence, intervar, and variant_count) |
| **gene=BRCA2 && intervar=Pathogenic** | get all known pathogenic variants in the BRCA2 gene |

Whatever type of query performed, the search results will appear in a new page that will be opened containing rows corresponding to the given query. Users can further perform column-based filtrations by entering the keyword (case-insensitive) in the textbox below each column name and only the rows containing this keyword will appear. The following expressions can be used to increase the functionality of the column-based filtrations:

* The symbols **‘&’** and **‘|’** are used for AND / OR operations to search for multiple words.  
* Numerical comparisons can be done with **\>, \<, \>=,** and **\<=** symbols.  
* The symbol **‘^’** is used to invert the search (show unmatched rows).

Finally, the search results can be exported to a TSV file for better inspection, visualization and correlation of data with other external data sources and tools. The TSV file can be opened in any spreadsheet software such as Microsoft Excel, Google Sheets and LibreOffice Calc.

# **9. Backup & Recovery**

A general backup plan has been proposed to save a second copy of the data on external storages. This can be done in two ways:

1) Copy the whole database folder with all SStables, configs, and bins. (**the database server must be stopped**)
```
tar --zstd -cf ${backup_directory}/db-backup-jun24.tar.zst ${database_folder}

### To recover it, run the following command:
tar --zstd -xf ${backup_directory}/db-backup-jun24.tar.zst [ -C /path/to/extract/ ]
```

2) Save the annotation table as TSV. (**the database server must be running**)
```
dsbulk unload -k genvardb -t annotations -delim "\t" | gzip -9c > ./db-backup-${date}.tsv.gz

### For recovery, import the table with dsbulk load.
dsbulk load -k genvardb -t annotations -delim "\t" --connector.csv.compression gzip --connector.csv.maxCharsPerColumn -1 -url ./db-backup-jun24.tsv.gz
```

# **10. Security**

Security measures and access restrictions include creating a specific user ID and password for each researcher to control the access to the data. In addition, all IP addresses that try to access the API are logged and can be checked to see if there are any unknown devices.

# **11. Troubleshooting**

Some potential problems users might encounter are addressed in the following table:

| Problem | Possible Causes | Solution |
| ----- | ----- | ----- |
| Connection Issues | Incorrect credentials. - Network problem. | Verify username & password. - Check network connectivity. |
| Slow Queries Low Performance | Too much data in a single query. - Server overload. | Optimize query structures. - Monitor server load. |
| Query Timeout | Long-running queries. - Server overload. | Optimize queries. - Monitor server load. |
| Inconsistent data (missing data) | Improper data validation. - Too much I/O operations. - Server overload. | Implement data validation rules. - Use database transactions. - Monitor server load. |
| Backup/Restore Error | Insufficient disk space. - Corrupted backup files. - Permission issues. | \[pre-backup\] Ensure enough disk space. - \[post-backup\] Check backup file integrity. - Verify user permissions. |

# **12. Best Practices**

To effectively use the database system and perform faster queries, it is highly recommended to use the genomic coordinates in your queries at least by specifying the chromosome and position of the variant instead of searching by gene names or variant identifiers. Further filtrations can be applied in the results page using the column-based filtrations. The following points shows how to perform efficient queries:

* When looking for a specific variant, it is better to specify it in the form of "chr-pos-ref-alt".  
* If you have a variant ID (from dbSNP for example), you can get the variant coordinates (chr-pos) by looking it up in dbSNP and then query the variant by these coordinates.  
* Online resources use different positioning systems, especially in representing INDELs. If you can’t find a variant, search before and after the given position by 1 bp (for example, if your variant is at position 105, query the region from 104 to 106).

# **13. Conclusion**

The proposed database model for storing genetic variants, as detailed in this project, demonstrates the critical role of robust solution to efficiently manage genetic variant data produced in the large-scale genomics projects. The structured storage of these variants facilitates the study and analysis of genetic information, essential for advancing genetic research, disease association studies, and personalized medicine. This database enables researchers and clinicians to store, query, and analyze genetic information effectively, thus enhancing data management, analysis, and clinical interpretations.
