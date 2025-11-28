"""
Web Interface for GenVarDB-Cassandra
"""

import logging
from genes import genes
from usr_dt import usr_dt
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from flask import Flask, render_template, request, redirect, url_for

app = Flask("GenVarDB-Cassandra Database Web App")
app.secret_key = 'mmeDMT9rTPx80xLRoFIfQZUh0og5xpQeYV9mdPza8cGFYQrKNB'
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')

cluster = Cluster(auth_provider=auth_provider)
SESSION = None

users_login = usr_dt

# Better column names (as list of lists to select)
better_cols = [['chr', '"Chr"'], ['pos', '"Pos"'], ['ref', '"Ref"'], ['alt', '"Alt"'], ['effect', '"Effect"'], ['hgvs_c', '"Hgvs C"'], ['hgvs_p', '"Hgvs P"'], ['dbsnp', '"dbSNP"'],
               ['gene_symbol', '"Gene Symbol"'], ['gene_hgnc_id', '"Gene Hgnc Id"'], ['mane_select', '"Mane Select"'], ['exon_rank', '"Exon Rank"'], ['exon_count', '"Exon Count"'],
               ['intron_rank', '"Intron Rank"'], ['clinvar_disease', '"ClinVar Disease"'], ['clinvar_classification', '"ClinVar Classification"'], ['clinvar_review_status', '"ClinVar Review Status"'],
               ['clinvar_submissions_summary', '"ClinVar Submissions Summary"'], ['acmg_classification', '"ACMG Classification"'], ['omim', '"OMIM"'], ['omim_ids', '"OMIM IDs"'],
               ['matched_hpo_count', '"Matched HPO Count"'], ['matched_hpo_terms', '"Matched HPO Terms"'], ['acmg_score', '"ACMG Score"'], ['acmg_criteria', '"ACMG Criteria"'],
               ['acmg_by_gene', '"ACMG By Gene"'], ['revel_score', '"Revel Score"'], ['revel_prediction', '"Revel Prediction"'], ['alphamissense_score', '"AlphaMissense Score"'],
               ['alphamissense_prediction', '"AlphaMissense Prediction"'], ['bayesdelnoaf_score', '"Bayesdelnoaf Score"'], ['bayesdelnoaf_prediction', '"Bayesdelnoaf Prediction"'],
               ['phylop100way_score', '"Phylop100Way Score"'], ['phylop100way_prediction', '"Phylop100Way Prediction"'], ['spliceai_max_score', '"SpliceAI Max Score"'],
               ['spliceai_max_prediction', '"SpliceAI Max Prediction"'], ['dbscsnv_ada_score', '"Dbscsnv Ada Score"'], ['dbscsnv_ada_prediction', '"Dbscsnv Ada Prediction"'],
               ['apogee2_score', '"Apogee2 Score"'], ['apogee2_prediction', '"Apogee2 Prediction"'], ['gnomad_exomes_af', '"gnomAD Exome AF"'], ['gnomad_genomes_af', '"gnomAD Genome AF"'],
               ['phenotype_combined', '"Phenotype Combined"'], ['pathogenicity_classification_combined', '"Pathogenicity Classification Combined"'], ['ncbi_gene', '"NCBI Gene"'],
               ['omim_gene', '"OMIM Gene"'], ['aa_ref', '"AA Ref"'], ['aa_alt', '"AA Alt"'], ['aa_length', '"AA Length"'], ['aa_start', '"AA Start"'], ['canonical', '"Canonical"'],
               ['cdna_length', '"cDNA Length"'], ['cdna_start', '"cDNA Start"'], ['cds_length', '"CDS Length"'], ['cds_start', '"CDS Start"'],
               ['computational_prediction_selected', '"Computational Prediction Selected"'], ['computational_score_selected', '"Computational Score Selected"'],
               ['computational_source_selected', '"Computational Source Selected"'], ['allele_count_ref_population', '"Allele Count ref Population"'],
               ['frequency_ref_population', '"Frequency ref Population"'], ['hom_count_ref_population', '"Hom Count ref Population"'], ['gnomad_exomes_ac', '"gnomAD Exomes Ac"'],
               ['gnomad_exomes_homalt', '"gnomAD Exomes Homalt"'], ['gnomad_genomes_ac', '"gnomAD Genomes Ac"'], ['gnomad_genomes_homalt', '"gnomAD Genomes Homalt"'],
               ['gnomad_mito_heteroplasmic', '"gnomAD Mito Heteroplasmic"'], ['gnomad_mito_homoplasmic', '"gnomAD Mito Homoplasmic"'], ['mitotip_prediction', '"Mitotip Prediction"'],
               ['mitotip_score', '"Mitotip Score"'], ['protein_coding', '"Protein Coding"'], ['protein_id', '"Protein ID"'], ['splice_prediction_selected', '"Splice Prediction Selected"'],
               ['splice_score_selected', '"Splice Score Selected"'], ['splice_source_selected', '"Splice Source Selected"'], ['strand', '"Strand"'], ['transcript_support_level', '"Transcript Support Level"'],
               ['diseases_description', '"Diseases Description"'], ['hpo_ids', '"HPO Ids"'], ['hpo_terms', '"HPO Terms"'], ['gene_description', '"Gene Description"'], ['variant_count', '"Variant Count"'],
               ['variant_homozygous', '"Variant Homozygous"'], ['variant_heterozygous', '"Variant Heterozygous"'], ['variant_samples', '"Variant Samples"']
               ]

@app.route('/', methods=['GET', 'POST'])
def login():
    """
    This function renders the login page where the users enter their credentials.
    If verified, the program proceeds to the next step.
    If not, it raises an error due to invalid credentials.
    """
    global username
    if request.method == 'POST':
        username = request.form['username'].strip().lower()
        password = request.form['password'].strip().lower()

        if bool(users_login.get(username)) and users_login[username] == password:
            return redirect(url_for('select_keyspace'))
        return render_template('invalid_credentials.html')

    return render_template('login.html')

@app.route('/select_keyspace', methods=['GET', 'POST'])
def select_keyspace():
    """
    This function renders a web page where the users select the database to navigate.
    """
    global SESSION
    cluster.connect()
    keyspaces = [i for i in cluster.metadata.keyspaces.keys() if 'system' not in i]
    if request.method == 'POST':
        keyspace = request.form['keyspace']
        SESSION = cluster.connect(keyspace)
        return redirect(url_for('search'))
    return render_template('select_keyspace.html', keyspaces=keyspaces)

# Route for the button action
@app.route('/execute_queries', methods=['POST'])
def execute_queries():
    results_data = {}

    query1 = "SELECT * FROM genvardb.annotations WHERE variant_count > 10 AND acmg_classification = 'Pathogenic' ALLOW FILTERING"
    query2 = "SELECT * FROM genvardb.annotations WHERE variant_count > 10 AND acmg_classification = 'Likely_pathogenic' ALLOW FILTERING"
    
    result1 = SESSION.execute(query1)
    result2 = SESSION.execute(query2)

    annotations_results = list(result1) + list(result2)

    if len(annotations_results) > 0:
        results_data["Gene Panel"] = annotations_results
    else:
        annotations_results = []
    return render_template('results.html', results_data=results_data)


@app.route('/search', methods=['GET', 'POST'])
def search():
    """
    This function renders the main query page where the users
    (1) enter their query,
    (2) select the tables to search in,
    and (3) select the columns they want to display in the results.
    """
    
    SESSION.default_fetch_size = 10000
    
    global tables_columns_dict
    if request.method == 'POST':
        search_pattern = request.form['search_pattern'].strip()
        tables = request.form.getlist('tables')
        columns = request.form.getlist('columns')
        return redirect(url_for('results',
                                search_pattern=search_pattern, tables=tables, columns=columns))

    tables = SESSION.execute(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{SESSION.keyspace}'")
    columns = SESSION.execute(f"SELECT table_name, column_name FROM system_schema.columns WHERE keyspace_name = '{SESSION.keyspace}'")

    table_list = [table.table_name for table in sorted(tables, reverse=True)]
    tables_columns_dict = {}
    for column in sorted(columns, reverse=True):
        if column.table_name not in tables_columns_dict:
            tables_columns_dict[column.table_name] = []
        tables_columns_dict[column.table_name].append(column.column_name)

    return render_template('search.html', tables=table_list, columns=tables_columns_dict)

@app.before_request
def suppress_logs():
    if request.path == '/results':
        log = logging.getLogger('werkzeug')
        log.setLevel(logging.ERROR)


@app.route('/results', methods=['GET'])
def results():
    """
    This function renders the results page containing
    (1) the table(s) with the matching records,
    and (2) a download buttom for each table to export it as TSV.
    """
    global results_data

    search_pattern = request.args.get('search_pattern')
    tables = request.args.getlist('tables')
    columns = request.args.getlist('columns')

    search_pattern = search_pattern.strip().replace(" ","").replace(",","").replace(":","-").replace("\t","-")
    location_search = True

    if search_pattern.count('-') == 3:
        chr, pos, ref, alt = search_pattern.split('-')
        query_conditions = f"chr = '{chr}' AND pos = {pos} AND ref = '{ref}' AND alt = '{alt}'"
    elif search_pattern.count('-') == 2:
        chr, begin, end = search_pattern.split('-')
        query_conditions = f"chr = '{chr}' AND pos >= {begin} AND pos <= {end}"
    elif search_pattern.count('-') == 1:
        chr, pos = search_pattern.split('-')
        query_conditions = f"chr = '{chr}' AND pos = {pos}"
    else:
        location_search = False
        
        query_list = []

        parameters = search_pattern.split('&&')

        for parameter in parameters:
            if '=' in parameter:
                query_parse = parameter.split('=')
                if query_parse[0] == 'gene':
                    chr, begin, end = genes[query_parse[1]].split('-')
                    query_list.append(f"chr = '{chr}' AND pos >= {begin} AND pos <= {end}")
                    location_search = True
                else:
                    query_list.append(f"{query_parse[0]} = '{query_parse[1]}'")               
            elif '>' in parameter:
                query_parse = parameter.split('>')
                query_list.append(f"{query_parse[0]} > {query_parse[1]}")
            elif '<' in parameter:
                query_parse = parameter.split('<')
                query_list.append(f"{query_parse[0]} < {query_parse[1]}")
            elif '>=' in parameter:
                query_parse = parameter.split('>=')
                query_list.append(f"{query_parse[0]} >= {query_parse[1]}")
            elif '<=' in parameter:
                query_parse = parameter.split('<=')
                query_list.append(f"{query_parse[0]} <= {query_parse[1]}")

        query_conditions = ' AND '.join(query_list)
         
    results_data = {}

    for table in tables:
        cols_string = ' '.join([i.split('.')[1] for i in sorted(columns, reverse=True) if i.split('.')[0] == table])
        cols_to_display = ', '.join(f"{i[0]} as {i[1]}" for i in better_cols if i[0] in cols_string)
        annotations_results = []
        query = f"SELECT {cols_to_display} FROM {table} WHERE {query_conditions} ALLOW FILTERING"
        rows = SESSION.execute(query)
        annotations_results = list(rows)

        if len(annotations_results) > 0:
            results_data[table.title()] = annotations_results
        else:
            annotations_results = []
   
    if bool(results_data):
        return render_template('results.html', results_data=results_data)
    else:
        return render_template('no_results.html')    

if __name__ == '__main__':
    app.run(host = '0.0.0.0',
            port = 5000,
            debug = True)
