    #!/usr/bin/env python3

"""
Script to add sample genotypes to the existing ones the database.
The script updates the current table with the new sample.

Usage:
    python3 generate_genotypes.py <genotypes_file>

Author : Muhammad Ashraf
GitHub : https://github.com/muhammadash717/
"""

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime
import sys, os
from time import time
from datetime import timedelta

query = None                  # to be used downstream
DELIMITER = '\t'              # delimiter used in the file
KEYSPACE = 'genvardb'         # keyspace name
TABLE = 'annotations'         # table name

genotypes_file = sys.argv[1]    # path of the input tsv file

def require_path(path: str):
    if not os.path.exists(path):
        print(f"Error: File or directory does not exist: {path}", file=sys.stderr)
        sys.exit(1)

require_path(genotypes_file)

with open(genotypes_file, "r", encoding="utf-8") as f:
    sample_name = genotypes_file.split('/')[-1].split('.')[0]   # extract sample name
    new_variants = f.read().strip().split('\n')
    
logfile_path = f"./logs/{sample_name}_genotypes.log"
os.makedirs(os.path.dirname(logfile_path), exist_ok=True)

def now(dt_only=False):
    """
    Function to print current timestamp for the log file
    """
    if dt_only:
        s = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    else:
        s = datetime.now().strftime('%A %d-%m-%Y %I:%M:%S %p')
    return s

# Function to check if a genotype is homozygous
def is_homozygous(genotype):
    """
    Check if a genotype is homozygous.
    Args: <str> The genotype to check.
    Returns: <bool> True if the genotype is homozygous, False otherwise.
    """
    return bool(genotype[0] == genotype[-1])

# Connecting to the cassandra cluster
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(['127.0.0.1'], auth_provider=auth_provider)
session = cluster.connect(KEYSPACE)

with open(logfile_path, 'w') as log_file:   # Creating the log file upon successful conncetion
    log_file.write(f"{now()}\t[INFO]\tConnected to the Cassandra Cluster.\n")

# Preparing the query statement to check existence of the variants
query_statement = session.prepare(f"SELECT * FROM {TABLE} WHERE chr = ? AND pos = ? AND ref = ? AND alt = ?")

all_count = 0
novel_count = 0
t0 = time()

output_file = open(genotypes_file + ".updated", "w")
output_file.write(new_variants[0] + "\n")

for variant in new_variants[1::]:
    row = variant.split(DELIMITER)
    new_samples = eval(row[-1])
    chr, pos, ref, alt = row[0:4]

    # Check for the existence of the variant
    query = session.execute(query_statement, (chr, int(pos), ref, alt)).one()
    
    if bool(query) and str(type(query.variant_samples)) != "<class 'NoneType'>": # pre-existing variant
        samples_dict = eval(query.variant_samples) # convert from string to python dictionary
        samples_dict.update(new_samples)
        
    else:   # novel variant
        samples_dict = new_samples
        novel_count += 1

    count_homozygous = sum([1 for i in samples_dict if is_homozygous(samples_dict[i])])     # counting the number of homozygous samples (will get the hetero by subtracting from count)
    samples_str = str(samples_dict).replace("'",'"') # reformatting as string
    all_count += 1

    output_file.write("\t".join(row[:-1]) + "\t" + "\t".join([samples_str, str(len(samples_dict)), str(count_homozygous), str(len(samples_dict)-count_homozygous)]) + "\n")

output_file.close()

with open(logfile_path, 'a') as log_file:
    log_file.write(f"{now()}\t[INFO]\tNumber of Total variants: {all_count}\n")
    log_file.write(f"{now()}\t[INFO]\tNumber of Novel variants: {novel_count}\n")
    log_file.write(f"{now()}\t[INFO]\t{sample_name}\t({str(timedelta(seconds=int(time()-t0)))})\n")

# Connection termination    
session.shutdown()
cluster.shutdown()

with open(logfile_path, 'a') as log_file:
    log_file.write(f"{now()}\t[INFO]\tConnection Terminated.\n")

exit()