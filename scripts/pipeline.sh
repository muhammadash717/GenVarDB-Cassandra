#!/bin/bash

# Description : The script  (1) annotates a VCF file using GeneBe
#                           (2) prep for database import
# Usage       : bash pipeline.sh --vcf <vcf_file> --output-directory <output_path>

# Author : Muhammad Ashraf
# GitHub : https://github.com/muhammadash717/

declare -A args
declare -A defaults

# Required arguments
required_args=("vcf" "output-directory")

# Optional arguments with default values
defaults=(
    ["genebe-path"]='./scripts'
    ["dsbulk-import"]='./scripts/dsbulk_import.sh'
    ["generate-genotypes"]='./scripts/generate_genotypes.py'
    ["cassandra"]="./apache-cassandra-5.0.6/bin/cassandra"
    ["tabix"]='tabix'
    ["bgzip"]='bgzip'
    ["bcftools"]='bcftools'
    ["threads"]=4
    ["flag-dir"]="./flags"
    ["max-retry"]=3
)

# Usage message for when the script is run incorrectly
usage() {
    echo -ne "\nUsage:\n\tbash $0 "
    for arg in "${required_args[@]}"; do
        echo -ne "--$arg <value> "
    done
    for key in "${!defaults[@]}"; do
        echo -ne "[--$key ${defaults[$key]}] "
    done
    echo ""
    exit 1
}

# Parse arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --*) 
            key="${1/--/}"  # Strip the leading '--'
            if [[ -z "$2" || "$2" == --* ]]; then
                echo "ERROR: Missing value for argument $1"
                usage
            fi
            args["$key"]="$2"
            shift ;;
        *) echo "ERROR: Unknown parameter passed: $1"; usage ;;
    esac
    shift
done

# Check for required arguments
for arg in "${required_args[@]}"; do
    if [[ -z "${args[$arg]}" ]]; then
        echo "ERROR: --$arg is a required argument."
        usage
    fi
done

# Set defaults for optional arguments if not provided
for key in "${!defaults[@]}"; do
    if [[ -z "${args[$key]}" ]]; then
        args["$key"]="${defaults[$key]}"
    fi
done

require_path() {
    local path="$1"

    if [ ! -e "$path" ]; then
        echo "Error: File or directory does not exist: $path" >&2
        exit 1
    fi
}

require_path ${args[vcf]}
require_path ${args[genebe-path]}
require_path ${args[dsbulk-import]}
require_path ${args[generate-genotypes]}
require_path ${args[cassandra]}

# Output the values of all arguments
echo "Arguments in use:"
for key in "${!args[@]}"; do
    echo -e "--$key ${args[$key]}"
done


mkdir -p ${args[flag-dir]}
mkdir -p ${args[output-directory]}

sample_name=$(basename ${args[vcf]%.vcf.gz})
output_prefix=${args[output-directory]}/${sample_name}
num_samples=`${args[bcftools]} query -l ${args[vcf]} | wc -l | cut -f1 -d' '`

echo -e "Number of samples: ${num_samples}\n"


# Step 1: Splitting multiallelic records in the VCF.
if [ ! -f "${args[flag-dir]}/${sample_name}_split.DONE" ]; then # Check the flag file and start the process if not exists.
    
    attempt=1

    while [ $attempt -le ${args[max-retry]} ]; do # Starting the process for the specified number of attempts.
        
        echo -e "[`date`]\tStep 1: Splitting multiallelics. (Attempt $attempt)"
        ${args[bcftools]} annotate --threads `nproc` -x INFO,^FORMAT/GT ${args[vcf]} | ${args[bcftools]} norm -m-both --threads `nproc` -Oz -o ${output_prefix}.norm.vcf.gz &> ${output_prefix}_norm.log   
        
        if [ $? -eq 0 ]; then # In case of success, the flag file created and the while loop exits.
            touch "${args[flag-dir]}/${sample_name}_split.DONE"
            echo -e "[`date`]\tStep 1: Splitting multiallelics Completed."
            break
        else    # In case of failure, retry.
            touch "${args[flag-dir]}/${sample_name}_split.FAIL.$attempt"
            attempt=$((attempt + 1)); if [ $attempt -gt ${args[max-retry]} ]; then exit 1; fi
            echo -e "[`date`]\tStep 1: Splitting multiallelics failed. Retrying."
        fi
    done

else # Skipping the step if the "DONE" flag file was found.
    echo -e "[`date`]\tStep 1: Splitting multiallelics already completed. Skipping."

fi


# Step 2: Annotating the splitted VCF.
if [ ! -f "${args[flag-dir]}/${sample_name}_genebe.DONE" ]; then # Check the flag file and start the process if not exists.
    
    attempt=1

    while [ $attempt -le ${args[max-retry]} ]; do # Starting the process for the specified number of attempts.
        
        echo -e "[`date`]\tStep 2: Annotating (Attempt $attempt)"
        python3 ${args[genebe-path]}/genebe_annotate_vcf.py --input_vcf ${output_prefix}.norm.vcf.gz --output_tsv ${output_prefix}_raw.tsv &> ${output_prefix}_genebe.log
        python3 ${args[genebe-path]}/genebe2html.py ${output_prefix}_raw.tsv &>> ${output_prefix}_genebe.log

        if [ $? -eq 0 ]; then # In case of success, not needed files removed, flag file created and the while loop exits.
            rm ${output_prefix}_raw.tsv
            touch "${args[flag-dir]}/${sample_name}_genebe.DONE"
            echo -e "[`date`]\tStep 2: Annotation Completed."
            break
        else    # In case of failure, retry.
            touch "${args[flag-dir]}/${sample_name}_genebe.FAIL.$attempt"
            attempt=$((attempt + 1)); if [ $attempt -gt ${args[max-retry]} ]; then exit 1; fi
            echo -e "[`date`]\tStep 2: Annotation failed. Retrying."
        fi
    done

else # Skipping the step if the "DONE" flag file was found.
    echo -e "[`date`]\tStep 2: Annotation already completed. Skipping."

fi

# Step 3: Parsing Genotypes.
for chr in $(${args[tabix]} -l ${args[vcf]}); do

    if [ ! -f "${args[flag-dir]}/${sample_name}_parsing_${chr}.DONE" ]; then # Check the flag file and start the process if not exists.
    
        (
            attempt=1

            while [ $attempt -le ${args[max-retry]} ]; do # Starting the process for the specified number of attempts.
                echo -e "[`date`]\tStep 3: Parsing Genotypes for ${chr} (Attempt $attempt)"
                
                (   head -1 ${output_prefix}_annotation.tsv | sed -E 's/$/\tvariant_count\tvariant_homozygous\tvariant_heterozygous/1' > ${output_prefix}_${chr}.tsv
                    grep -P "^${chr//chr/}\s" ${output_prefix}_annotation.tsv | awk 'BEGIN{OFS=FS="\t"} { for(i=1; i<=NF; i++) { if($i == "." || $i == "NA") $i = ""; }} 1' >> ${output_prefix}_${chr}.tsv
                    python3 ${args[generate-genotypes]} ${output_prefix}_${chr}.tsv
                    rm -f ${output_prefix}_${chr}.tsv
                ) 2> ${output_prefix}_parsing.log

                if [ $? -eq 0 ]; then # In case of success, the flag file created and the while loop exits.
                    touch "${args[flag-dir]}/${sample_name}_parsing_${chr}.DONE"
                    echo -e "[`date`]\tStep 3: Parsing Genotypes for ${chr} Completed."
                    break
                else    # In case of failure, retry.
                    touch "${args[flag-dir]}/${sample_name}_parsing_${chr}.FAIL.$attempt"
                    attempt=$((attempt + 1)); if [ $attempt -gt ${args[max-retry]} ]; then exit 1; fi
                    echo -e "[`date`]\tStep 3: Parsing Genotypes for ${chr} failed. Retrying."
                fi
            done
        ) &

    else # Skipping the step if the "DONE" flag file was found.
        echo -e "[`date`]\tStep 3: Parsing Genotypes for ${chr} already completed. Skipping."

    fi
done
wait

# Step 4: Importing Data into the database. 
for chr in $(${args[tabix]} -l ${args[vcf]}); do

    if [ ! -f "${args[flag-dir]}/${sample_name}_import_${chr}.DONE" ]; then # Check the flag file and start the process if not exists.

        attempt=1

        while [ $attempt -le ${args[max-retry]} ]; do # Starting the process for the specified number of attempts.
            echo -e "[`date`]\tStep 4: Importing ${chr} into the database (Attempt $attempt)"
            
            bash ${args[dsbulk-import]} --tsv ${output_prefix}_${chr}.tsv.updated

            if [ $? -eq 0 ]; then # In case of success, the flag file created and the while loop exits.
                touch "${args[flag-dir]}/${sample_name}_import_${chr}.DONE"
                echo -e "[`date`]\tStep 4: Importing ${chr} into the database Completed"
                break
            else    # In case of failure, retry.
                touch "${args[flag-dir]}/${sample_name}_import_${chr}.FAIL.$attempt"
                attempt=$((attempt + 1)); if [ $attempt -gt ${args[max-retry]} ]; then exit 1; fi
                echo -e "[`date`]\tStep 4: Importing ${chr} into the database failed. Retrying."
                sleep 15
                ${args[cassandra]} # Relaunch the database server just in case it crashed.
                sleep 2m
            fi
        done

    else # Skipping the step if the "DONE" flag file was found.
        echo -e "[`date`]\tStep 4: Importing ${chr} into the database already completed. Skipping."
    fi

done


# Step 5: Cleaning up the data files.
if [ ! -f "${args[flag-dir]}/${sample_name}_cleanup.DONE" ]; then # Check the flag file and start the process if not exists.
    
    attempt=1

    while [ $attempt -le ${args[max-retry]} ]; do # Starting the process for the specified number of attempts.
        
        echo -e "[`date`]\tStep 5: Cleaning up the data files (Attempt $attempt)"

        cat ${output_prefix}_*.tsv.updated | gzip -9c > ${output_prefix}.tsv.updated.gz && rm -f ${output_prefix}_*.tsv.updated

        if [ $? -eq 0 ]; then # In case of success, the flag file created and the while loop exits.
            touch "${args[flag-dir]}/${sample_name}_cleanup.DONE"
            echo -e "[`date`]\tStep 5: Cleaning up the data files Completed."
            break
        else    # In case of failure, retry.
            touch "${args[flag-dir]}/${sample_name}_cleanup.FAIL.$attempt"
            attempt=$((attempt + 1)); if [ $attempt -gt ${args[max-retry]} ]; then exit 1; fi
            echo -e "[`date`]\tStep 5: Cleaning up the data files failed. Retrying."
        fi
    done

else # Skipping the step if the "DONE" flag file was found.
    echo -e "[`date`]\tStep 5: Cleaning up the data files already completed. Skipping."

fi

echo -e "[`date`]\tCompleted!"