#!/bin/bash

# Description : The script  (1) annotates a VCF file using ANNOVAR
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
    ["annovar-path"]='./cassandra_db/annotation/annovar'
    ["dsbulk-import"]='./cassandra_db/updated_scripts/dsbulk_import.sh'
    ["generate-genotypes"]='./cassandra_db/updated_scripts/generate_genotypes.py'
    ["cassandra"]="./cassandra_db/apache-cassandra-5.0.0/bin/cassandra"
    ["tabix"]='tabix'
    ["bgzip"]='bgzip'
    ["bcftools"]='bcftools'
    ["threads"]=4
    ["flag-dir"]="./cassandra_db/flags"
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
require_path ${args[annovar-path]}
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
if [ ! -f "${args[flag-dir]}/${sample_name}_annovar.DONE" ]; then # Check the flag file and start the process if not exists.
    
    attempt=1

    while [ $attempt -le ${args[max-retry]} ]; do # Starting the process for the specified number of attempts.
        
        echo -e "[`date`]\tStep 2: Annotating (Attempt $attempt)"
        perl ${args[annovar-path]}/table_annovar.pl ${output_prefix}.norm.vcf.gz ${args[annovar-path]}/humandb/ -buildver hg38 -out ${output_prefix} -thread ${args[threads]} -maxgenethread ${args[threads]} -remove -protocol refGeneWithVer,intervar_20180118,ALL.sites.2015_08,dbnsfp47a -operation g,f,f,f -nastring . -polish -vcfinput &> ${output_prefix}_annovar.log
        
        if [ $? -eq 0 ]; then # In case of success, not needed files removed, flag file created and the while loop exits.
            ${args[bgzip]} --threads `nproc` --force ${output_prefix}.hg38_multianno.vcf
            ${args[tabix]} --force ${output_prefix}.hg38_multianno.vcf.gz
            rm -f ${output_prefix}.avinput ${output_prefix}.hg38_multianno.txt ${output_prefix}.norm.vcf.gz
            touch "${args[flag-dir]}/${sample_name}_annovar.DONE"
            echo -e "[`date`]\tStep 2: Annotation Completed."
            break
        else    # In case of failure, retry.
            touch "${args[flag-dir]}/${sample_name}_annovar.FAIL.$attempt"
            attempt=$((attempt + 1)); if [ $attempt -gt ${args[max-retry]} ]; then exit 1; fi
            echo -e "[`date`]\tStep 2: Annotation failed. Retrying."
        fi
    done

else # Skipping the step if the "DONE" flag file was found.
    echo -e "[`date`]\tStep 2: Annotation already completed. Skipping."

fi

# Step 3: VCF parsing & Generating Genotypes.
for chr in $(${args[tabix]} -l ${output_prefix}.hg38_multianno.vcf.gz); do

    if [ ! -f "${args[flag-dir]}/${sample_name}_parsing_${chr}.DONE" ]; then # Check the flag file and start the process if not exists.
    
        (
            attempt=1

            while [ $attempt -le ${args[max-retry]} ]; do # Starting the process for the specified number of attempts.
                echo -e "[`date`]\tStep 3: VCF parsing & Generating Genotypes for ${chr} (Attempt $attempt)"
                
                ( echo -e "chromosome\tposition\treference\talternate\teffect\tgene\tconsequence\tamino_acid\tintervar\tall_1kg\tsift_pred\trevel_score\tcadd_phred\tprimateai_pred\tclinpred_pred\talphamissense_pred\tdann_score\tvariant_samples\tvariant_count\tvariant_homozygous\tvariant_heterozygous" > ${output_prefix}_${chr}.tsv && ${args[bcftools]} query -r ${chr} -e 'FORMAT/GT == "./." | FORMAT/GT == "." | FORMAT/GT == ".|." | FORMAT/GT == "0/0" | FORMAT/GT == "0" | FORMAT/GT == "0|0"' -f "%CHROM\t%POS\t%REF\t%ALT\t%INFO/Func.refGeneWithVer\t%INFO/Gene.refGeneWithVer\t%INFO/ExonicFunc.refGeneWithVer\t%INFO/AAChange.refGeneWithVer\t%INFO/InterVar_automated\t%INFO/ALL.sites.2015_08\t%INFO/SIFT_pred\t%INFO/REVEL_score\t%INFO/CADD_phred\t%INFO/PrimateAI_pred\t%INFO/ClinPred_pred\t%INFO/AlphaMissense_pred\t%INFO/DANN_score\t{[\"%SAMPLE\":\"%GT\", ]}\n" ${output_prefix}.hg38_multianno.vcf.gz | sed 's/, }/}/g' | sed 's/\\x3b/;/g' | awk 'BEGIN{OFS=FS="\t"} { for(i=1; i<=NF; i++) { if($i == "." || $i == "NA") $i = ""; }} 1' >> ${output_prefix}_${chr}.tsv && python3 ${args[generate-genotypes]} ${output_prefix}_${chr}.tsv && rm -f ${output_prefix}_${chr}.tsv ) 2> ${output_prefix}_parsing.log

                if [ $? -eq 0 ]; then # In case of success, the flag file created and the while loop exits.
                    touch "${args[flag-dir]}/${sample_name}_parsing_${chr}.DONE"
                    echo -e "[`date`]\tStep 3: VCF parsing & Generating Genotypes for ${chr} Completed."
                    break
                else    # In case of failure, retry.
                    touch "${args[flag-dir]}/${sample_name}_parsing_${chr}.FAIL.$attempt"
                    attempt=$((attempt + 1)); if [ $attempt -gt ${args[max-retry]} ]; then exit 1; fi
                    echo -e "[`date`]\tStep 3: VCF parsing & Generating Genotypes for ${chr} failed. Retrying."
                fi
            done
        ) &

    else # Skipping the step if the "DONE" flag file was found.
        echo -e "[`date`]\tStep 3: VCF parsing & Generating Genotypes for ${chr} already completed. Skipping."

    fi
done
wait

# Step 4: Importing Data into the database. 
for chr in $(${args[tabix]} -l ${output_prefix}.hg38_multianno.vcf.gz); do

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