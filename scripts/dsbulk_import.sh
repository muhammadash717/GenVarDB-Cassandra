#!/bin/bash

# Script      : dsbulk_import.sh
# Description : This script imports the data from TSV into the specified cassandra table/database.
# NOTE        : You need to make sure that all defaults arguments are correct.

# Author : Muhammad Ashraf
# GitHub : https://github.com/muhammadash717/

# Declare an associative array for arguments and default values
declare -A args
declare -A defaults
required_args=("tsv")

# Default values for optional arguments
defaults=(
    ["dsbulk-path"]='./dsbulk-1.11.0/bin/dsbulk'
    ["log-directory"]='./logs'
    ["compression"]='none'
    ["delimiter"]='"\t"'
    ["keyspace"]="genvardb"
    ["table"]="annotations"
)

# Usage message if the script run incorrectly
usage() {
    echo -e "\nUsage:\n\tbash $(basename $0)"
    for arg in "${required_args[@]}"; do
        echo -ne "\t\t"; echo "--$arg <value>"
    done
    for key in "${!defaults[@]}"; do
        echo -ne "\t\t"; echo "[ --$key ${defaults[$key]} ]"
    done
    exit 1
}

# Parse arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --*) 
            key="${1/--/}"  # Strip the leading '--'
            if [[ -z "$2" || "$2" == --* ]]; then
                echo "ERROR: Missing value for argument $1"; usage
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
        echo "ERROR: --$arg is a required argument."; usage
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

require_path ${args[tsv]}
require_path ${args[dsbulk-path]}

DSBULK=${args[dsbulk-path]}
LOG_DIR=${args[log-directory]}
mkdir -p $LOG_DIR

INPUT_TSV=${args[tsv]}

$DSBULK load \
    --connector.name csv \
    --connector.csv.compression ${args[compression]} \
    --connector.csv.url $INPUT_TSV \
    --connector.csv.delimiter ${args[delimiter]} \
    --connector.csv.header true \
    --connector.csv.maxColumns $(head -1 ${args[tsv]} | tr ${args[delimiter]} '\n' | wc -l) \
    --connector.csv.maxCharsPerColumn -1 \
    --schema.keyspace ${args[keyspace]} \
    --schema.table ${args[table]} \
    --schema.allowMissingFields true \
    --driver.basic.contact-points ["127.0.0.1"] \
    --driver.basic.default-port 9042 \
    --driver.basic.request.timeout "120 minutes" \
    --driver.advanced.retry-policy.max-retries 100 \
    --driver.advanced.continuous-paging.timeout.first-page "120 minutes" \
    --driver.advanced.continuous-paging.timeout.other-pages "120 minutes" \
    --driver.advanced.heartbeat.timeout "120 minutes" \
    --monitoring.reportRate "2 seconds" \
    --log.directory $LOG_DIR



##### In case of authentication #####
    # --driver.advanced.auth-provider.class PlainTextAuthProvider \
    # --driver.advanced.auth-provider.username cassandra \
    # --driver.advanced.auth-provider.password cassandra \

