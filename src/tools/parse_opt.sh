#!/bin/bash
set -e

while (( "$#" )); do
    case "$1" in
        --s3_path)
            if [ -n "$2" ] && [ ${2:0:1} != "-" ]; then
                S3_PATH=$2
                shift 2
            else
                echo "Error: Argument for $1 is missing" >&2
                exit 1
            fi
            ;;
        --target_date)
            if [ -n "$2" ] && [ ${2:0:1} != "-" ]; then
                TARGET_DATE=$2
                shift 2
            else
                echo "Error: Argument for $1 is missing" >&2
                exit 1
            fi
            ;;
        *)
            echo "Skip unknown option: $1" >&2
            shift 1
            ;;
    esac
done
