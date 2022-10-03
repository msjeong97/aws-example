#!/bin/bash
set -e

if [[ $# -ne 4 ]]
then
  echo "usage: bash $0 --s3_path \${path} --target_date \${date}"
  exit 1
fi

arguments=$@
. $( dirname $0 )/../../tools/parse_opt.sh $arguments

echo "TARGET_DATE: ${TARGET_DATE}"
echo "S3_PATH:     ${S3_PATH}"

sub_path_file="path.txt"
size_file="size.txt"

echo "" > ${size_file}

aws s3 ls ${S3_PATH} | grep ${TARGET_DATE} | awk '{ print $2 }' > ${sub_path_file}


for SUB_PATH in $(cat ${sub_path_file})
do
  echo ${SUB_PATH}
  aws s3 ls ${S3_PATH}${SUB_PATH} --summarize | tail -n 1 | awk '{ print $3}' >> ${size_file}
done

cat ${size_file} | awk 'BEGIN {total=0}{total+=$1}END{print total/1024/1024/1024" GB"}'

rm ${sub_path_file}
rm ${size_file}
