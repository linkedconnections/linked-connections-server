#!/bin/bash
COMPANY_NAME="$1"
FILE_NAME="$2"
STORAGE="$3"

sort -T ${STORAGE}/tmp/ -t \" -k 17 ${STORAGE}/real_time/${COMPANY_NAME}/${FILE_NAME}.jsonld > ${STORAGE}/real_time/${COMPANY_NAME}/${FILE_NAME}_tmp.jsonld
rm ${STORAGE}/real_time/${COMPANY_NAME}/${FILE_NAME}.jsonld
mv ${STORAGE}/real_time/${COMPANY_NAME}/${FILE_NAME}_tmp.jsonld ${STORAGE}/real_time/${COMPANY_NAME}/${FILE_NAME}.jsonld