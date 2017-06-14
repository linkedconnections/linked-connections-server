COMPANY_NAME="$1"
LAST_MODIFIED="$2"

gtfs2lc-sort ../../datasets/${COMPANY_NAME}/${LAST_MODIFIED}_tmp
gtfs2lc ../../datasets/${COMPANY_NAME}/${LAST_MODIFIED}_tmp -f jsonld > ../../linked_connections/${COMPANY_NAME}/${LAST_MODIFIED}_tmp.jsonld
rm -r ../../datasets/${COMPANY_NAME}/${LAST_MODIFIED}_tmp
sort -t \" -k 17 ../../linked_connections/${COMPANY_NAME}/${LAST_MODIFIED}_tmp.jsonld > ../../linked_connections/${COMPANY_NAME}/${LAST_MODIFIED}.jsonld
rm ../../linked_connections/${COMPANY_NAME}/${LAST_MODIFIED}_tmp.jsonld
gzip ../../linked_connections/${COMPANY_NAME}/${LAST_MODIFIED}.jsonld
mkdir ../../linked_pages/${COMPANY_NAME}/${LAST_MODIFIED}