#/bin/bash

base_url=$1

wget -r -nd --delete-after $base_url/load_fake_documents && \
wget -r -nd --delete-after -l inf $base_url/construct_document_index?task=map_master && \
wget -r -nd --delete-after -l inf $base_url/construct_document_index?task=reduce_master