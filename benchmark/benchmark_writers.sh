#!/usr/bin/bash

# This file is open source software, licensed to you under the terms
# of the Apache License, Version 2.0 (the "License").  See the NOTICE file
# distributed with this work for additional information regarding copyright
# ownership.  You may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#
# Copyright (C) 2020 ScyllaDB
#

set -e

SEASTAR_WRITER=seastar-parquet-tools/build/parquet-writer
ARROW_WRITER=arrow-parquet-tools/build/parquet-writer

TIMEFORMAT=%R

declare -A CASES

CASES[int32_plain_uncompressed]="
	--filetype int32 \
	--compression uncompressed \
	--plain true \
	--rows 4000000 \
	--rowgroups 20 \
	--page 65536"

CASES[int32_plain_snappy]="
	--filetype int32 \
	--compression snappy \
	--plain true \
	--rows 4000000 \
	--rowgroups 120 \
	--page 65536"

CASES[int32_dict_uncompressed]="
	--filetype int32 \
	--compression uncompressed \
	--plain false \
	--rows 4000000 \
	--rowgroups 60 \
	--page 65536"

CASES[int32_dict_snappy]="
	--filetype int32 \
	--compression snappy \
	--plain false \
	--rows 4000000 \
	--rowgroups 120 \
	--page 65536"

CASES[int64_plain_uncompressed]="
	--filetype int64 \
	--compression uncompressed \
	--plain true \
	--rows 2000000 \
	--rowgroups 20 \
	--page 65536"

CASES[int64_plain_snappy]="
	--filetype int64 \
	--compression snappy \
	--plain true \
	--rows 2000000 \
	--rowgroups 120 \
	--page 65536"

CASES[int64_dict_uncompressed]="
	--filetype int64 \
	--compression uncompressed \
	--plain false \
	--rows 2000000 \
	--rowgroups 120 \
	--page 65536"

CASES[int64_dict_snappy]="
	--filetype int64 \
	--compression snappy \
	--plain false \
	--rows 2000000 \
	--rowgroups 240 \
	--page 65536"

CASES[string8_plain_uncompressed]="
	--filetype string \
	--compression uncompressed \
	--plain true \
	--rows 10000000 \
	--rowgroups 2 \
	--page 65536 \
	--string 8"

CASES[string8_plain_snappy]="
	--filetype string \
	--compression snappy \
	--plain true \
	--rows 10000000 \
	--rowgroups 20 \
	--page 65536 \
	--string 8"

CASES[string8_dict_uncompressed]="
	--filetype string \
	--compression uncompressed \
	--plain false \
	--rows 1000000 \
	--rowgroups 80 \
	--page 65536 \
	--string 8"

CASES[string8_dict_snappy]="
	--filetype string \
	--compression snappy \
	--plain false \
	--rows 1000000 \
	--rowgroups 300 \
	--page 65536 \
	--string 8"

CASES[string80_plain_uncompressed]="
	--filetype string \
	--compression uncompressed \
	--plain true \
	--rows 1000000 \
	--rowgroups 4 \
	--page 65536 \
	--string 80"

CASES[string80_plain_snappy]="
	--filetype string \
	--compression snappy \
	--plain true \
	--rows 1000000 \
	--rowgroups 20 \
	--page 65536 \
	--string 80"

CASES[string80_dict_uncompressed]="
	--filetype string \
	--compression uncompressed \
	--plain false \
	--rows 1000000 \
	--rowgroups 4 \
	--page 65536 \
	--string 80"

CASES[string80_dict_snappy]="
	--filetype string \
	--compression snappy \
	--plain false \
	--rows 1000000 \
	--rowgroups 30 \
	--page 65536 \
	--string 80"

mkdir -p writer_tmp
for CASE in ${!CASES[@]}; do
	echo $CASE
	FILENAME=writer_tmp/$CASE.parquet
	OPTIONS="${CASES[$CASE]}"
	for WRITER in $ARROW_WRITER $SEASTAR_WRITER; do
		time (
			${WRITER} $OPTIONS --filename $FILENAME

			sync $FILENAME
		)
		rm $FILENAME
	done
done
