

--添加列簇
disable 'cst_stream_hour_statistics'
alter 'cst_stream_hour_statistics',{NAME => 'mileage', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'PREFIX_TREE',  COMPRESSION => 'SNAPPY', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}
enable 'cst_stream_hour_statistics'



disable 'cst_stream_day_statistics'
alter 'cst_stream_day_statistics',{NAME => 'mileage', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'PREFIX_TREE',  COMPRESSION => 'SNAPPY', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}
enable 'cst_stream_day_statistics'

disable 'cst_stream_month_statistics'
alter 'cst_stream_month_statistics',{NAME => 'mileage', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'PREFIX_TREE',  COMPRESSION => 'SNAPPY', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}
enable 'cst_stream_month_statistics'

disable 'cst_stream_year_statistics'
alter 'cst_stream_year_statistics',{NAME => 'mileage', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'PREFIX_TREE',  COMPRESSION => 'SNAPPY', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}
enable 'cst_stream_year_statistics'
