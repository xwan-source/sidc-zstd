noinst_LIBRARIES=libindex.a
libindex_a_SOURCES=index.c fingerprint_cache.c kvstore.c kvstore_htable.c sampling_method.c segmenting_method.c similarity_detection.c base_container_cache.c db_mysql.c
libindex_a_LDFLAGS=-lmysqlclient
INCLUDE=-I./  -I/usr/local/mysql/include/
LIBS=-lglib -L/usr/lib/mysql -lmysqlclient
