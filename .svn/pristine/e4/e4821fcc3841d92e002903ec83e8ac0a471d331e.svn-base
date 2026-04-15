/*
 * db_mysql.h
 *
 *  Created on: Aug 25, 2013
 *      Author: fumin
 */

#ifndef DB_MYSQL_H_
#define DB_MYSQL_H_

#include "../destor.h"

typedef int BOOL;

BOOL db_init();
int64_t db_close();
containerid db_lookup_fingerprint(fingerprint *finger);
void db_insert_fingerprint(fingerprint* finger, containerid id);
int64_t db_lookup_fingerprint_for_hint(fingerprint *finger);
void db_insert_fingerprint_with_hint(fingerprint* finger, containerid id,
		int64_t block_hint);

#endif /* DB_MYSQL_H_ */
