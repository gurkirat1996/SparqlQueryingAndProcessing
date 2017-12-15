/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 1997, 2016 Oracle and/or its affiliates.  All rights reserved.
 *
 * ex_btrec-- A basic example of using record numbers in a btree.
 *  
 *   This example program shows how to store automatically numbered records in a
 *   btree database, one of the kinds of access methods provided by Berkeley DB.
 *   Access methods determine how key-value pairs are stored in the file.
 *   B-tree is one of the most commonly used types because it supports sorted
 *   access to variable length records.
 *   
 *   The program first reads 1000 records from file "wordlist" and then stores
 *   the data in the "access.db" database. The key of each record is the record
 *   number concatenated with a word from the word list; the data is the same,
 *   but in reverse order. Then it opens a cursor to fetch key/data pairs.
 *   The user selects a record by entering its record number. Both the specified
 *   record and the one following it will be displayed.
 *   
 * Database: access.db 
 * Wordlist directory: ../test/tcl/wordlist
 *
 * $Id: ex_btrec.c,v 0f73af5ae3da 2010/05/10 05:38:40 alexander $
 */

#include <sys/types.h>

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <db.h>
#define ull unsigned long long

#define	DATABASE	"forward_tweak.db"
#define	WORDLIST	"../../../B+ tree/data.txt"
#define QUERY		"../../../forward_queries"

int	main __P((void));

int	ex_btrec __P((void));
void	show __P((const char *, DBT *, DBT *));

ull hash1(char* str)
{
    ull hash = 5381;
    int c;
    while (c = *str++){
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    }
    return hash;
}

ull hash2(char* str)
{
    ull hash = 5381;
    int c;
    while (c = *str++){
        hash = c + (hash << 6) + (hash << 16) - hash;
    }
    return hash;
}

FILE *result;

int
main()
{
	result = fopen("result.txt","w");
	return (ex_btrec() == 1 ? EXIT_FAILURE : EXIT_SUCCESS);
}

int
ex_btrec()
{
	DB *dbp;		/* Handle of the main database to store the content of wordlist. */
	DBC *dbcp;		/* Handle of database cursor used for putting or getting the word data. */
	DBT key;		/* The key to dbcp->put()/from dbcp->get(). */
	DBT data;		/* The data to dbcp->put()/from dbcp->get(). */
	DB_BTREE_STAT *statp;	/* The statistic pointer to record the total amount of record number. */
	FILE *fp;		/* File pointer that points to the wordlist. */
	db_recno_t recno;	/* Record number to retrieve a record in access.db database. */
	size_t len;		/* The size of buffer. */
	long long int cnt;		/* The count variable to read records from wordlist. */
	int ret;		/* Return code from call into Berkeley DB. */
	char *p;		/* Pointer to store buffer. */
	char *t;		/* Pointer to store reverse buffer. */
	char buf[377405];		/* Buffer to store key value. */
	char rbuf[1024];	/* Reverse buffer to store data value. */
	const char *progname = "ex_btrec";		/* Program name. */

	/* Open the text file containing the words to be inserted. */
	if ((fp = fopen(WORDLIST, "r")) == NULL) {
		fprintf(stderr, "%s: open %s: %s\n",
		    progname, WORDLIST, db_strerror(errno));
		return (1);
	}

	/* Remove the previous database. */
	//(void)remove(DATABASE);

	/* Create the database handle. */
	if ((ret = db_create(&dbp, NULL, 0)) != 0) {
		fprintf(stderr,
		    "%s: db_create: %s\n", progname, db_strerror(ret));
		return (1);
	}

	/*
	 * Prefix any error messages with the name of this program and a ':'.
	 * Setting the errfile to stderr is not necessary, since that is the
	 * default; it is provided here as a placeholder showing where one
	 * could direct error messages to an application-specific log file.
	 */
	dbp->set_errfile(dbp, stderr);
	dbp->set_errpfx(dbp, progname);

	/* Configure the database to use 1KB page sizes and record numbering. */
	if ((ret = dbp->set_pagesize(dbp, 1024*64)) != 0) {
		dbp->err(dbp, ret, "set_pagesize");
		return (1);
	}
	if ((ret = dbp->set_flags(dbp, DB_RECNUM)) != 0) {
		dbp->err(dbp, ret, "set_flags: DB_RECNUM");
		return (1);
	}
	if ((ret = dbp->set_cachesize(dbp,8,1024*1024*1024*4,1)) != 0) {
                dbp->err(dbp, ret, "open: %s", DATABASE);
                return (1);
        }
	/* Open it with DB_CREATE, making it a DB_BTREE. */
	if ((ret = dbp->open(dbp,
	    NULL, DATABASE, NULL, DB_BTREE, DB_CREATE, 0664)) != 0) {
		dbp->err(dbp, ret, "open: %s", DATABASE);
		return (1);
	}

	/*
	 * Insert records in the wordlist into the database. The key is the
	 * word preceded by its record number, and the data contains the same
	 * characters in the key, but in reverse order.
	 */
	memset(&key, 0, sizeof(DBT));
	memset(&data, 0, sizeof(DBT));
	// for (cnt = 1; cnt <= 300000000; ++cnt) {
	// 	if(cnt%100000==0)
	// 		printf("%lld\n",cnt);
	// 	//strcpy(buf,"");
	// 	//(void)sprintf(buf, "%04d_", cnt);
	// 	if (fgets(buf, sizeof(buf), fp) == NULL)
	// 		break;
	// 	/* The values exclude the trailing newline, hence the -1. */
		
	// 	len = strlen(buf) - 1;
	// 	buf[len] = '\0';
	// 	/*
	// 	 * Fill the reverse buffer 'rbuf' with the characters from
	// 	 * 'buf', but in reverse order.
	// 	 */
	// 	ull x1=hash1(buf);
	// 	ull x2=hash2(buf);
	// 	// sprintf(buf, "%20llu", x1);
	// 	// sprintf(buf+20, "%20llu", x2);
	// 	// buf[20+20]='\0';
	// 	memcpy(buf,&x1,sizeof(unsigned long long));
	// 	memcpy(buf + sizeof(unsigned long long), &x2,sizeof(unsigned long long));
	// 	buf[2*sizeof(unsigned long long)]='\0';
	// 	len = strlen(buf) ;
		
	// 	// for (t = rbuf, p = buf + len; p > buf;){
	// 	// 	*t++ = *--p;
			
	// 	// }
	// 	// *t++ = '\0';

	// 	int count = cnt;
	// 	long long temp,i;
	// 	sprintf(rbuf,"%lld",cnt);
	// 	rbuf[strlen(rbuf)] = '\0';
	// 	// memcpy(rbuf,&cnt,sizeof(int));

	// 	//printf("len buf: %d rbuf: %d\n",strlen(buf),strlen(rbuf));
	// 	// while(count>0){
	// 	// 	*t++ = '0' + count%10;
	// 	// 	count = count/10;
	// 	// }
	// 	// *t++ = '\0';
	// 	// for(i=0;i<strlen(arr);i++){
	// 	// 	rbuf[i]=arr[strlen(arr)-1-i];
	// 	// }

	// 	temp = strlen(rbuf);
	// 	// if(cnt%1000==1){
	// 	// 	int i;
	// 	// 	for(i=0;i<len+1;i++)
	// 	// 		printf("%c",buf[i]);
	// 	// 	printf("  %d\n",cnt);
	// 	// }

	// 	// if(cnt%1000==1)
	// 	// {
	// 	// 	for(i=0;i<temp;i++){
	// 	// 			printf("%c",rbuf[i]);
	// 	// 	}
	// 	// 	printf("\n");
	// 	// }

	// 	/*
	// 	 * Now that we have generated the values for the key and data
	// 	 * items, set the DBTs to point to them.
	// 	 */

		

	// 	key.data = buf;
	// 	data.data = rbuf;
	// 	data.size = (u_int32_t)temp; 
	// 	key.size = 2*sizeof(unsigned long long) ;

	// 	if ((ret =
	// 	    dbp->put(dbp, NULL, &key, &data, DB_NOOVERWRITE)) != 0) {
	// 		dbp->err(dbp, ret, "DB->put");
	// 		if (ret != DB_KEYEXIST)
	// 			goto err1;
	// 	}
	// }

	/* We are done with the file of words. */
	(void)fclose(fp);

	/* Get the database statistics and print the total number of records. */
	if ((ret = dbp->stat(dbp, NULL, &statp, 0)) != 0) {
		dbp->err(dbp, ret, "DB->stat");
		goto err1;
	}
	printf("%s: database contains %lu records\n",
	    progname, (u_long)statp->bt_ndata);
	free(statp);

	/* Acquire a cursor for sequential access to the database. */
	if ((ret = dbp->cursor(dbp, NULL, &dbcp, 0)) != 0) {
		dbp->err(dbp, ret, "DB->cursor");
		goto err1;
	}

	/*
	 * Repeatly prompt the user for a record number, then retrieve and
	 * display that record as well as the one after it. Quit on EOF or
	 * when a zero record number is entered.
	 */
	//return 0;
	FILE *fp2 = fopen(QUERY,"r");
	struct timeval begin, end;
	gettimeofday(&begin, NULL);
	for (;;) {
		//dbp->cursor(dbp, NULL, &dbcp, 0);
		/* Get a record number. */
		fflush(stdout);
		if (fgets(buf, sizeof(buf), fp2) == NULL)
			break;
		buf[strlen(buf)-1] = '\0';
		recno = buf;
		/*
		 * Zero is an invalid record number: exit when that (or a
		 * non-numeric string) is entered.
		 */
		if (recno == 0)
			break;

		/*
		 * Reset the key each time, the dbp->get() routine returns
		 * the key and data pair, not just the key!
		 */
		ull x1=hash1(buf);
		ull x2=hash2(buf);
		// sprintf(buf, "%20llu", x1);
		// sprintf(buf+20, "%20llu", x2);
		// buf[20+20]='\0';
		memcpy(buf,&x1,sizeof(unsigned long long));
		memcpy(buf + sizeof(unsigned long long), &x2,sizeof(unsigned long long));
		buf[2*sizeof(unsigned long long)]='\0';

		int i;
		len = sizeof(unsigned long long);

		// for(i=0;i<len;i++)
		// 	printf("%c",buf[i]);
		// printf("\n");

		key.data = buf;
		key.size = 2*sizeof(unsigned long long);
		//show("k/d\t", &key, &key);
		if ((ret = dbcp->get(dbcp, &key, &data, DB_SET)) != 0){
			printf("inside\n");
			goto get_err;
		}

		/* Display the key and data. */
		show("record no.\t", &key, &data);
		
		/* DB_NEXT moves the cursor to the next record. */
		 if ((ret = dbcp->get(dbcp, &key, &data, DB_NEXT)) != 0)
		 	goto get_err;
		//show("record no.\t", &key, &data);
		/* Display the successor. */
		//show("next\t", &key, &data);

		/*
		 * Retrieve the record number for the successor into "recno"
		 * and print it. Set data flags to DB_DBT_USERMEM so that the
		 * record number will be retrieved into a proper, aligned
		 * integer, which is needed by some hardware platforms. It
		 * also makes it easier to print the value: no u_int32_t
		 * casting is needed.
		 */
		data.data = &buf;
		data.size = sizeof(buf);
		data.ulen = sizeof(buf);
		data.flags |= DB_DBT_USERMEM;
		if ((ret = dbcp->get(dbcp, &key, &data, DB_GET_RECNO)) != 0) {
get_err:		dbp->err(dbp, ret, "DBcursor->get");
			if (ret != DB_NOTFOUND && ret != DB_KEYEMPTY)
				goto err2;
		} else
			;//printf("retrieved recno: %lu\n", (u_long)buf);

		/* Reset the data DBT. */
		memset(&data, 0, sizeof(data));
	}

	gettimeofday(&end, NULL);
	printf("time taken: %ld\n", ((end.tv_sec * 1000000 + end.tv_usec)
		  - (begin.tv_sec * 1000000 + begin.tv_usec)));
	

	/* Close the cursor, then its database. */
	if ((ret = dbcp->close(dbcp)) != 0) {
		dbp->err(dbp, ret, "DBcursor->close");
		goto err1;
	}
	if ((ret = dbp->close(dbp, 0)) != 0) {
		/*
		 * This uses fprintf rather than dbp->err because the dbp has
		 * been deallocated by dbp->close() and may no longer be used.
		 */
		fprintf(stderr,
		    "%s: DB->close: %s\n", progname, db_strerror(ret));
		return (1);
	}

	return (0);

err2:	(void)dbcp->close(dbcp);
err1:	(void)dbp->close(dbp, 0);
	return (ret);

}

/*
 * show --
 *	Display a key/data pair.
 *
 * Parameters:
 *  msg		print message
 *  key		the target key to print
 *  data	the target data to print
 */
void
show(msg, key, data)
	const char *msg;
	DBT *key, *data;
{
	fprintf(result,"%.*s\n",
	    (int)data->size, (char *)data->data);
}


