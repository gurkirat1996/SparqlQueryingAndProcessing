#include "../bpt.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
//#include <value_t>
#include <bits/stdc++.h>
using namespace std;

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

//----------------I/O INFORMATION-----------------------//
//command : ./bpt_dump_numbers forward_db_prefix reverse_db_prefix data_file
//Note each db will be of type prefixi , where i is varies from 0...N
//-----------------------------------------------------//


const int N= 5005;                // total databases (max)
bpt::bplus_tree *dbf[N],*dbr[N];	// arrays of forward and reverse database pointers
char fwd[N][400000];              // each stores the starting string of each database
unsigned long long rev[N];         // each stores the starting line number of each reverse db
char argv1_copy[40],argv2_copy[40];  // argument to be passed while making fwd and rev db


const int sz=50000;		//max size of each database

int main(int argc, char *argv[])
{
	clock_t tm = clock();
	if(argc!=4){
		cout<<"Invalid Argument"<<endl;
		return 0;
	}
	strcpy(argv1_copy,argv[1]);	//copy prefix
	strcpy(argv2_copy,argv[2]);
	char s[400000];
	ull cnt=1;			// line number
	int clashes=0;
	int cur=-1;					// database number
	bpt::key_t start,start2;   	// fwd and rev key respectively
	ull sum=0;					// number of bytes

	int flen=strlen(argv[1]);
	int rlen=strlen(argv[2]);
	char buf[10];	// stores the current database number as string to be appended to prefix

		// data file
	clock_t x = clock();
	FILE *tim= fopen("times.txt","w");	// to print times
	
	
	if(fork()==0){
	freopen(argv[3],"r",stdin);
	
	while (gets(s) != NULL)
	{
		if(cnt%10000==0){
			cout<<"fwd "<<cnt<<endl;
		}
		if(cnt%sz==1){
			//fprintf(tim, "%d %f\n",cur+1,(float)(clock() - x)/CLOCKS_PER_SEC );
			x = clock() ;
			cur++;
			sprintf(buf,"%d",cur);
			argv1_copy[flen]='\0';
			//argv2_copy[rlen]='\0';
			strcat(argv1_copy,buf);
			//strcat(argv2_copy,buf);
			dbf[cur]=new bpt::bplus_tree(argv1_copy, true);	//make new db
			//dbr[cur]=new bpt::bplus_tree(argv2_copy, true);
			strcpy(fwd[cur],s);
			//rev[cur]=cnt;
		}

		start.k.X=hash1(s),start.k.Y=hash2(s);
		if(dbf[cur]->insert(start, cnt ))         //returns 1 key is already present
		{
			clashes++;
			printf("%d\n%s\n%llu\n%llu\n\n",cnt,s,start.k.X,start.k.Y);
		}
		//start2.k=make_pair(cnt,cnt);
		//dbr[cur]->insert(start2,sum);   //this inserts in reverse db
		//sum+=strlen(s)+1;	//update byte number (+1 for newline)
		cnt++;
	}
	}

	/*else {
	freopen(argv[3],"r",stdin);
	while (gets(s) != NULL)
	{
		if(cnt%10000==0){
			cout<<"rev "<<cnt<<endl;
		}
		if(cnt%sz==1){
			//fprintf(tim, "%d %f\n",cur+1,(float)(clock() - x)/CLOCKS_PER_SEC );
			x = clock() ;
			cur++;
			sprintf(buf,"%d",cur);
			//argv1_copy[flen]='\0';
			argv2_copy[rlen]='\0';
			//strcat(argv1_copy,buf);
			strcat(argv2_copy,buf);
			//dbf[cur]=new bpt::bplus_tree(argv1_copy, true);	//make new db
			dbr[cur]=new bpt::bplus_tree(argv2_copy, true);
			//strcpy(fwd[cur],s);
			rev[cur]=cnt;
		}

		// start.k.X=hash1(s),start.k.Y=hash2(s);
		// if(dbf[cur]->insert(start, cnt ))         //returns 1 key is already present
		// {
		// 	clashes++;
		// 	printf("%d\n%s\n%llu\n%llu\n\n",cnt,s,start.k.X,start.k.Y);
		// }
		
		start2.k=make_pair(cnt,cnt);
		dbr[cur]->insert(start2,sum);   //this inserts in reverse db
		sum+=strlen(s)+1;	//update byte number (+1 for newline)
		cnt++;
	}
	
	}*/

	FILE *file_fwd,*file_rev;
	file_fwd = fopen ("file_fwd","w");
	file_rev = fopen ("file_rev","w");
	cur++;
	fprintf (file_fwd, "%d\n",cur);
	fprintf (file_rev, "%d\n",cur);
	for(int i=0;i<cur;i++){
    fprintf (file_rev, "%llu\n",rev[i]);
    fprintf (file_fwd, "%s\n",fwd[i]);
  }
	cout<<"clashes: "<<clashes<<endl;
	tm = clock()-tm ;
	cout <<"time: "<< (float)(tm)/CLOCKS_PER_SEC << "sec\n";

    return 0;
}

