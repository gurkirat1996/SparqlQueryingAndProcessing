#include "../bpt.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
//#include <value_t>
#include <bits/stdc++.h>
using namespace std;
#define ll long long





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
///WARNING : INCREASE n-> char fwd[N][400000]; LOOK HERE
//fgets buffer size
// build fwd array

const int N= 300000;                // total databases (max)
bpt::bplus_tree *dbf[N],*dbr[N];	// arrays of forward and reverse database pointers
//char fwd[N][20000];              // each stores the starting string of each database
unsigned long long rev[N];         // each stores the starting line number of each reverse db
char argv1_copy[40],argv2_copy[40];  // argument to be passed while making fwd and rev db
ll linesDB[N],sizeDB[N],clashes[N];                         //total lines in each database && size of each database

const ll sz=2500000000;		//max size of each database
const int num_thr=2;
ll tot_sz;

void build(char *db,ll cur, char *s, FILE* f){
	cout<<"build\n";
	//char fileName[100],dbName[100];
	//strcpy(fileName,file);
	//strcpy(dbName,db);
	ll temp=0,cnt=0,ignore=0;
	//char s[50001];
	//int flen=strlen(dbName); //length of prefix
	//sprintf(buff,"%d",cur);
    //dbName[flen]='\0';
	//argv2_copy[rlen]='\0';
	//strcat(dbName,buff);
    
    //FILE *f=fopen(file,"r");
    //fseek ( f , seek , SEEK_SET );
    bpt::key_t start,start2;

    dbf[cur]=new bpt::bplus_tree(db, true);
    
    if(cur!=0){
    	fgets (s, 20001, f);
    	ignore = strlen(s);     //ignore is size of skipped line
    	temp+=strlen(s);
    }
    
    fgets (s, 20001, f);
    //strcpy(fwd[cur],s);
    start.k.X=hash1(s),start.k.Y=hash2(s);
	if(dbf[cur]->insert(start, cnt ))         //returns 1 key is already present
	{
		clashes[cur]++;
		printf("%lld\n%s\n%llu\n%llu\n\n",cnt,s,start.k.X,start.k.Y);
	}
	temp+=strlen(s);
	cnt++;

    while(fgets (s, 20001, f)!=NULL and temp<=sz){
    	(s[strlen(s)-1]=='\n');
  		//cout<<cnt<<endl;	
		temp+=strlen(s);
		cnt++;
		start.k.X=hash1(s),start.k.Y=hash2(s);
		/*if(dbf[cur]->insert(start, cnt ))         //returns 1 key is already present
		{
			clashes[cur]++;
			printf("%lld\n%s\n%llu\n%llu\n\n",cnt,s,start.k.X,start.k.Y);
		}*/
		dbf[cur]->insert(start, cnt );

		if(cnt%10000==0)
			cout<<cnt<<endl;
  	}
  	linesDB[cur] = cnt;
  	sizeDB[cur] = temp-ignore; 
  	dbf[cur]->close_file();
  	fclose(f);
  	//cout<<"build returning "<<' '<<temp<<' '<<cnt<<endl;
}

void func(ll seek,char *file,char *db,ll cur, ll total){
    
    //cout<<"in func "<<' '<<total<<' '<<seek<<cur<<' '<<endl;

    char dbName[100];
	strcpy(dbName,db);
	char s[50001];
	char buff[10];
	int flen=strlen(dbName); //length of prefix
	//argv2_copy[rlen]='\0';
	FILE *f=fopen(file,"r");
    for(ll i=0;i<total;i++){
    	dbName[flen]='\0';
    	sprintf(buff,"%d",cur+i);
    	strcat(dbName,buff);
    	fseek ( f , seek+i*sz , SEEK_SET );
    	cout<<"call build "<<i<<endl;
        build(dbName,cur+i,s,f);
        cout<<" func done for "<<i<<endl;
    }
    
    //cout<<"func returrning\n";
}



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
	FILE *pFile = fopen(argv[3],"r");
	if (pFile==NULL) perror ("Error opening file");
  	else
  	{
    	fseek (pFile, 0, SEEK_END);   // non-portable
    	tot_sz=ftell (pFile);

    	fclose (pFile);
  	}
	
	
	if(fork()==0){
	freopen(argv[3],"r",stdin);
	
	std::thread myThreads[10000];
	
	ll blocks=tot_sz/sz+(tot_sz%sz!=0);
	ll pt=blocks/num_thr;
	ll ex=blocks-pt*num_thr;
	ll tot_db=0; //no of db made
	cout<<blocks<<endl;
	for(ll i=num_thr-1;i>=0;i--){
	    
	    ll tm=pt; //no of blocks ith thread will make
	    if(ex>0){ex--;tm++;}
	    tot_db+=tm;
	    cerr<<i<<" "<<tot_db<<" calling func "<<sz*(blocks-tot_db)<<' '<<(blocks-tot_db)<<' '<<tm<<endl;
	    myThreads[i] = std::thread(func,sz*(blocks-tot_db), argv[3],argv1_copy,(blocks-tot_db),tm);
		// for(ll j=0;j<tm;j++){
		// 	cout<<"hello\n";
		// 	build(sz*(blocks-tot_db) + j*sz,argv[3],argv1_copy,blocks-tot_db + j);
		// }
	}
	for(ll i=0;i<num_thr;i++)
  		myThreads[i].join();
	/*
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
	*/

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

// 		start.k.X=hash1(s),start.k.Y=hash2(s);
// 		if(dbf[cur]->insert(start, cnt ))         //returns 1 key is already present
// 		{
// 			clashes++;
// 			printf("%d\n%s\n%llu\n%llu\n\n",cnt,s,start.k.X,start.k.Y);
// 		}
		
		start2.k=make_pair(cnt,cnt);
		dbr[cur]->insert(start2,sum);   //this inserts in reverse db
		sum+=strlen(s)+1;	//update byte number (+1 for newline)
		cnt++;
	}
	
	}
	*/

	FILE *file_fwd,*file_rev;
	file_fwd = fopen ("file_fwd","w");
	file_rev = fopen ("file_rev","w");
	cur++;
	fprintf (file_fwd, "%d\n",cur);
	fprintf (file_rev, "%d\n",cur);
	for(int i=0;i<cur;i++){
    fprintf (file_rev, "%llu\n",rev[i]);
    //fprintf (file_fwd, "%s\n",fwd[i]);
  }
	cout<<"clashes: "<<clashes<<endl;
	tm = clock()-tm ;
	cout <<"time: "<< (float)(tm)/CLOCKS_PER_SEC << "sec\n";

    return 0;
}

