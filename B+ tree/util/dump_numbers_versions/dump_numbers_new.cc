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

const int N= 30000;                // total databases (max)
bpt::bplus_tree *dbf[N],*dbr[N];	// arrays of forward and reverse database pointers
string fwd[N];              // each stores the starting string of each database
unsigned long long rev[N];         // each stores the starting line number of each reverse db
char argv1_copy[40],argv2_copy[40];  // argument to be passed while making fwd and rev db
ll linesDB[N],sizeDB[N],clashes[N];                         //total lines in each database && size of each database

const ll sz=500000;		//max size of each database
const int num_thr=1;
ll tot_sz;

void build(ll seek,char *file,char *db,ll cur){
	
	char fileName[100],dbName[100];
	strcpy(fileName,file);
	strcpy(dbName,db);
	ll temp=0,cnt=1,ignore=0;
	char s[50001];
	char buff[10];
	int flen=strlen(dbName); //length of prefix
	sprintf(buff,"%d",cur);
    dbName[flen]='\0';
	//argv2_copy[rlen]='\0';
	strcat(dbName,buff);
    
    FILE *f=fopen(fileName,"r");
    fseek ( f , seek , SEEK_SET );
    bpt::key_t start,start2;

    dbf[cur]=new bpt::bplus_tree(dbName, true);
    
    if(cur!=0){
    	fgets (s, 50001, f);
    	ignore = strlen(s)+1;     //ignore is size of skipped line
    	temp+=strlen(s)+1;
    }
    
    fgets (s, 50001, f);
    string str(s);
    fwd[cur]=str;
    
    start.k.X=hash1(s),start.k.Y=hash2(s);
	if(dbf[cur]->insert(start, cnt ))         //returns 1 key is already present
	{
		clashes[cur]++;
		printf("%lld\n%s\n%llu\n%llu\n\n",cnt,s,start.k.X,start.k.Y);
	}
	temp+=strlen(s)+1;
	

    while(fgets (s, 50001, f)!=NULL && temp<=sz){
  			
		temp+=strlen(s)+1;
		cnt++;
		start.k.X=hash1(s),start.k.Y=hash2(s);
		if(dbf[cur]->insert(start, cnt ))         //returns 1 key is already present
		{
			clashes[cur]++;
			printf("%lld\n%s\n%llu\n%llu\n\n",cnt,s,start.k.X,start.k.Y);
		}

		if(cnt%10000==0)
			cout<<cnt<<endl;
  	}
  	linesDB[cur] = cnt;
  	sizeDB[cur] = temp-ignore; 
  	fclose(f);
  	dbf[cur]->close_file();
  	//cout<<"build returning "<<' '<<temp<<' '<<cnt<<endl;
}

void func(ll seek,char *fileName,char *dbName,ll cur, ll total){
    //cout<<"in func "<<' '<<total<<' '<<seek<<cur<<' '<<endl;
    for(ll i=0;i<total;i++){

        build(seek + i*sz,fileName,dbName,cur+i);
        //cout<<" func done for "<<i<<endl;
    }
    //cout<<"func returrning\n";
}

void build2(ll seek,char *file,char *db,ll cur){
	
	char fileName[100],dbName[100];
	strcpy(fileName,file);
	strcpy(dbName,db);
	ll temp=0,cnt=1,ignore=0;
	char s[50001];
	char buff[10];
	int rlen=strlen(dbName); //length of prefix
	sprintf(buff,"%d",cur);
    dbName[rlen]='\0';
	//argv2_copy[rlen]='\0';
	strcat(dbName,buff);
    
    FILE *f=fopen(fileName,"r");
    fseek ( f , seek , SEEK_SET );
    bpt::key_t start,start2;

    dbf[cur]=new bpt::bplus_tree(dbName, true);
    
    if(cur!=0){
    	fgets (s, 50001, f);
    	ignore = strlen(s)+1;     //ignore is size of skipped line
    	temp+=strlen(s)+1;
    }
    
    fgets (s, 50001, f);
    rev[cur]=1;
    
    start2.k=make_pair(cnt,cnt);
	dbr[cur]->insert(start2,temp+seek);   //this inserts in reverse db, byte offset= seek+temp
	temp+=strlen(s)+1;

    while(fgets (s, 50001, f)!=NULL && temp<=sz){
  		start2.k=make_pair(cnt,cnt);
		dbr[cur]->insert(temp+seek,sum);   //this inserts in reverse db
		temp+=strlen(s)+1;
		cnt++;
		if(cnt%10000==0)
			cout<<cnt<<endl;
  	}
//   	linesDB[cur] = cnt;
//   	sizeDB[cur] = temp-ignore; 
  	fclose(f);
  	dbr[cur]->close_file();
  	//cout<<"build returning "<<' '<<temp<<' '<<cnt<<endl;
}

void func2(ll seek,char *fileName,char *dbName,ll cur, ll total){
    //cout<<"in func "<<' '<<total<<' '<<seek<<cur<<' '<<endl;
    for(ll i=0;i<total;i++){

        build2(seek + i*sz,fileName,dbName,cur+i);
        //cout<<" func done for "<<i<<endl;
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
	
	for(ll i=num_thr-1;i>=0;i--){
	    
	    ll tm=pt; //no of blocks ith thread will make
	    if(ex>0){ex--;tm++;}
	    tot_db+=tm;
	    //cerr<<i<<" "<<tot_db<<"calling func "<<sz*(blocks-tot_db)<<' '<<(blocks-tot_db)<<' '<<tm<<endl;
	    myThreads[i] = std::thread(func,sz*(blocks-tot_db), argv[3],argv1_copy,(blocks-tot_db),tm);
		
	}
	for(ll i=0;i<num_thr;i++)
  		myThreads[i].join();
	

	}
	else {
	freopen(argv[3],"r",stdin);
	
	std::thread myThreads[10000];
	
	ll blocks=tot_sz/sz+(tot_sz%sz!=0);
	ll pt=blocks/num_thr;
	ll ex=blocks-pt*num_thr;
	ll tot_db=0; //no of db made
	
	for(ll i=num_thr-1;i>=0;i--){
	    
	    ll tm=pt; //no of blocks ith thread will make
	    if(ex>0){ex--;tm++;}
	    tot_db+=tm;
	    cerr<<i<<" "<<tot_db<<"calling func "<<sz*(blocks-tot_db)<<' '<<(blocks-tot_db)<<' '<<tm<<endl;
	    myThreads[i] = std::thread(func2,sz*(blocks-tot_db), argv[3],argv2_copy,(blocks-tot_db),tm);
		
	}
	for(ll i=0;i<num_thr;i++)
  		myThreads[i].join();
	/*while (gets(s) != NULL)
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
	}*/
	
	}
	

	FILE *file_fwd,*file_rev;
	file_fwd = fopen ("file_fwd","w");
	file_rev = fopen ("file_rev","w");
	cur++;
	fprintf (file_fwd, "%d\n",cur);
	fprintf (file_rev, "%d\n",cur);
	for(int i=0;i<cur;i++){
        fprintf (file_rev, "%llu\n",rev[i]);
        fprintf (file_fwd, "%s\n",fwd[i].cstr());
  }
	cout<<"clashes: "<<clashes<<endl;
	tm = clock()-tm ;
	cout <<"time: "<< (float)(tm)/CLOCKS_PER_SEC << "sec\n";

    return 0;
}

