#include "../bpt.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <bits/stdc++.h>
#include <sys/time.h>
using namespace std;
#define ll long long
#define ull unsigned long long int

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

const ll N= 300000;                // total databases (max)
bpt::bplus_tree *dbf[N];	// arrays of forward and reverse database pointers
bpt_rev::bplus_tree *dbr[N];
string fwd[N];              // each stores the starting string of each database
unsigned long long rev[N];         // each stores the starting line number of each reverse db
char argv1_copy[40],argv2_copy[40];  // argument to be passed while making fwd and rev db
ll linesDB[N],sizeDB[N],clashes_fwd[N],clashes_rev[N];                         //total lines in each database && size of each database

const ull sz=10000000;		//max size of each database
const int num_thr=4;
ull tot_sz;


void build(char *db_forward,char *db_reverse,ll cur, char *s, FILE* f,ull seek, ofstream &outfile){
	ull temp=0,cnt=1,ignore=0,length=0;
	
    bpt::key_t start;
    bpt_rev::key_t start2;

    dbf[cur]=new bpt::bplus_tree(db_forward, true);
    dbr[cur]=new bpt_rev::bplus_tree(db_reverse, true);
    if(cur!=0){
    	fgets (s, 400001, f);
    	ignore = strlen(s);
    	temp = strlen(s);
    }
    
    fgets (s, 400001, f);
    length = strlen(s);
    
    if(s[length-1]=='\n'){
        s[length-1]='\0';
    }
    string s_temp(s);
    fwd[cur] = s_temp;
    rev[cur] = 1;

    start.k.X=hash1(s),start.k.Y=hash2(s);
    start2.k=cnt;

	if(dbf[cur]->insert(start, cnt ))         //returns 1 key is already present
	{
		clashes_fwd[cur]++;
		printf("%lld\n%s\n%llu\n%llu\n\n",cnt,s,start.k.X,start.k.Y);
	}

	if(dbr[cur]->insert(start2, temp + seek ))         //returns 1 key is already present
	{
		clashes_rev[cur]++;
		printf("%lld\n%s\n%llu\n%llu\n\n",cnt,s,start.k.X,start.k.Y);
	}

	temp+=length;
	//fprintf(fp,"%lld\n",seek+ignore);
	ull x = seek+ignore;
	int xx =4;
	outfile.write((char *)&x, sizeof(ull));

    while(fgets (s, 4000001, f)!=NULL and temp<=sz){
    	//fprintf(fp,"%lld\n",temp);
        outfile.write((char *)&temp, sizeof(ull));
        length = strlen(s);
    	if(s[length-1]=='\n'){
            s[length-1]='\0';
        }
		cnt++;
		start.k.X=hash1(s),start.k.Y=hash2(s);
		start2.k=cnt;

		if(dbf[cur]->insert(start, cnt ))         //returns 1 key is already present
		{
			clashes_fwd[cur]++;
			printf("%lld\n%s\n%llu\n%llu\n\n",cnt,s,start.k.X,start.k.Y);
		}

		if(dbr[cur]->insert(start2, temp + seek))         //returns 1 key is already present
		{
			clashes_rev[cur]++;
			printf("%lld\n%s\n%llu\n%llu\n\n",cnt,s,start.k.X,start.k.Y);
		}
		temp+=length;

  	}
  	linesDB[cur] = cnt;
  	sizeDB[cur] = temp-ignore; 
  	dbf[cur]->close_file();
  	dbr[cur]->close_file();
  	
}

void func(ull seek,char *file,char *db_forward,char *db_reverse,ll cur, ll total, ll num){
    string temp = "temp/temp";
    string result = temp + to_string(num);
    const char *cstr = result.c_str();
    //FILE *fp = fopen(cstr,"w");
	
    ofstream outfile;
    outfile.open(cstr, ios::binary | ios::out);

	cout<<"Thread "<<num<<" will build "<<total<<" db\n";
    char dbNameForward[100],dbNameReverse[100];
	strcpy(dbNameForward,db_forward);
	strcpy(dbNameReverse,db_reverse);
	char s[400001];
	char buff[10];
	int flen=strlen(dbNameForward); //length of prefix
	int rlen=strlen(dbNameReverse);
	//argv2_copy[rlen]='\0';
	FILE *f=fopen(file,"r");
    for(ll i=0;i<total;i++){
    	dbNameForward[flen]='\0';
    	dbNameReverse[rlen]='\0';
    	sprintf(buff,"%lld",cur+i);
    	strcat(dbNameForward,buff);
    	strcat(dbNameReverse,buff);
    	fseek ( f , seek+i*sz , SEEK_SET );
		build(dbNameForward,dbNameReverse,cur+i,s,f,seek+i*sz,outfile);
		if(i%100==0)cout<<"Thread "<<num<<' '<<i<<endl;
    }
    fclose(f);
}




int main(int argc, char *argv[])
{
    struct timeval begin, end;
    gettimeofday(&begin, NULL);
    
	if(argc!=4){
		cout<<"Invalid Argument"<<endl;
		return 0;
	}
	strcpy(argv1_copy,argv[1]);	//copy prefix
	strcpy(argv2_copy,argv[2]);
	char s[400001];
	ull cnt=1;			// line number
	int clashes=0;
	int cur=-1;					// database number
	bpt::key_t start;   	// fwd and rev key respectively
	bpt_rev::key_t start2;
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

	//tot_sz=atoll(argv[4]);
	cerr<<"total size "<<tot_sz<<"\n";

	//main code
	freopen(argv[3],"r",stdin);
	
	std::thread myThreads[10000];
	
	ll blocks=tot_sz/sz+(tot_sz%sz!=0);
	cout<<"total databases to be made "<<blocks<<endl;
	ll pt=blocks/num_thr;
	ll ex=blocks-pt*num_thr;
	ll tot_db=0; //no of db made
	cout<<blocks<<endl;
	
	for(ll i=num_thr-1;i>=0;i--){
	    
	    ll tm=pt; //no of blocks ith thread will make
	    if(ex>0){ex--;tm++;}
	    tot_db+=tm;
	    myThreads[i] = std::thread(func,sz*(blocks-tot_db), argv[3],argv1_copy,argv2_copy,(blocks-tot_db),tm,i);
	
	}
	for(ll i=0;i<num_thr;i++)
  		myThreads[i].join();
  	////
	

// REVERSE 

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

	FILE *file_fwd,*file_rev, *lines;
	file_fwd = fopen ("file_fwd","w");
	lines = fopen("linesDB","w");
	cur++;
	fprintf (file_fwd, "%lld\n",blocks);
	fprintf (lines, "%lld\n",blocks);

	for(int i=0;i<blocks;i++){
	    fprintf (file_fwd, "%s\n",fwd[i].c_str());
	    if(i>0) linesDB[i] += linesDB[i-1];
	    fprintf (lines, "%lld\n",linesDB[i]);
  	}

	cout<<"clashes: "<<clashes<<endl;
	gettimeofday(&end, NULL);
	printf("time taken: %ld\n", ((end.tv_sec * 1000000 + end.tv_usec)
		  - (begin.tv_sec * 1000000 + begin.tv_usec)));

    return 0;
}


