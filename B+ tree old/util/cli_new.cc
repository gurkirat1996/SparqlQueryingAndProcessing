/*
UPDATED CODE MAY CONTAIN BUGS. NOT CHECKED CAREFULLY. F
CHANGES- REVERSE FILE SCANNING TO OUPUT STRINGS OCCURS PARALLELY. HOPING FOR MAJOR TIME IMPROVEMENT IN REVERSE QUERIES
NO CHANGES TO FORWARD QUERYING (ALREADY PRETTY FAST!)
*/
#include <bits/stdc++.h>
#include <sys/time.h>
using namespace std;
#include "../bpt.h"
using namespace bpt;
#include <fcntl.h>
#include <string.h>
#define ll long long

ull hash1(char* str)
{
    ull hash = 5381;
    ll c;
    while (c = *str++){
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    }
    return hash;
}

ull hash2(char* str)
{
    ull hash = 5381;
    ll c;
    while (c = *str++){
        hash = c + (hash << 6) + (hash << 16) - hash;
    }

    return hash;
}

const ll N= 300000, Q = 10000001; // total databases (max)
const ll num_thr=1;

vector  <pair<value_t, ll> > a; //for offline;  result, query number
string fwd[N];
ll linesDB[N];
string result[Q]; // for stroring the final strings
ll inde[Q];       // for storing actual indexes of queries
bpt::bplus_tree *dbf[N],*dbr[N];
char argv1_copy[40],argv2_copy[40],argv_copy[40];
char str[400001];   //400001 is the max length of any query string
ll total_queries;

//----------------I/O INFORMATION-----------------------//
//command : ./bpt_cli forward_db_prefix reverse_db_prefix data_file 1/2  file_name //1-fwd, 2-rev
//sample forward : ./bpt_cli folder/f_t folder/r_t all_data_commso 1 hunny
//sample reverse : ./bpt_cli folder/f_t folder/r_t   all_data_commso 2 hunny
//for 2, input from argv[5], output offline in "output"
//for 1, input argv[5], output online on terminal
//-----------------------------------------------------//


void func (ll last,ll query_start,char *filename)
{
    FILE *f=fopen(filename,"r");
    if (f==NULL) perror ("Error opening file");
    
    ll x = query_start;
    char s[400001];
    while(a[x].first <last){
        fseek ( f , a[x].first , SEEK_SET );
        fgets(s,400001,f);
        result[x].assign(begin(s),end(s)-1);
        x++;
        if(x>=total_queries)break;
    }

}
vector< pair<ll, ll > > v1;

int main(ll argc, char *argv[])
{

    char buf[10];
    ll rlen=strlen(argv[2]),flen=strlen(argv[1]);
    ll tot_db;  //total number of databases
    strcpy(argv_copy,argv[3]);
    if(!strcmp(argv[4],"1")){   //fwd query

        strcpy(argv1_copy,argv[1]);
        freopen("file_fwd","r",stdin);  //file_fwd has starting strings of each db

        cin>>tot_db;
        getchar();
	ll hunny=0;
	char s[400001];
        for(ll i=0;i<tot_db;i++){
            gets(s);  //##
	    fwd[i]=s;
            sprintf(buf,"%d",i);
            argv1_copy[flen]='\0';
            strcat(argv1_copy,buf);
	    cerr<<"here "<<i<<endl;
            dbf[i]=new bpt::bplus_tree(argv1_copy);
            
        }
        freopen("linesDB","r",stdin);   //linesDB contains prefix sum of #lines in each db
        cin>>tot_db;
        for(ll i=0;i<tot_db;i++){
            cin>>linesDB[i];
        }

        ull value;  // to store output of query
        bpt::key_t start;       //the key to beequeried (formed from num)
        ll ind=0;          //index of query's db
	cerr<<"before freopen\n";
        freopen(argv[5],"r",stdin);
	cerr<<"after freopen\n";
        freopen("output_fwd","w",stdout);
        while(gets(str)!=NULL)
        {
            ind=0;
            ll lo=0,hi=tot_db-1,mi;        //binary search to find index of db (low,high,mid)
            while(lo<hi){
                mi=lo+hi+1>>1;
                if(strcmp(str,fwd[mi].c_str())<0)hi=mi-1;
                else lo=mi;
            }
            ind=lo;
    //        cout<<"found index of query's db: "<< ind<<endl;
    //        tm2=clock()-tm2;
    //        cout<<(double)(tm2)/CLOCKS_PER_SEC << "\n";
    //        tm2=clock();
            ll offset = (ind>0?linesDB[ind-1]:0);      //line offset
            start.k=make_pair(hash1(str),hash2(str));
            if (dbf[ind]->search(start, &value) != 0){
                printf("Key %s not found\n", str);
                cout<<start.k.first<<endl<<start.k.second<<endl;
            }
                else printf("%llu\n",value+offset);
        }
    }

    else{   //rev query
	cerr<<"entered\n";
        strcpy(argv2_copy,argv[2]);
        freopen("linesDB","r",stdin);  //file_rev has starting strings of each db
       	cerr<<"opened linesDB\n";
	 cin>>tot_db;
	ll hunny=0;
        for(ll i=0;i<tot_db;i++){
            cin>>linesDB[i];
            sprintf(buf,"%lld",i);
            argv2_copy[rlen]='\0';
            strcat(argv2_copy,buf);
//	    cerr<<"here\n";
            dbr[i]=new bpt::bplus_tree(argv2_copy);
//	    cerr<<"here2\n";
//	    cout<<hunny++;
        }

        FILE *pf= fopen ("output_rev","w");     //to dump output;
	cerr<<"before data file\n";
        FILE *f=fopen(argv[3],"r"); //data file
	cerr<<"after data fifo\n";
        ull num;        //number to be read
        ull value;  // to store output of query
        ll cnt = 0;       // how many read
        freopen(argv[5],"r",stdin);     //which all line numbers to find;
        bpt::key_t start;       //the key to bee queried (formed from num)
        ll ind=0;          //index of query's db
	cerr<<"tot_db "<<tot_db<<endl;
        while (scanf("%llu",&num)!=EOF)v1.push_back({num, cnt});
	sort(v1.begin(), v1.end());
	for(auto x: v1){
	    ll num=x.first;
            ind=upper_bound(linesDB, linesDB+tot_db, num) - linesDB ;
//	    cerr<<"ind "<<ind<<endl;
	    if(ind>0 and linesDB[ind-1]==num)ind--;
//	    cerr<<ind<<' '<<num<<endl;
            //line_number[cnt]=num; no need(?)
            num -= (ind>0?linesDB[ind-1]:0);
            start.k=make_pair(num,num);
            if (dbr[ind]->search(start, &value)){
                printf("Key %llu not found\n", num);
                cout<<ind<<' '<<start.k.first<<endl;
            }
            else a.push_back(make_pair(value,x.second));
            if(cnt%1000==0){
                cout<<cnt<<endl;
                
            }
            cnt++;
        }

        cerr<<"\n\n\nsorting started\n\n\n\n";
        sort(a.begin(),a.end());
        cerr<<"\n\n\nsorting finished\n\n\n\n";

        //offlines algorithm begins
        std::thread myThreads[num_thr];

        total_queries = a.size();
        ll que_start[num_thr],j=0;      //que_start indicates from which byte each thread will start scanning
        ll tot_sz = (a[total_queries-1].first);
        ll sz = tot_sz/num_thr+1;
        
        memset(que_start,-1,sizeof(que_start));
        
        for(ll i=0;i<total_queries;i++){
            while(a[i].first >= (j+1)*sz){
                j++;
            }
            if(a[i].first >= j*sz and a[i].first < (j+1)*sz){
                que_start[j] = i;
                j++;
            }
            
        }

        //for(int i=0;i<num_thr;i++)cout<<que_start[i]<<endl;
        for(ll i=0;i<total_queries;i++)inde[a[i].second] = i;  //actual query index of ith entry in a
        
        for(ll i=0;i<num_thr;i++){
	if(que_start[i]!=-1)
                myThreads[i] = std::thread(func,min(tot_sz+1,(i+1)*sz),que_start[i],argv[3]);
        }

        a.clear();

        for(ll i=0;i<num_thr;i++)
            myThreads[i].join();
        
        //output
        freopen("output_rev","w",stdout);
        for(ll i=0;i<total_queries;i++)printf("%s", result[inde[i]].c_str());
      
      }


    return 0;
}

