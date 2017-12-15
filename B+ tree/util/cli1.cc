#include "../bpt.h"
using namespace bpt;
#include <fcntl.h>
#include <string.h>


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

vector<pair<value_t, int> > a; //for offline;  result, query number
value_t ans_rev[10000005] ;      //stores answer of rev offline, max 1e7 size
int line_number[10000005];
const int N= 5005; // total databases (max)
char fwd[N][400000];
ull rev[N];
bpt::bplus_tree *dbf[N],*dbr[N];
char argv1_copy[40],argv2_copy[40];

//----------------I/O INFORMATION-----------------------//
//command : ./bpt_cli forward_db_prefix reverse_db_prefix data_file 1/2  file_name //1-fwd, 2-rev
//sample forward : ./bpt_cli folder/f_t folder/r_t all_data_commso 1 hunny
//sample reverse : ./bpt_cli folder/f_t folder/r_t all_data_commso 2 hunny
//for 2, input from argv[5], output offline in "output"
//for 1, input argv[5], output online on terminal
//-----------------------------------------------------//
char str[400000];
int main(int argc, char *argv[])
{
    clock_t tm = clock(),tm2=clock();

    char buf[10];
    int rlen=strlen(argv[2]),flen=strlen(argv[1]);
    int tot_db;  //total number of databases

    if(!strcmp(argv[4],"1")){	//fwd query

    	strcpy(argv1_copy,argv[1]);
    	freopen("file_fwd","r",stdin);	//file_fwd has starting strings of each db

    	cin>>tot_db;
    	getchar();
    	for(int i=0;i<tot_db;i++){
        	gets(fwd[i]);
          //printf("%d\n%s\n\n",i,fwd[i]);
        	sprintf(buf,"%d",i);
        	argv1_copy[flen]='\0';
        	strcat(argv1_copy,buf);
        	dbf[i]=new bpt::bplus_tree(argv1_copy);
        }

    	tm2=clock()-tm2;
        cout<<"\n\n\ndatabase fwd loaded: time= "<<(double)(tm2)/CLOCKS_PER_SEC << "\n";
    	tm2=clock();

        ull value;  // to store output of query
        bpt::key_t start;       //the key to beequeried (formed from num)
        int ind=0;          //index of query's db

        freopen(argv[5],"r",stdin);
        freopen("output_fwd","w",stdout);
        while(gets(str)!=NULL)
        {
			ind=0;
			int lo=0,hi=tot_db-1,mi;		//binary search to find index of db (low,high,mid)
			while(lo<hi){
				mi=lo+hi+1>>1;
				if(strcmp(str,fwd[mi])<0)hi=mi-1;
				else lo=mi;
			}
			ind=lo;
	//        cout<<"found index of query's db: "<< ind<<endl;
	//        tm2=clock()-tm2;
	//        cout<<(double)(tm2)/CLOCKS_PER_SEC << "\n";
	//        tm2=clock();

			start.k=make_pair(hash1(str),hash2(str));
			if (dbf[ind]->search(start, &value) != 0){
				printf("Key %s not found\n", str);
				cout<<start.k.first<<endl<<start.k.second<<endl;
			}
				else printf("%llu\n",value);
        }
    }

    else{	//rev query

        strcpy(argv2_copy,argv[2]);
    	freopen("file_rev","r",stdin);	//file_rev has starting strings of each db
    	cin>>tot_db;
    	for(int i=0;i<tot_db;i++){
        	cin>>rev[i];
        	sprintf(buf,"%d",i);
        	argv2_copy[rlen]='\0';
        	strcat(argv2_copy,buf);
        	dbr[i]=new bpt::bplus_tree(argv2_copy);
        }

        tm2=clock()-tm2;
        cout<<"\n\n\ndatabase rev loaded: time= "<<(double)(tm2)/CLOCKS_PER_SEC << "\n";
        tm2=clock();

    	FILE *pf= fopen ("output_rev","w");     //to dump output;
    	FILE *f=fopen(argv[3],"r");	//data file

    	ull num;        //number to be read
    	ull value;  // to store output of query
    	int cnt = 1;       // how many read
    	freopen(argv[5],"r",stdin);     //which all line numbers to find;
    	bpt::key_t start;       //the key to bee queried (formed from num)
        int ind=0;          //index of query's db

    	while (scanf("%llu",&num)!=EOF)
    	{
    	    ind=0;
    	    int lo=0,hi=tot_db-1,mi;
    	    while(lo<hi){
    	    	mi=lo+hi+1>>1;
    	    	if(rev[mi]>num)hi=mi-1;
    	    	else lo=mi;
    	    }
    	    ind=lo;
    	    //cout<<ind<<endl;
    	    line_number[cnt]=num;
    	    //while(ind<tot_db-1 && rev[ind+1]<=num)ind++;
    	    start.k=make_pair(num,num);
    		if (dbr[ind]->search(start, &value)){
    			printf("Key %llu not found\n", num);
    			cout<<start.k.first<<endl<<start.k.second<<endl;
    		}
    		else a.push_back(make_pair(value,cnt));
    		if(cnt%10000==0){
    			cout<<cnt<<endl;
    			tm2=clock()-tm2;
				cout<<(double)(tm2)/CLOCKS_PER_SEC << "\n";
				tm2=clock();
    		}
    		cnt++;
    	}

      tm2=clock()-tm2;
      cout<<(double)(tm2)/CLOCKS_PER_SEC << "\n";
      tm2=clock();

      cout<<"\n\n\nsorting started\n\n\n\n";
	  sort(a.begin(),a.end());
	  cout<<"\n\n\nsorting finished\n\n\n\n";

      tm2=clock()-tm2;
      cout<<(double)(tm2)/CLOCKS_PER_SEC << "\n";
      tm2=clock();

      cnt=1;

      //loop over file to find all indexes
      for(auto x:a){
      fseek( f, x.X, SEEK_SET );
		fscanf (f , "%s" , str);
		// print query number, line number, string \n
		fprintf(pf,"%d : %d : %s\n",x.Y,line_number[x.Y],str);
		if(cnt%10000==0)
			cout<<cnt<<endl;
			cnt++;
      	}

    	fclose(pf);
    	fclose(f);
    }


    tm = clock()-tm ;
    cout <<"total time: " <<(double)(tm)/CLOCKS_PER_SEC << "sec\n";

    return 0;
}
