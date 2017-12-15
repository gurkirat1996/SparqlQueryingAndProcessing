#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
//#include <value_t>
#include <bits/stdc++.h>
using namespace std;
#include <fstream>
#include <string>
#include <sys/wait.h>
#include <fstream>
#include <time.h>

int main(int argc, char *argv[])
{	
	ios_base::sync_with_stdio(false);cin.tie(NULL);
	FILE * pFile;
  	long long size;

  	pFile = fopen ("../datasets/lubm1b","r");
  	if (pFile==NULL) perror ("Error opening file");
  	else
  	{
    	fseek (pFile, 0, SEEK_END);   // non-portable
    	size=ftell (pFile);

    	fclose (pFile);
  	}

 //  	FILE * qFile;
 //  	char s[50];
	// qFile = fopen ( "example.txt" , "r" );
	// //fputs ( "This is an apple." , pFile );
	// fseek ( qFile , 2 , SEEK_SET );
	// while(fgets(s,50,qFile)!=NULL)
	// 	cout<<s;

	// fclose ( qFile );
	int num_thr=8;
	for(long long i=0;i<num_thr;i++){
		int p=fork();
		if(p==0){
  		long long count=0,sz=0;
  		char s[50001];
  		//FILE *f1 = fopen("data.txt","r");
  		std::ifstream infile("../datasets/lubm1b");
  		infile.seekg (i*size/num_thr, infile.beg);
  		//fseek(f1,0,SEEK_SET);
  		while(sz<size/num_thr){
  			infile>>s;
  			sz+=strlen(s);
  			count++;
  			if(count%10000==0)
  				cout<<i<<' '<<count<<endl;
  			
  		}
  		cout<<"Process #"<<i<<" complete \n Count<<"<< sz<<endl;
		return 0;
		}
//		fflush(stdout);
		sleep(2);

		cout<<"Returned from sleep "<<i<<endl;
	}  	
  		long long count=0,sz=0;
  		char s[50001];
  		//FILE *f1 = fopen("data.txt","r");
  		std::ifstream infile("../datasets/lubm1b");
  		infile.seekg ((num_thr-1)*size/num_thr, infile.beg);
  		//fseek(f1,0,SEEK_SET);
  		while(sz<size/num_thr){
  			infile>>s;
  			sz+=strlen(s);
  			count++;
  			if(count%10000==0)
  				cout<<num_thr-1<<' '<<count<<endl;
  			
  		}
		
  		cout<<"Parent #"<<num_thr-1<<" complete \n Count<<"<< sz<<endl;
  		wait(NULL);
		cout<<"\nyo\n";
    return 0;
}
