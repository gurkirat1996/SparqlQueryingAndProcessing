#include <bits/stdc++.h>
using namespace std;

int main(){

	srand(time(NULL));
	vector<long long> in;
	long long lines = 200000000;
	for (int i=0;i<10000000;i++){
		in.push_back(rand()%lines);
	}
	sort(in.begin(), in.end());

	in.resize(distance(in.begin(), unique(in.begin(), in.end())));
	cerr<<in.size();
	return 0;
	FILE * f = fopen("data.txt","r");
	char str[400005];
	int len = in.size();
	int cur_ind = 0;
	vector<string> out;
	long long y=0;

	while(fscanf(f, "%s", str)!=EOF){

		y++;
		if(in[cur_ind] == y){
			out.push_back(str);		
			cur_ind++;
			if(cur_ind%1000==0)cout<<cur_ind<<endl;
			if(cur_ind == len)break;
		}
		
	}

	return 0;
}