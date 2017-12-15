#include <bits/stdc++.h>
using namespace std;
// #include "./B+ tree/bpt.h"
// using namespace bpt;
#include <fcntl.h>
#include <string.h>
// #include "cli1.h"

void reverse_lookup(vector <int> input, vector <string> &output);
void bpt_initialize(int forward = 1, int reverse = 1, int pointLookup=0);

int main(){

	srand(time(NULL));
	vector<int> out = {};
	for (int i=0;i<1000000;i++){
		out.push_back(rand()%200000000);
	}
	
	vector<string> in = {};
	cout<<"vector generated\n";
	bpt_initialize(0,1);

	cout<<"initialize complete\n";
	reverse_lookup(out, in);
	cout<<"querying complete\n";
	freopen("output_rev", "w", stdout);
	// for (auto x: in)cout<<x;
	// cout<<"hello";
}