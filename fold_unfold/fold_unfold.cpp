#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <bits/stdc++.h>
#include <sys/time.h>
using namespace std;
#define ll long long

std::list<pair(int,char *)> bitmat;

const ll no_threads = 8;
ll result[no_threads][1024];
ll block_sz;

void func(list<int,char *>::iterator it, ll sz){

}


int main(){
	
	std::thread myThreads[10];

	/*
	LIST OPERATIONS HERE
	*/
	list<int,char *>::iterator it[no_threads];
	ll bottom_array[1024],sz=bitmat.size();
	block_sz = (sz/no_threads) + (sz%no_threads!==0);
	it[0] = bitmat.begin();
	myThreads[0] = thread(func,it[0],min(block_sz,sz));
	for(ll i=1;i<no_threads;i++){
		it[i] = it[i-1];
    	advance(it[i], block_sz);
    	thread(func,it[i],min(block_sz,sz - block_sz*(i));
	}



	return 0;

}