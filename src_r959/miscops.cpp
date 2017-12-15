/*
 * Copyright 2011, 2012 Medha Atre
 * 
 * This file is part of BitMat.
 * 
 * BitMat is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * BitMat is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with BitMat.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Report any bugs or feature requests to <firstname>.<lastname>@gmail.com
 * ("firstname" and "lastname" of the first copyright holder above).
 */

#include "bitmat.hpp"

void best_match_subsumption(char *file)
{
	char fname[128];
	sprintf(fname, "%s_fin", file);
	FILE *fp = fopen64(fname, "r");
	assert(fp != NULL);

	FILE *ofstr = fopen64(file, "w");
	assert(ofstr != NULL);
	char buff[0xf00000];
	setvbuf(ofstr, buff, _IOFBF, 0xf00000);

	char line[1024], line_cp[1024]; char *c, *line_ptr; int num_space = 0;
	char int_str[12];
	vector<int> tuple, prev;

	while (!feof(fp)) {
		if(fgets(line, sizeof(line), fp) != NULL) {
			strcpy(line_cp, line);

			tuple.clear();
			c = strchr(line, '\n');
			assert(c != NULL);
			*c = '\0';

			int len = strlen(line);
//			num_space = 0;
	        assert(len <= 1024);
			if (num_space == 0) {
				for (int i = 0; i < len; i++) {
					if (isblank(line[i]))
						num_space++;
				}
			}
	        line_ptr = line;
	        for (int i = 0; i < num_space; i++) {
	        	c = strchr(line_ptr, ' ');
	        	*c = '\0';
   				strcpy(int_str, line_ptr);
   				tuple.push_back(atoi(int_str));

   				line_ptr = c+1;
	        }
	        if (prev.size() == 0) {
	        	fprintf(ofstr, "%s", line_cp);
	        	prev.insert(prev.end(), tuple.begin(), tuple.end());
	        } else {
	        	vector<int>::iterator tit = tuple.begin();
	        	for (vector<int>::iterator pit = prev.begin(); pit != prev.end() && tit != tuple.end(); pit++, tit++) {
	        		if (!(*pit == *tit || *tit == 0)) {
	        			//This is not dominated by prev tuple, so replace prev
	        			//and emit this tuple
	    	        	fprintf(ofstr, "%s", line_cp);
	    	        	prev.erase(prev.begin(), prev.end());
	    	        	prev.insert(prev.end(), tuple.begin(), tuple.end());
						break;
	        		}
	        	}
	        }

		}
	}
	fclose(fp); fclose(ofstr);
	unlink(fname);
}

void test_cycle_detection()
{
	vector<vector<struct node*>> cycles;

	/////////////////////
	//DEBUG
	////////////////////
	for (int i = 0; i < 6; i++) {
		JVAR *jvar = new JVAR;
		jvar->nodenum = -1 *(i+1);
		graph[i].data = jvar;
		jvar->gjvar = &graph[i];
		graph[i].nextTP = NULL;
		graph[i].numTPs = 0;
		graph[i].nextadjvar = NULL;
		graph[i].numadjvars = 0;
		graph[i].isNeeded = true;
		graph[i].type = JVAR_NODE;
	}
	// -1 --> -2, -3
	graph[0].nextadjvar = (LIST *) malloc (sizeof(LIST));
	graph[0].nextadjvar->gnode = &graph[1];
	graph[0].nextadjvar->next = (LIST *) malloc (sizeof(LIST));
	graph[0].nextadjvar->next->gnode = &graph[2];
	graph[0].nextadjvar->next->next = NULL;
	graph[0].numadjvars = 2;

	// -2 --> -1, -3
	graph[1].nextadjvar = (LIST *) malloc (sizeof(LIST));
	graph[1].nextadjvar->gnode = &graph[0];
	graph[1].nextadjvar->next = (LIST *) malloc (sizeof(LIST));
	graph[1].nextadjvar->next->gnode = &graph[2];
	graph[1].nextadjvar->next->next = NULL;
	graph[1].numadjvars = 2;

	// -3 --> -1, -2, -4
	graph[2].nextadjvar = (LIST *) malloc (sizeof(LIST));
	graph[2].nextadjvar->gnode = &graph[0];
	graph[2].nextadjvar->next = (LIST *) malloc (sizeof(LIST));
	graph[2].nextadjvar->next->gnode = &graph[1];
	graph[2].nextadjvar->next->next = (LIST *) malloc (sizeof(LIST));
	graph[2].nextadjvar->next->next->gnode = &graph[3];
	graph[2].nextadjvar->next->next->next = NULL;
	graph[2].numadjvars = 3;

	// -4 --> -3, -5, -6
	graph[3].nextadjvar = (LIST *) malloc (sizeof(LIST));
	graph[3].nextadjvar->gnode = &graph[2];
	graph[3].nextadjvar->next = (LIST *) malloc (sizeof(LIST));
	graph[3].nextadjvar->next->gnode = &graph[4];
	graph[3].nextadjvar->next->next = (LIST *) malloc (sizeof(LIST));
	graph[3].nextadjvar->next->next->gnode = &graph[6];
	graph[3].nextadjvar->next->next->next = NULL;
	graph[3].numadjvars = 3;

	// -5 --> -4, -6
	graph[4].nextadjvar = (LIST *) malloc (sizeof(LIST));
	graph[4].nextadjvar->gnode = &graph[3];
	graph[4].nextadjvar->next = (LIST *) malloc (sizeof(LIST));
	graph[4].nextadjvar->next->gnode = &graph[5];
	graph[4].nextadjvar->next->next = NULL;
	graph[4].numadjvars = 2;

	// -6 --> -4, -5
	graph[5].nextadjvar = (LIST *) malloc (sizeof(LIST));
	graph[5].nextadjvar->gnode = &graph[3];
	graph[5].nextadjvar->next = (LIST *) malloc (sizeof(LIST));
	graph[5].nextadjvar->next->gnode = &graph[4];
	graph[5].nextadjvar->next->next = NULL;
	graph[5].numadjvars = 2;

	graph_var_nodes = 6;

//	// -1 --> -2, -3
//	graph[0].nextadjvar = (LIST *) malloc (sizeof(LIST));
//	graph[0].nextadjvar->gnode = &graph[1];
//	graph[0].nextadjvar->next = (LIST *) malloc (sizeof(LIST));
//	graph[0].nextadjvar->next->gnode = &graph[2];
//	graph[0].nextadjvar->next->next = NULL;
//	graph[0].numadjvars = 2;
//
//	// -2 --> -4, -5
//	graph[1].nextadjvar = (LIST *) malloc (sizeof(LIST));
//	graph[1].nextadjvar->gnode = &graph[3];
//	graph[1].nextadjvar->next = (LIST *) malloc (sizeof(LIST));
//	graph[1].nextadjvar->next->gnode = &graph[4];
//	graph[1].nextadjvar->next->next = NULL;
//	graph[1].numadjvars = 2;
//
//	// -3 --> -1, -5
//	graph[2].nextadjvar = (LIST *) malloc (sizeof(LIST));
//	graph[2].nextadjvar->gnode = &graph[0];
//	graph[2].nextadjvar->next = (LIST *) malloc (sizeof(LIST));
//	graph[2].nextadjvar->next->gnode = &graph[4];
//	graph[2].nextadjvar->next->next = NULL;
//	graph[2].numadjvars = 2;
//
//	// -4 --> -2, -5
//	graph[3].nextadjvar = (LIST *) malloc (sizeof(LIST));
//	graph[3].nextadjvar->gnode = &graph[1];
//	graph[3].nextadjvar->next = (LIST *) malloc (sizeof(LIST));
//	graph[3].nextadjvar->next->gnode = &graph[4];
//	graph[3].nextadjvar->next->next = NULL;
//	graph[3].numadjvars = 2;
//
//	// -5 --> -2, -3, -4
//	graph[4].nextadjvar = (LIST *) malloc (sizeof(LIST));
//	graph[4].nextadjvar->gnode = &graph[1];
//	graph[4].nextadjvar->next = (LIST *) malloc (sizeof(LIST));
//	graph[4].nextadjvar->next->gnode = &graph[2];
//	graph[4].nextadjvar->next->next = (LIST *) malloc (sizeof(LIST));
//	graph[4].nextadjvar->next->next->gnode = &graph[3];
//	graph[4].nextadjvar->next->next->next = NULL;
//	graph[4].numadjvars = 3;
//
//	graph_var_nodes = 5;

//	// -1 --> -2
//	graph[0].nextadjvar = (LIST *) malloc (sizeof(LIST));
//	graph[0].nextadjvar->gnode = &graph[1];
//	graph[0].nextadjvar->next = NULL;
//	graph[0].numadjvars = 1;
//
//	// -2 --> -1, -3
//	graph[1].nextadjvar = (LIST *) malloc (sizeof(LIST));
//	graph[1].nextadjvar->gnode = &graph[0];
//	graph[1].nextadjvar->next = (LIST *) malloc (sizeof(LIST));
//	graph[1].nextadjvar->next->gnode = &graph[2];
//	graph[1].nextadjvar->next->next = NULL;
//	graph[1].numadjvars = 2;
//
//	// -3 --> -2
//	graph[2].nextadjvar = (LIST *) malloc (sizeof(LIST));
//	graph[2].nextadjvar->gnode = &graph[1];
//	graph[2].nextadjvar->next = NULL;
//	graph[2].numadjvars = 1;
//
//	graph_var_nodes = 3;

	get_longest_cycles(cycles);

/////////////////////////////////////////////////////////
//	//Moved to the above function
//	for (int i = 0; i < graph_var_nodes; i++) {
//		if (graph[i].type == JVAR_NODE) {
//			vector<struct node *>path; set<struct node *> visited;
//			path.push_back(&graph[i]);
//			enumerate_cycles(&graph[i], NULL, path, visited, cycles);
//			path.clear(); visited.clear();
//		}
//	}
//	cout << "----------- Cycles ------------" << endl;
//	unsigned int longest_cycle = 0;
//
//	for (vector<vector<struct node*> >::iterator itr = cycles.begin(); itr != cycles.end(); ) {
//		vector<struct node *> cycle = *itr;
//		struct node *first = cycle[0];
//		struct node *last = cycle[cycle.size()-1];
//
//		bool found = false;
//
//		for (LIST *next = first->nextadjvar; next; next = next->next) {
//			if (last == next->gnode) {
//				found = true;
//				break;
//			}
//		}
//		//Check if found a proper cycle
//		if (!found) {
//			itr = cycles.erase(itr);
//			continue;
//		}
//
//		if (longest_cycle < cycle.size()) {
//			longest_cycle = cycle.size();
//		}
//
//		cout << "[";
//		for (vector<struct node *>::iterator cit = cycle.begin(); cit != cycle.end(); cit++) {
//			cout << ((JVAR *)(*cit)->data)->nodenum << " ";
//		}
//		cout << "]" << endl;
//
//		itr++;
//	}
//
//	cout << "Longest cycle " << longest_cycle << endl;
//
//	vector<set<struct node *> > cycle_node_set;
//
//	for (vector<vector<struct node*> >::iterator itr = cycles.begin(); itr != cycles.end(); ) {
//		vector<struct node *> cycle = *itr;
//		//erase shorter cycles
//		if (cycle.size() < longest_cycle) {
//			itr = cycles.erase(itr);
//			continue;
//		}
//
//		set<struct node *> node_set; node_set.insert(cycle.begin(), cycle.end());
//		bool found_dupl = false;
//
//		for (vector<set<struct node *> >::iterator vit = cycle_node_set.begin(); vit != cycle_node_set.end(); vit++) {
//			set<struct node *> nset = *vit;
//			for (set<struct node *>::iterator sit = nset.begin(); sit != nset.end(); sit++) {
//				if (node_set.find(*sit) != node_set.end()) {
//					node_set.erase(*sit);
//				}
//			}
//
//			if (node_set.empty()) {
//				//a permutation of this cycle already exists so delete it
//				found_dupl = true;
//				break;
//			}
//		}
//		if (found_dupl) {
//			itr = cycles.erase(itr);
//			continue;
//		}
//
//		node_set.clear();
//		node_set.insert(cycle.begin(), cycle.end());
//		cycle_node_set.push_back(node_set);
//
////			cout << "[";
////			for (vector<struct node *>::iterator cit = cycle.begin(); cit != cycle.end(); cit++) {
////				cout << ((JVAR *)(*cit)->data)->nodenum << " ";
////			}
////			cout << "]" << endl;
//		itr++;
//	}
///////////////////////////////////////////////////////////
	cout << "Longest unique cycles" << endl;

	for (vector<vector<struct node*> >::iterator itr = cycles.begin(); itr != cycles.end(); ) {
		vector<struct node *> cycle = *itr;
		cout << "[";
		for (vector<struct node *>::iterator cit = cycle.begin(); cit != cycle.end(); cit++) {
			cout << ((JVAR *)(*cit)->data)->nodenum << " ";
		}
		cout << "]" << endl;
		itr++;
	}

}

void test_new_simple_fold()
{
	BitMat bitmat;
	unsigned int num_rows = 15, num_columns = 42, num_preds = 20;
	init_bitmat(&bitmat, num_rows, num_preds, num_columns, 0, SPO_BITMAT);
	bitmat.subfold[0] |= (0x80); bitmat.subfold[0] |= (0x80 >> 2); bitmat.subfold[0] |= (0x80 >> 3);
	bitmat.subfold[0] |= (0x80 >> 4); bitmat.subfold[1] |= (0x80 >> 6);
	unsigned int size = 0; struct row r = {1, NULL};

	//first row [0] 9 8 7 6
	unsigned char *data = (unsigned char *) malloc (21 * sizeof (unsigned char));
	unsigned int tmpval = 17;
	memcpy(data, &tmpval, ROW_SIZE_BYTES);
	data[ROW_SIZE_BYTES] = 0x00;

	tmpval = 9;
	memcpy(&data[ROW_SIZE_BYTES+1+size], &tmpval, GAP_SIZE_BYTES);
	size += GAP_SIZE_BYTES;
	tmpval = 8;
	memcpy(&data[ROW_SIZE_BYTES+1+size], &tmpval, GAP_SIZE_BYTES);
	size += GAP_SIZE_BYTES;
	tmpval = 7;
	memcpy(&data[ROW_SIZE_BYTES+1+size], &tmpval, GAP_SIZE_BYTES);
	size += GAP_SIZE_BYTES;
	tmpval = 6;
	memcpy(&data[ROW_SIZE_BYTES+1+size], &tmpval, GAP_SIZE_BYTES);
	size += GAP_SIZE_BYTES;
	assert(size+1 == 17);
	r.rowid = 1; r.data = data;
	bitmat.bm.push_back(r);

	//second row [0] 8 8 8 8
	data = (unsigned char *) malloc (21 * sizeof (unsigned char));
	tmpval = 17;
	memcpy(data, &tmpval, ROW_SIZE_BYTES);
	data[ROW_SIZE_BYTES] = 0x00;
	size = 0;

	tmpval = 8;
	memcpy(&data[ROW_SIZE_BYTES+1+size], &tmpval, GAP_SIZE_BYTES);
	size += GAP_SIZE_BYTES;
	tmpval = 8;
	memcpy(&data[ROW_SIZE_BYTES+1+size], &tmpval, GAP_SIZE_BYTES);
	size += GAP_SIZE_BYTES;
	tmpval = 8;
	memcpy(&data[ROW_SIZE_BYTES+1+size], &tmpval, GAP_SIZE_BYTES);
	size += GAP_SIZE_BYTES;
	tmpval = 8;
	memcpy(&data[ROW_SIZE_BYTES+1+size], &tmpval, GAP_SIZE_BYTES);
	size += GAP_SIZE_BYTES;
	assert(size+1 == 17);
	r.rowid = 3; r.data = data;
	bitmat.bm.push_back(r);

	//third row [0] 17 8 8 9
	data = (unsigned char *) malloc (21 * sizeof (unsigned char));
	tmpval = 17;
	memcpy(data, &tmpval, ROW_SIZE_BYTES);
	data[ROW_SIZE_BYTES] = 0x00;
	size = 0;

	tmpval = 17;
	memcpy(&data[ROW_SIZE_BYTES+1+size], &tmpval, GAP_SIZE_BYTES);
	size += GAP_SIZE_BYTES;
	tmpval = 8;
	memcpy(&data[ROW_SIZE_BYTES+1+size], &tmpval, GAP_SIZE_BYTES);
	size += GAP_SIZE_BYTES;
	tmpval = 8;
	memcpy(&data[ROW_SIZE_BYTES+1+size], &tmpval, GAP_SIZE_BYTES);
	size += GAP_SIZE_BYTES;
	tmpval = 9;
	memcpy(&data[ROW_SIZE_BYTES+1+size], &tmpval, GAP_SIZE_BYTES);
	size += GAP_SIZE_BYTES;
	assert(size+1 == 17);
	r.rowid = 4; r.data = data;
	bitmat.bm.push_back(r);

	//fourth row [1] 8 8 8 8 8
	data = (unsigned char *) malloc (25 * sizeof (unsigned char));
	tmpval = 21;
	memcpy(data, &tmpval, ROW_SIZE_BYTES);
	data[ROW_SIZE_BYTES] = 0x01;
	size = 0;

	tmpval = 8;
	memcpy(&data[ROW_SIZE_BYTES+1+size], &tmpval, GAP_SIZE_BYTES);
	size += GAP_SIZE_BYTES;
	tmpval = 8;
	memcpy(&data[ROW_SIZE_BYTES+1+size], &tmpval, GAP_SIZE_BYTES);
	size += GAP_SIZE_BYTES;
	tmpval = 8;
	memcpy(&data[ROW_SIZE_BYTES+1+size], &tmpval, GAP_SIZE_BYTES);
	size += GAP_SIZE_BYTES;
	tmpval = 8;
	memcpy(&data[ROW_SIZE_BYTES+1+size], &tmpval, GAP_SIZE_BYTES);
	size += GAP_SIZE_BYTES;
	tmpval = 8;
	memcpy(&data[ROW_SIZE_BYTES+1+size], &tmpval, GAP_SIZE_BYTES);
	size += GAP_SIZE_BYTES;

	assert(size+1 == 21);
	r.rowid = 5; r.data = data;
	bitmat.bm.push_back(r);

	//fifth row [1] 17 8 8
	data = (unsigned char *) malloc (17 * sizeof (unsigned char));
	tmpval = 13;
	memcpy(data, &tmpval, ROW_SIZE_BYTES);
	data[ROW_SIZE_BYTES] = 0x01;
	size = 0;

	tmpval = 17;
	memcpy(&data[ROW_SIZE_BYTES+1+size], &tmpval, GAP_SIZE_BYTES);
	size += GAP_SIZE_BYTES;
	tmpval = 8;
	memcpy(&data[ROW_SIZE_BYTES+1+size], &tmpval, GAP_SIZE_BYTES);
	size += GAP_SIZE_BYTES;
	tmpval = 8;
	memcpy(&data[ROW_SIZE_BYTES+1+size], &tmpval, GAP_SIZE_BYTES);
	size += GAP_SIZE_BYTES;

	assert(size+1 == 13);
	r.rowid = 15; r.data = data;
	bitmat.bm.push_back(r);

	assert(96 == count_triples_in_bitmat(&bitmat));

	unsigned char *maskarr = (unsigned char *) malloc (2 * sizeof(unsigned char));
	maskarr[0] = 0xaa; maskarr[1] = 0xaa;

	simple_unfold(&bitmat, maskarr, 2, COLUMN, 3);

//	assert(39 == bitmat.num_triples);
//	assert(39 == count_triples_in_bitmat(&bitmat));
	assert(19 == bitmat.num_triples);
	assert(19 == count_triples_in_bitmat(&bitmat));

	cout << "Exiting test_new_simple_fold" << endl;
}
/*
void load_mappings(char *subjfile, char *predfile, char *objfile)
{
	FILE *fp;
	unsigned int cnt = 0;
	int i;

	printf("Inside load_mapping\n");
	
	for (i = 0; i < 3; i++) {

		if (i == 0)
			fp = fopen64(subjfile, "r");
		else if (i == 1)
			fp = fopen64(predfile, "r");
		else if (i == 2)
			fp = fopen64(objfile, "r");

		if (fp == NULL) {
			printf("Error opening mapping file\n");
			exit(-1);
		}

		char line[3072];
		char digit[15], str[2048];
		unsigned int intmapping = 0;
		char *c=NULL, *c2=NULL;

		memset(line, 0, 3072);

		while (!feof(fp)) {

			if(fgets(line, sizeof(line) - 1, fp) != NULL) {
				c = strchr(line, ' ');
				*c = '\0';
				strncpy(digit, line, 10);
				intmapping = atoi(digit);

				if (intmapping != cnt + 1) {
					printf("****ERROR: count mismatch intmapping %u digit %s cnt %d\n", intmapping, digit, cnt+1);
					exit(-1);
				}

				c2 = strchr(c+1, '\n');
				*c2 = '\0';
				strncpy(str, c+1, 2048);

				if (i == 0) {
					subjmapping[cnt] = (unsigned char *) malloc (strlen(str) + 1);
					strcpy((char *)subjmapping[cnt], str);
				} else if (i == 1) {
					predmapping[cnt] = (unsigned char *) malloc (strlen(str) + 1);
					strcpy((char *)predmapping[cnt], str);
				} else if (i == 2) {
					objmapping[cnt] = (unsigned char *) malloc (strlen(str) + 1);
					strcpy((char *)objmapping[cnt], str);
				}
				cnt++;
			}
		}

		cnt = 0;
		fclose(fp);
	}
	printf("Exiting load_mapping\n");
}
/////////////////////////////////////////
void init_mappings()
{

	subjmapping = (unsigned char **) malloc (gnum_subs * sizeof(unsigned char *));
	predmapping  = (unsigned char **) malloc (gnum_preds * sizeof(unsigned char *));
	objmapping = (unsigned char **) malloc (gnum_objs * sizeof(unsigned char *));

	memset(subjmapping, 0, gnum_subs * sizeof (unsigned char *));
	memset(predmapping, 0, gnum_preds * sizeof (unsigned char *));
	memset(objmapping, 0, gnum_objs * sizeof (unsigned char *));
}
*/
