/*
 * Copyright 2010-2017 Medha Atre
 * 
 * This file is part of BitMat.
 * 
 * BitMat is a free software: you can redistribute it and/or modify
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

unsigned int gnum_subs, gnum_preds, gnum_objs, gnum_comm_so, gsubject_bytes, gobject_bytes, gpredicate_bytes, gcommon_so_bytes;//, grow_size;
unsigned int row_size_bytes, gap_size_bytes;
unsigned int comp_folded_arr;
map <std::string, std::string> config;
bool resbitvec = false;
char *buf_out = (char *) malloc (0xf000000);

////////////////////////////////////////////////////////////
void test_function(char *fname_dump, unsigned int numbms, bool compfold, unsigned int numsubs)
{
	char tablefile[2048];
	sprintf(tablefile, "%s_table", fname_dump);

	int fd_table = open(tablefile, O_RDONLY | O_LARGEFILE);

	assert(fd_table != -1);

	unsigned long offset = 0;

	int fd = open(fname_dump, O_RDONLY | O_LARGEFILE);

	assert(fd != -1);

	unsigned int numtriples = 0;

	unsigned int subject_bytes = (numsubs%8>0 ? numsubs/8+1 : numsubs/8);

	unsigned char subfold[subject_bytes];
	memset(subfold, 0, subject_bytes);

	cout << "Subject_bytes " << subject_bytes << endl;

	cout << fname_dump << " " << tablefile << endl;

	for (unsigned int i = 0; i < numbms; i++) {
//		offset = get_offset(fname_dump, i+1);
//		cout << "Offset is " << offset << " ";
		read(fd_table, &offset, table_col_bytes);
		assert (lseek(fd, offset, SEEK_SET) != -1);
		//Moving over the numtriples field
		read(fd, &numtriples, sizeof(unsigned int));
		cout << "***#triples " << numtriples << " ";
		cout << "***Counting rows for bmnum " << i + 1 << " -- ";
		if (compfold) {
			unsigned int comp_arr_size = 0;
			read(fd, &comp_arr_size, ROW_SIZE_BYTES);
			unsigned char *comp_arr = (unsigned char *) malloc (comp_arr_size);
			dgap_uncompress(comp_arr, comp_arr_size, subfold, subject_bytes);
		} else {
			read(fd, subfold, subject_bytes);
		}

		//Now count set bits in subfold
		cout << count_bits_in_row(subfold, subject_bytes) << endl;

		offset = 0;
	}
}
////////////////////////////////////////////////////////////
int main(int args, char **argv)
{
	struct timeval start_time, stop_time, start_prune, start_final, start_init;
//	clock_t  t_start, t_end;
	double curr_time;
	double st, en, prune, init;
	int c = 0;
	char qfile[25][124], outfile[25][124];
	bool loaddata = false, querydata = true, pruning = false;
	char *dump_file = NULL;

	int q_count = 0, op_count = 0;

	if (args < 11) {
		printf("Copyright 2010-2017 Medha Atre\n\n");
		printf("Usage: bitmat -l [y/n] -Q [y/n] -f config-file -q query-file -o res-output-file -d dump_file\n\n");
		exit (-1);
	}

	printf("Copyright 2010-2017 Medha Atre\n\n");

	while((c = getopt(args, argv, "t:l:Q:f:q:o:d:")) != -1) {
		switch (c) {
			case 'f':
				parse_config_file(optarg);
				break;
			case 'l':
				if (!strcmp(optarg, "y")) {
					loaddata = true;
				}
				break;
			case 'Q':
				if (!strcmp(optarg, "n")) {
					cout << "Query option" << endl;
					querydata = false;
				}
				break;
			case 'q':
				strcpy(qfile[q_count], optarg);
				q_count++;
				break;
			case 'o':
				strcpy(outfile[op_count], optarg);
				op_count++;
				break;
			case 't':
				if (!strcmp(optarg, "b")) {
					resbitvec = true;
				} else if (!strcmp(optarg, "h")) {
					resbitvec = false;
				} else
					assert(0);
				assert(resbitvec == RESBITVEC);
				break;
			case 'd':
				dump_file = (char *)malloc (strlen(optarg));
				strcpy(dump_file, optarg);
				break;
			default:
				printf("Usage: bitmat -f config-file -q query-file -o res-output-file\n");
				exit (-1);

		}
	}

	printf("Process id = %d\n", (unsigned)getpid());

	if (row_size_bytes != ROW_SIZE_BYTES || gap_size_bytes != GAP_SIZE_BYTES) {
		cerr << "**** ERROR: Descrepancy in the row/gap_size_bytes values" << endl;
		cerr << "row_size_bytes " << row_size_bytes << " ROW_SIZE_BYTES " << ROW_SIZE_BYTES << 
			" gap_size_bytes " << gap_size_bytes << " GAP_SIZE_BYTES " << GAP_SIZE_BYTES << endl;
		exit(-1);
	}

	if (loaddata) {
		gettimeofday(&start_time, (struct timezone *)0);

		vector<struct twople> triplelist;

		cout << "*********** LOADING BITMATS ***********" << endl;
		BitMat bmorig_spo;
		init_bitmat(&bmorig_spo, gnum_subs, gnum_preds, gnum_objs, gnum_comm_so, SPO_BITMAT);
		cout << "Loading vertically for SPO bitmat" << endl;
		load_data_vertically((char *)config[string("RAWDATAFILE_SPO")].c_str(), triplelist, &bmorig_spo, (char *)config[string("BITMATDUMPFILE_SPO")].c_str(), true, false, true, (char *)config[string("TMP_STORAGE")].c_str());
		bmorig_spo.freebm();

		BitMat bmorig_ops;
		init_bitmat(&bmorig_ops, gnum_objs, gnum_preds, gnum_subs, gnum_comm_so, OPS_BITMAT);
		cout << "Loading vertically for OPS bitmat" << endl;
		load_data_vertically((char *)config[string("RAWDATAFILE_OPS")].c_str(), triplelist, &bmorig_ops, (char *)config[string("BITMATDUMPFILE_OPS")].c_str(), true, false, true, (char *)config[string("TMP_STORAGE")].c_str());
		bmorig_ops.freebm();

		BitMat bmorig_pso;
		init_bitmat(&bmorig_pso, gnum_preds, gnum_subs, gnum_objs, gnum_comm_so, PSO_BITMAT);
		cout << "Loading vertically for PSO bitmat" << endl;
		load_data_vertically((char *)config[string("RAWDATAFILE_PSO")].c_str(), triplelist, &bmorig_pso, (char *)config[string("BITMATDUMPFILE_PSO")].c_str(), true, false, true, NULL);
		bmorig_pso.freebm();

		BitMat bmorig_pos;
		init_bitmat(&bmorig_pos, gnum_preds, gnum_objs, gnum_subs, gnum_comm_so, POS_BITMAT);
		cout << "Loading vertically for POS bitmat" << endl;
		load_data_vertically((char *)config[string("RAWDATAFILE_POS")].c_str(), triplelist, &bmorig_pos, (char *)config[string("BITMATDUMPFILE_POS")].c_str(), true, false, true, NULL);
		bmorig_pos.freebm();

		gettimeofday(&stop_time, (struct timezone *)0);
		en = stop_time.tv_sec + (stop_time.tv_usec/MICROSEC);

		st = start_time.tv_sec + (start_time.tv_usec/MICROSEC);
		curr_time = en-st;

		printf("Time for loading(gettimeofday): %f\n", curr_time);

	}
//////////////////////////////////////////////////////
//// Querying code
///////////////////

	if (querydata) {

		vector<struct twople> triplelist;
		gettimeofday(&start_time, (struct timezone *)0);

		bool opt = parse_query_and_build_graph(qfile[0]);
//		print_graph();
		vector<vector<struct node*>> cycles;
		bool best_match_reqd = get_longest_cycles(cycles);

		bool all_slaves_one_jvar = !(slaves_with_more_than_one_jvar());

		if (cycles.size() > 0 && !best_match_reqd) {
			//means absolute masters have a cycle
			//so check if any slave has more than one tp
			best_match_reqd = !all_slaves_one_jvar;
		}

		gettimeofday(&stop_time, (struct timezone *)0);
		en = stop_time.tv_sec + (stop_time.tv_usec/MICROSEC);

		st = start_time.tv_sec + (start_time.tv_usec/MICROSEC);
		curr_time = en-st;

		printf("Time for parse_query_and_build_graph(gettimeofday): %f\n", curr_time);
		bool bushy = false;

		gettimeofday(&start_init, (struct timezone *)0);
		
//		cout << "Before init_tp_nodes_new" << endl;
		if (!init_tp_nodes_new(bushy)) {
			cout << "Query has 0 results" << endl;
			gettimeofday(&stop_time, (struct timezone *)0);
			en = stop_time.tv_sec + (stop_time.tv_usec/MICROSEC);

			st = start_init.tv_sec + (start_init.tv_usec/MICROSEC);
			curr_time = en-st;

			printf("Time for init_tp_nodes_new(gettimeofday): %f\n", curr_time);
//			printf("Time for init_tp_nodes_new: %f\n", (((double)clock()) - t_start)/CLOCKS_PER_SEC);

			return 0;
		}
//		cout << "Done init_tp_nodes_new" << endl;

		build_jvar_tree(best_match_reqd, (cycles.size() > 0), all_slaves_one_jvar);

		if (!best_match_reqd) {
			best_match_reqd = !(is_allslave_bottomup_pass());
		}

//		cout << "Done build_jvar_tree ***** best_match_reqd " << best_match_reqd << endl;

		if (!populate_all_tp_bitmats()) {
//			cout << "Done populate_all_tp_bitmats" << endl;
			cout << "**** Query has 0 results" << endl;
			gettimeofday(&stop_time, (struct timezone *)0);
			en = stop_time.tv_sec + (stop_time.tv_usec/MICROSEC);

			st = start_init.tv_sec + (start_init.tv_usec/MICROSEC);
			curr_time = en-st;

			printf("Time for init_tp_nodes_new(gettimeofday): %f\n", curr_time);
//			printf("Time for init_tp_nodes_new: %f\n", (((double)clock()) - t_start)/CLOCKS_PER_SEC);
			return 0;
		}

//		cout << "Done populate_all_tp_bitmats" << endl;
		gettimeofday(&stop_time, (struct timezone *)0);
		en = stop_time.tv_sec + (stop_time.tv_usec/MICROSEC);

		st = start_init.tv_sec + (start_init.tv_usec/MICROSEC);
		curr_time = en-st;

		printf("Time for init_tp_nodes_new(gettimeofday): %f\n", curr_time);
//		printf("Time for init_tp_nodes_new: %f\n", (((double)clock()) - t_start)/CLOCKS_PER_SEC);

//		cout << "Pruning triples" << endl;
		gettimeofday(&start_prune, (struct timezone *)0);

		if (!prune_triples_new(bushy, opt, cycles, best_match_reqd, all_slaves_one_jvar)) {
			cout << "**** Query has 0 results" << endl;

			gettimeofday(&stop_time, (struct timezone *)0);
			en = stop_time.tv_sec + (stop_time.tv_usec/MICROSEC);

			st = start_prune.tv_sec + (start_prune.tv_usec/MICROSEC);
			curr_time = en-st;

			printf("Time for prune_triples_new(gettimeofday): %f\n", curr_time);

		} else {

//			cout << "Done prune_triples_new" << endl;

			gettimeofday(&stop_time, (struct timezone *)0);
			en = stop_time.tv_sec + (stop_time.tv_usec/MICROSEC);

			st = start_prune.tv_sec + (start_prune.tv_usec/MICROSEC);
			curr_time = en-st;

			printf("Time for prune_triples_new(gettimeofday): %f\n", curr_time);

			if (dump_file != NULL) {
				get_all_triples_in_query(dump_file);
				return 0;
			}

//			printf("Time for prune_triples_new: %f\n", (((double)clock()) - t_prune)/CLOCKS_PER_SEC);

			FILE *ofstr = NULL;

			if (best_match_reqd) {
				ofstr = fopen64((char *)config[string("TMP_STORAGE")].c_str(), "w");
				assert(ofstr != NULL);
				setvbuf(ofstr, buf_out, _IOFBF, 0xf000000);
			} else {
				ofstr = fopen64(outfile[0], "w");
				assert(ofstr != NULL);
				setvbuf(ofstr, buf_out, _IOFBF, 0xf000000);
			}

			gettimeofday(&start_final, (struct timezone *)0);
			bool doneQuery = update_graph(ofstr);

//			cout << "Done update_graph" << endl;

			if (!doneQuery) {

				fix_all_tp_bitmats();

				if (update_graph_for_final()) {

					vector<struct node *> tplist = spanning_tree_tpnodes();

					fix_all_tp_bitmats2(tplist);

					int num_null_pads = 0;
					vector<TP *>null_tps;

					for (set<string>::iterator sit = null_perm.begin(); sit != null_perm.end(); sit++) {
						for (int i = 0; i < graph_tp_nodes; i++) {
							if (!graph[MAX_JVARS_IN_QUERY + i].isNeeded)
								continue;
							TP *tp = (TP*)graph[MAX_JVARS_IN_QUERY + i].data;
							//A needed node that's in null_perm list, which is not listed in result gen tplist
							if (null_perm.find(tp->strlevel) != null_perm.end() &&
									(find(tplist.begin(), tplist.end(), &graph[MAX_JVARS_IN_QUERY+i]) == tplist.end())) {
								if (tp->sub < 0)
									num_null_pads++;
								if (tp->pred < 0)
									num_null_pads++;
								if (tp->obj < 0)
									num_null_pads++;
								null_tps.push_back(tp);
							}
						}
					}

					struct node *tparr[tplist.size()+1];
					int i = 0;
					for (vector<struct node *>::iterator it = tplist.begin(); it != tplist.end(); it++, i++) {
						tparr[i] = *it;
					}
					tparr[tplist.size()] = NULL;

					char null_pad_str[2*num_null_pads + 1];
					memset(null_pad_str, 0, 2*num_null_pads + 1);
					for (int i = 0; i < num_null_pads; i++) {
						strcat(null_pad_str, "0 ");
					}

//					printf("[%s] %d\n", null_pad_str, (int)strlen(null_pad_str));

					match_query_graph_new(ofstr, tparr, 1, tplist.size(), null_pad_str, best_match_reqd);
//					cout << "Done match_query_graph" << endl;
				}
			}
			
			fclose(ofstr);

//			cout << "slave_cycle? " << slave_cycle << " bottomup_pass? " << bottomup_pass << endl;
			cout << "Best match reqd? " << best_match_reqd << endl;

			//Do best_match only if a slave supernode has a cycle or
			//during build_jvar_tree the pass is not happening in bottomup
			//manner at one of the slave supernodes
			if (best_match_reqd) {
				char cmd[1024];
				sprintf(cmd, "sort -S5G -ru %s > %s_fin", (char *)config[string("TMP_STORAGE")].c_str(), outfile[0]);
				system(cmd);
				unlink((char *)config[string("TMP_STORAGE")].c_str());
				best_match_subsumption(outfile[0]);
			}

			gettimeofday(&stop_time, (struct timezone *)0);
			en = stop_time.tv_sec + (stop_time.tv_usec/MICROSEC);

			st = start_final.tv_sec + (start_final.tv_usec/MICROSEC);
			curr_time = en-st;

			printf("Final res gen time(gettimeofday): %f\n", curr_time);

		}
		
		gettimeofday(&stop_time, (struct timezone *)0);
		en = stop_time.tv_sec + (stop_time.tv_usec/MICROSEC);

		st = start_time.tv_sec + (start_time.tv_usec/MICROSEC);
		curr_time = en-st;
		printf("Total query time(gettimeofday): %f\n", curr_time);
//		printf("Total query time: %f\n", (((double)clock()) - t_start)/CLOCKS_PER_SEC);

#if MMAPFILES
		munmap_all_files();
#endif

	}

	return 0;

}
///////////////////////////////////////
void parse_config_file(char *fname)
{
	FILE *fp;
	char str[300];
	char key[50];
	char value[249];
	bool rawdata = false;

	fp = fopen(fname, "r");
	
	if (fp == NULL) {
		fprintf(stderr, "Could not open config file %s\n", fname);
		exit (-1);
	}

	while(!feof(fp)) {
		memset(str, 0, sizeof(str));
		memset(key, 0, sizeof(key));
		memset(value, 0, sizeof(value));
		if (fgets(str, 300, fp) == NULL)
			break;
		
		//Commented line
		if (index(str, '#') == str) {
			continue;
		}
		
		char *delim = index(str, '=');
		strncpy(key, str, delim - str);
		char *newline = index(str, '\n');
		*newline = '\0';
		strcpy(value, delim + 1);
		config[string(key)]= string(value);
	}

	gnum_subs = atoi(config[string("NUM_SUBJECTS")].c_str());
	gnum_preds = atoi(config[string("NUM_PREDICATES")].c_str());
	gnum_objs = atoi(config[string("NUM_OBJECTS")].c_str());
	gnum_comm_so = atoi(config[string("NUM_COMMON_SO")].c_str());
	table_col_bytes = atoi(config[string("TABLE_COL_BYTES")].c_str());
	row_size_bytes = atoi(config[string("ROW_SIZE_BYTES")].c_str());
	gap_size_bytes = atoi(config[string("GAP_SIZE_BYTES")].c_str());

	comp_folded_arr = atoi(config[string("COMPRESS_FOLDED_ARRAY")].c_str());

	gsubject_bytes = (gnum_subs%8>0 ? gnum_subs/8+1 : gnum_subs/8);
	gpredicate_bytes = (gnum_preds%8>0 ? gnum_preds/8+1 : gnum_preds/8);
	gobject_bytes = (gnum_objs%8>0 ? gnum_objs/8+1 : gnum_objs/8);
	gcommon_so_bytes = (gnum_comm_so%8>0 ? gnum_comm_so/8+1 : gnum_comm_so/8);

}
