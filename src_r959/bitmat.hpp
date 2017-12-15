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

#ifndef _BITMAT_H_
#define _BITMAT_H_

#include <iostream>
#include <vector>
#include <map>
#include <utility>
#include <unordered_set>
#include <stack>
#include <algorithm>
#include <fstream>
#include <list>
#include <set>
#include <cstring>
#include <string>
#include <cmath>
#include <fcntl.h>
#include <stdio.h>
//#include <math.h>
#include <sys/mman.h>
#include <assert.h>

#include <sys/types.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stdbool.h>
#include <time.h>

using namespace std;

#define SUB_DIMENSION 		1
#define PRED_DIMENSION 		2
#define OBJ_DIMENSION 		3
#define SPO_BITMAT			4
#define OPS_BITMAT			5
#define PSO_BITMAT			6
#define POS_BITMAT			7
#define SOP_BITMAT			8
#define OSP_BITMAT			9
#define SO_DIMENSION		10
#define TRIPLE_STR_SPACE    50
#define ROW					10012
#define COLUMN				10013

#define MMAPFILES			1

#define SELECTIVITY_THRESHOLD	128

#define USE_MORE_BYTES		0

#define MAX_TPS_IN_QUERY		32														  
#define MAX_JVARS_IN_QUERY		32
#define	MAX_JVARS			-MAX_JVARS_IN_QUERY
#define MAX_NODES_IN_GRAPH		MAX_JVARS_IN_QUERY + MAX_TPS_IN_QUERY

#define JVAR_NODE			1
#define TP_NODE				2
#define	NON_JVAR_NODE		3

#define	ASSERT_ON			0
#define	REALLOC_ON			0
#define DEBUG				0

#define WHITE				10
#define GREY				11
#define BLACK				12

#define SUB_EDGE			1001
#define PRED_EDGE			1002
#define OBJ_EDGE			1003
#define SO_EDGE				1004

#define NORM_BRACE			1005
#define OPT_BRACE			1006

#define ROW_SIZE_BYTES		4
#define GAP_SIZE_BYTES		4
#define RESBITVEC			0

#define MICROSEC 		1000000.0

extern map <std::string, std::string> config;
extern unsigned int gnum_subs, gnum_preds, gnum_objs, gnum_comm_so, gsubject_bytes, gobject_bytes, gpredicate_bytes, gcommon_so_bytes, grow_size;
extern unsigned int row_size_bytes, gap_size_bytes;
extern unsigned int comp_folded_arr;
extern unsigned int table_col_bytes;
extern unsigned char *grow;

extern unsigned int graph_tp_nodes, graph_tp_nodes_new;
extern unsigned int graph_jvar_nodes, graph_var_nodes;
extern unsigned char **subjmapping, **predmapping, **objmapping;
extern unsigned char *distresult;
extern unsigned long distresult_size;
extern map<int, struct node *> selvars;
extern map<int, struct node *> seljvars;
extern map<string, char *> mmap_table;
//extern set<struct node *> unvisited;
extern bool resbitvec, isCycle;
extern set<string> null_perm;
////////////////////////////////////////////////////////////
// Data structures for multi-join graph

typedef unordered_set<std::string> ResultSet;
extern ResultSet resset;

struct row {
	unsigned int rowid;
	unsigned char *data;
	bool operator<(const struct row &a) const
	{
		return (this->rowid < a.rowid);
	}
};

typedef struct BitMat {
	list<struct row> bm;
	vector<struct row> vbm;
	bool single_row;
	unsigned int num_subs, num_preds, num_objs, num_comm_so;
//	unsigned int row_size_bytes, gap_size_bytes;
	unsigned int subject_bytes, predicate_bytes, object_bytes, common_so_bytes, dimension, last_unfold;
	unsigned long num_triples;
	unsigned char *subfold;
	unsigned char *objfold;
//	unsigned char *subfold_prev;
//	unsigned char *objfold_prev;

	void freebm(void)
	{
		for (std::list<struct row>::iterator it = bm.begin(); it != bm.end(); ){
			free((*it).data);
			it = bm.erase(it);
		}
		if (subfold != NULL) {
			free(subfold);
			subfold = NULL;
		}
		if (objfold != NULL) {
			free(objfold);
			objfold = NULL;
		}
		num_triples = 0;
	}

//	void freerows(void)
//	{
//		for (std::list<struct row>::iterator it = bm.begin(); it != bm.end(); ){
//			free((*it).data);
//			it = bm.erase(it);
//		}
//	}

	void reset(void)
	{
		for (std::list<struct row>::iterator it = bm.begin(); it != bm.end(); ){
			free((*it).data);
			it = bm.erase(it);
		}
		if (subfold != NULL) {
			memset(subfold, 0, subject_bytes);
		}
		if (objfold != NULL) {
			memset(objfold, 0, object_bytes);
		}
		num_triples = 0;
	}

	BitMat()
	{
		num_subs = num_preds = num_objs = num_comm_so = num_triples = 0;
		subfold = objfold = NULL;
	}

	~BitMat()
	{
		for (std::list<struct row>::iterator it = bm.begin(); it != bm.end(); ){
			free((*it).data);
			it = bm.erase(it);
		}
		if (subfold != NULL)
			free(subfold);
		if (objfold != NULL)
			free(objfold);
//		free(subfold_prev);
//		free(objfold_prev);
	}

} BitMat;

extern BitMat bmorig;

struct triple {
	unsigned int sub;
	unsigned int pred;
	unsigned int obj;
};

struct twople {
	unsigned int sub;
	unsigned int obj;
};

struct level {
	int level;
	char ch;
};

typedef struct blevel {
	unsigned char *joinres;
	unsigned int joinres_size;
	int joinres_dim;
	bool empty;
//	int m_level, s_level;
//	stack<struct strlevel> level;
	string strlevel;
} BitVecJoinres;

typedef struct linkedlist {
	struct node *gnode;
	int edgetype;
	struct linkedlist *next;
} LIST;

struct node {
	int type;
	int level;
	//following 2 vars are for cycle detection algo
	int color;
	bool isNeeded;
	void *data;
	LIST *nextadjvar;
	unsigned int numadjvars;
	unsigned int numadjjvars;
	LIST *nextTP;
	unsigned int numTPs;
};

typedef struct triplepattern {
	int nodenum; // unique in the graph
	int sub;
	int pred;
	int obj;
	deque<struct level> level;
	deque<string> pseudomaster;
	deque<string> real_masters;
	string parent;
	string strlevel;
	string old_level;
	BitMat bitmat;
	BitMat bitmat2;
	int unfolded;
	bool rollback;
	int numpass;
	unsigned int triplecnt;
	unsigned short numjvars;
	unsigned short numvars;
	struct node *gtp;

	bool containsJvar(int jvar) {
		if (sub > MAX_JVARS && sub == jvar)
			return true;
		if (obj > MAX_JVARS &&  obj == jvar)
			return true;
		if (pred > MAX_JVARS && pred == jvar)
			return true;
		return false;
	}

	string deque_to_string()
	{
		strlevel.clear();
		for (deque<struct level>::iterator it = this->level.begin(); it != this->level.end(); it++) {
			char str[20];
			sprintf(str, "%c%d", (*it).ch, (*it).level);
			strlevel.append(str);
		}
		return strlevel;
	}
	bool isSlave(string that) {
		if (this->strlevel.find(that) == 0) {
			return true;
		}
		for (deque<string>::iterator it = this->pseudomaster.begin(); it != this->pseudomaster.end(); it++) {
//			if (that.find(*it) == 0) 
			if ((*it).find(that) == 0) {
				//that is a slave of one of the psuedomasters
				//TODO: Needs a check in the logic, psuedomaster should be a slave of that
				//and not the other way round
				return true;
			}
		}
		return false;
	}
	bool isSlaveOfAnyone(set<string> thatvec)
	{
		for (set<string>::iterator it = thatvec.begin(); it != thatvec.end(); it++) {
			if (this->isSlave(*it))
				return true;
		}
		return false;
	}
	bool isSlaveOld(string that)
	{
		if (this->old_level.find(that) == 0) {
			return true;
		}
		for (deque<string>::iterator it = this->pseudomaster.begin(); it != this->pseudomaster.end(); it++) {
			if (that.find(*it) == 0) {
				//that is a slave of one of the psuedomasters
				return true;
			}
		}
		return false;
	}
	void printPseudomasters(void)
	{
		if (this->pseudomaster.size() == 0)
			return;
		cout << " Psuedomasters";
		for (deque<string>::iterator it = this->pseudomaster.begin(); it != this->pseudomaster.end(); it++) {
			cout << " [" << *it << "]";
		}
	}
	void printRealMasters(void)
	{
		if (this->real_masters.size() == 0)
			return;
		cout << " RealMasters";
		for (deque<string>::iterator it = this->real_masters.begin(); it != this->real_masters.end(); it++) {
			cout << " [" << *it << "]";
		}
	}
//	void print(void)
//	{
//		cout << "[" << this->sub << " " << this->pred << " " << this->obj << "] " << this->strlevel << " Parent " << parent << " ";
//	}
	std::string toString()
	{
		char str[1024]; sprintf(str, "[%d %d %d] %s Parent %s bitmat.dim %d bitmat2.dim %d", this->sub, this->pred, this->obj,
							this->strlevel.c_str(), this->parent.c_str(), this->bitmat.dimension, this->bitmat2.dimension);
		return string(str);
	}

	std::string plainString()
	{
		char str[1024]; sprintf(str, "[%d %d %d]", this->sub, this->pred, this->obj);
		return string(str);
	}

} TP;

//extern map<struct node*, struct triple> q_to_gr_node_map;  // Maps query node to the graph triple.

typedef struct var {
	int nodenum;
	unsigned int val;
	struct node *gjvar;
//	bool sojoin;
	//this is for s-o join
	//in case of s-o join if S dimension gets 
	//evaluated first, then joinres points to
	//subject array o/w to compressed obj array
	//joinres_so_dim indicates the dimension of joinres_so
//	unsigned char *joinres;
//	int joinres_dim;
//	unsigned char *joinres_so;
	vector<BitVecJoinres*> joinres;
//	int joinres_so_dim;
//	unsigned int joinres_size;
//	unsigned int joinres_so_size;
	unsigned int upperlim, lowerlim, range;
} JVAR;

extern struct node graph[MAX_NODES_IN_GRAPH];
extern struct node *jvarsitr2[MAX_JVARS_IN_QUERY];
extern map<struct node*, vector <struct node*> > tree_edges_map;  // for tree edges in the query graph.

void parse_config_file(char *fname);

void init_bitmat(BitMat *bitmat, unsigned int snum, unsigned int pnum, unsigned int onum, unsigned int commsonum, int dimension);

void shallow_init_bitmat(BitMat *bitmat, unsigned int snum, unsigned int pnum, unsigned int onum, unsigned int commsonum, int dimension);

unsigned int dgap_compress(unsigned char *in, unsigned int size, unsigned char *out);

unsigned int dgap_compress_new(unsigned char *in, unsigned int size, unsigned char *out);

unsigned long count_bits_in_row(unsigned char *in, unsigned int size);

void dgap_uncompress(unsigned char *in, unsigned int insize, unsigned char *out, unsigned int outsize);

void cumulative_dgap(unsigned char *in, unsigned int size, unsigned char *out);

void de_cumulative_dgap(unsigned char *in, unsigned int size, unsigned char *out);

void bitmat_cumulative_dgap(unsigned char **bitmat);

void bitmat_de_cumulative_dgap(unsigned char **bitmat);

unsigned int dgap_AND(unsigned char *in1, unsigned int size1,
					unsigned char *in2, unsigned int size2, 
					unsigned char *res);

unsigned int concatenate_comp_arr(unsigned char *in1, unsigned int size1,
								unsigned char *in2, unsigned int size2);

void map_to_row_wo_dgap(unsigned char **in, unsigned int pbit, unsigned int obit,
				unsigned int spos, bool cflag, bool start);

void map_to_row_wo_dgap_ops(unsigned char **in, unsigned int pbit, unsigned int obit,
				unsigned int spos, bool cflag, bool start);

unsigned long count_triples_in_row(unsigned char *in, unsigned int size);

unsigned int fixed_p_fixed_o (unsigned char *in, unsigned int size, unsigned char *out,
								unsigned int ppos, unsigned int opos);

unsigned int fixed_p_var_o(unsigned char *in, unsigned int size, unsigned char *out,
							unsigned int ppos);

unsigned int var_p_fixed_o(unsigned char *in, unsigned int size, unsigned char *out,
							unsigned int opos, unsigned char *and_array, unsigned int and_array_size);

unsigned char **filter(unsigned char **ts, unsigned int sub, unsigned int pred, unsigned int obj);

unsigned long count_triples_in_bitmat(BitMat *bitmat);

unsigned int fold(unsigned char **ts, int ret_dimension, unsigned char *foldarr);

void simple_fold(BitMat *bitmat, int ret_dimension, unsigned char *foldarr, unsigned int size);

void unfold(BitMat *bitmat, unsigned char *maskarr, unsigned int maskarr_size, int ret_dimension);

void simple_unfold(BitMat *bitmat, unsigned char *maskarr, unsigned int maskarr_size, int ret_dimension, int numpass);

unsigned int count_size_of_bitmat(unsigned char **bitmat);
unsigned long get_size_of_bitmat(int dimension, unsigned int node);

void list_triples_in_bitmat(unsigned char **bitmat, unsigned int dimension, unsigned long num_triples, char *triplefile);

void go_over_triples_in_row(unsigned char *in, unsigned int rownum, struct node *n,
							int curr_node, struct node *parent, int in_dimension, ofstream& outfile);

//void match_query(struct node* n, int curr_node, struct node *parent, ofstream& outfile);

unsigned char **load_ops_bitmat(char *fname, BitMat *ops_bm, BitMat *spo_bm);

unsigned long list_enctriples_in_bitmat(unsigned char **bitmat, unsigned int dimension, unsigned int num_triples, char *triplefile);

void init_TP_nodes(TP **tplist, unsigned int listsize);

bool cycle_detect(struct node *node);

void build_graph(TP **tplist, unsigned int tplist_size,
					JVAR **jvarlist, unsigned int jvarlist_size);

void build_graph_new();

void build_graph_and_init(char *fname);

bool parse_query_and_build_graph(char *fname);

bool do_fold_unfold_ops(struct node *gnode, unsigned int gnode_idx, unsigned int loopcnt);

void multi_join(char *fname);

void load_data(char *file);

unsigned int load_from_dump_file(char *fname_dump, unsigned int bmnum, BitMat *bitmat,
		bool readtcnt, bool readarray, unsigned char *maskarr, unsigned int mask_size, int maskarr_dim,
		char *fpos, int fd, bool fold_objdim);

void spanning_tree(struct node *n, int curr_node, int nodetype);
vector<struct node *> spanning_tree_tpnodes();
//vector<struct node *> final_res_gen_tp_seq();

void spanning_tree_bfs(struct node *n, int nodetype);

struct node *optimize_and_get_start_node();

struct node *get_start_node_subgraph_matching();

void print_mem_usage();

void clear_rows(BitMat *bitmat, bool clearbmrows, bool clearfoldarr, bool optimize);

void load_data_vertically(char *file, vector<struct twople> &triplelist, BitMat *bitmat, char *fname_dump, bool ondisk, bool invert, bool loadlist, char *tmpfile);

bool add_row(BitMat *bitmat, char *fname, unsigned int bmnum, unsigned int readdim, unsigned int rownum, bool load_objfold);

bool filter_and_load_bitmat(BitMat *bitmat, int fd, char *fpos, unsigned char *and_array, unsigned int and_array_size);

bool init_tp_nodes_new(bool bushy);

unsigned int get_and_array(BitMat *bitmat, unsigned char *and_array, unsigned int bit);

unsigned long get_offset(char *fname, unsigned int bmnum);
	
void print_node_info(struct node *n);

void print_spanning_tree(int nodetype);

void build_jvar_tree(bool best_match_reqd, bool iscylic, bool allslaves_with_one_jvar);

bool prune_triples_new(bool bushy, bool opt, vector<vector<struct node *>> &cycles,
						bool best_match_reqd, bool allslaves_one_jvar);

bool populate_all_tp_bitmats();

//void match_query_graph(FILE *outfile, struct node **tparr, int, int, set<string> &null_tmp, bool opt);

void match_query_graph_new(FILE *outfile, struct node **bfsarr, int sidx, int eidx, char *num_null_pads, bool bestm);

void list_enctrips_bitmat_new(BitMat *bitmat, unsigned int bmnum, vector<twople> &twoplist, FILE *outfile);

void list_enctrips_bitmat2(BitMat *bitmat, unsigned int bmnum, vector<twople> &twoplist, FILE *outfile);

void load_mappings(char *subjfile, char *predfile, char *objfile);

void init_mappings();

void cleanup_graph(void);

void fix_all_tp_bitmats(void);

void fix_all_tp_bitmats2(vector<struct node *> &tplist);

void init_bitmat_rows(BitMat *bitmat, bool initbm, bool initfoldarr);

unsigned long count_size_of_bitmat(BitMat *bitmat);

void list_all_data(int dimension, unsigned int, char *outfile);

void print_graph(void);

bool update_graph(FILE *);

void print_results(FILE *);

bool mmap_all_files();
bool munmap_all_files();

bool update_graph_for_final(void);

unsigned int wrapper_load_from_dump_file(char *fname_dump, unsigned int bmnum, struct node *, bool readtcnt, bool readarray);

void get_all_triples_in_query(char *file);

void identify_true_jvars(void);

void print_stats(char *fname_dump, unsigned int numbms, bool compfold, unsigned int numsubs, unsigned int numobjs);

unsigned int count_intersect(unsigned int bm1, unsigned int bm2);

unsigned int count_intersect_oo(unsigned int bm1, unsigned int bm2);

unsigned int count_intersect_ss(unsigned int bm1, unsigned int bm2);

void get_all_triples_in_query2(char *file);

void test_new_simple_fold();

unsigned int print_set_bits_in_row(unsigned char *in, unsigned int size);

void enumerate_cycles(struct node *jvar, struct node *parent,
			vector<struct node *> &path, set<struct node *> &visited, vector<vector<struct node*>> &cycles);

bool get_longest_cycles(vector<vector<struct node*>> &cycles);

void mark_needed_tps(vector<vector<struct node*>> &cycles);

void test_cycle_detection();

void best_match_subsumption(char *file);

bool is_allslave_bottomup_pass(void);

bool slaves_with_more_than_one_jvar();

#endif
