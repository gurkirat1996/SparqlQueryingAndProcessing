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
#include <queue>

map<struct node*, vector <struct node*> > tree_edges_map;  // for tree edges in the query graph.
vector<struct node *> leaf_nodes;
unsigned int graph_tp_nodes = 0;
unsigned int graph_jvar_nodes;
unsigned int graph_var_nodes;
struct node graph[MAX_NODES_IN_GRAPH];
struct node *jvarsitr2[MAX_JVARS_IN_QUERY];
map<int, struct node *> selvars;
map<int, struct node *> seljvars;
set<string> null_perm;

struct retval {
	struct node *q_node;
	bool atLeastOne;
};

//struct level {
//	int m_level;
//	int s_level;
//
//	bool operator<(struct level &a) const
//	{
//		if (this->m_level < a.m_level) return true;
//		if (this->m_level == a.m_level && this->s_level < a.s_level) return true;
//		return false;
//	}
//};

map<string, vector<struct node *> > levelwise;
map<string, set<struct node *> > leveljvars;
map<struct node *, set<string>> jvarlevels;
map<string, set<struct node *> > true_jvars;
vector<struct node *> gtpnodes;
bool isCycle;

bool object_dim_join(struct node *gnode, struct node *tpnode, bool first);
bool subject_dim_join(struct node *gnode, struct node *tpnode, bool first);
bool predicate_dim_join(struct node *gnode, struct node *tpnode, bool first);
void subject_dim_unfold(JVAR *jvar, TP *tp, bool force, int numpass);
void object_dim_unfold(JVAR *jvar, TP *tp, bool force, int numpass);
void predicate_dim_unfold(JVAR *jvar, TP *tp, bool force, int numpass);
void populate_objfold(BitMat *bitmat);
bool pass_results_across_levels(struct node *jvarnode);
unsigned int get_mask_array(unsigned char *, unsigned int);
vector<struct node *> spanning_tree_jvar(void);
////////////////////////////////////////////////////////////

bool sort_tp_nodes_rev(struct node *a, struct node *b)
{
	if (((TP*)a->data)->bitmat.num_triples >= ((TP*)b->data)->bitmat.num_triples)
		return true;
	return false;
}

bool sort_master_tp_nodes(struct node *a, struct node *b)
{
	if (((TP*)a->data)->bitmat.num_triples <= ((TP*)b->data)->bitmat.num_triples)
		return true;
	return false;
}


bool sort_tp_nodes_prune(struct node *a, struct node *b)
{
//	if (a->m_level < b->m_level) return true;
//	if (a->m_level == b->m_level && a->s_level < b->s_level) return true;
//	return false;

	TP *atp = (TP*)a->data;
	TP *btp = (TP*)b->data;

	string sa = atp->strlevel;
	string sb = btp->strlevel;

	if (sa.find("s") == string::npos && sb.find("s") == string::npos) {
		if (atp->bitmat.num_triples <= btp->bitmat.num_triples)
			return true;
		else
			return false;
	}

	//a is master and b is not
	if (sa.find("s") == string::npos && sb.find("s") <= sb.length()) {
		return true;
	}

	//b is master and a is not
	if (sa.find("s") <= sa.length() && sb.find("s") == string::npos) {
		return false;
	}

	//both at the same level
	if (sa == sb) {
		if (atp->bitmat.num_triples <= btp->bitmat.num_triples)
			return true;
		else
			return false;
	}
	
	//sb is slave of sa
	if (btp->isSlave(sa)) {
		return true;
	}

	//sa is slave of sb
	if (atp->isSlave(sb))
		return false;

	//Mutually exclusive slaves
	if (atp->bitmat.num_triples <= btp->bitmat.num_triples)
		return true;
	else
		return false;
}
//Sort with TP with lower triplecount and the one who's master
//ahead of the TP with higher triplecount and one who's a slave
bool sort_tpnodes_jvar_tree(struct node *a, struct node *b)
{
	string sa = ((TP*)a->data)->strlevel;
	string sb = ((TP*)b->data)->strlevel;

	if (sa == sb) {
		return (((TP*)a->data)->bitmat.num_triples <= ((TP*)b->data)->bitmat.num_triples);
	}
	if (((TP*)a->data)->isSlave(sb) && !((TP*)b->data)->isSlave(sa))
		return false;
	if (((TP*)b->data)->isSlave(sa) && !((TP*)a->data)->isSlave(sb))
		return true;
	if (!((TP*)a->data)->isSlave(sb) && !((TP*)b->data)->isSlave(sa)) {
		if (sa.find("s") != string::npos && sb.find("s") == string::npos) {
			return false;
		}
		if (sa.find("s") == string::npos && sb.find("s") != string::npos) {
			return true;
		}
		if (sa.size() > sb.size())
			return false;
		if (sa.size() < sb.size())
			return true;

		return (((TP*)a->data)->bitmat.num_triples <= ((TP*)b->data)->bitmat.num_triples);
	}

	return true;
}

bool sort_tpgr_nodes(struct node *a, struct node *b)
{
	string sa = ((TP*)a->data)->strlevel;
	string sb = ((TP*)b->data)->strlevel;

//	if (((TP*)a->data)->m_level < ((TP*)b->data)->m_level) return true;
//	if (((TP*)a->data)->m_level == ((TP*)b->data)->m_level && ((TP*)a->data)->s_level < ((TP*)b->data)->s_level) return true;
	if (sa.compare(sb) == 0) {
		return (((TP*)a->data)->bitmat.num_triples < ((TP*)b->data)->bitmat.num_triples);
	}
	return (sa < sb);
}
///////////////////////////////////////////////////////////
bool sort_triples_inverted(struct twople tp1, struct twople tp2)
{
	if (tp1.obj < tp2.obj)
		return true;
	else if (tp1.obj == tp2.obj)
		return (tp1.sub < tp2.sub);
	return false;

}

///////////////////////////////////////////////////////////
//void build_graph(TP **tplist, unsigned int tplist_size,
//					JVAR **jvarlist, unsigned int jvarlist_size)
//{
//	int i, j, k;
//	TP *tp;
//	JVAR *jvar;
//	LIST *next = NULL;
//	LIST *nextvar = NULL;
//
////	printf("build_graph\n");
////	fflush(stdout);
//
//	for (i = 0; i < tplist_size; i++) {
//		tp = tplist[i];
//		graph[MAX_JVARS_IN_QUERY + i].type = TP_NODE;
//		graph[MAX_JVARS_IN_QUERY + i].data = tp;
//		//this is always null for TP nodes
//		//as they are not conn to each other
//		graph[MAX_JVARS_IN_QUERY + i].nextTP = NULL;
//		graph[MAX_JVARS_IN_QUERY + i].numTPs = 0;
//		graph[MAX_JVARS_IN_QUERY + i].nextadjvar = NULL;
//		graph[MAX_JVARS_IN_QUERY + i].numadjvars = 0;
//	}
//
//	for (i = 0; i < jvarlist_size; i++) {
//		jvar = jvarlist[i];
//		graph[i].type = JVAR_NODE;
//		graph[i].data = jvar;
//		graph[i].nextTP = NULL;
//		graph[i].numTPs = 0;
//		graph[i].nextadjvar = NULL;
//		graph[i].numadjvars = 0;
//	}
//
//	//build graph's adjacency list representation
//	//this loop only creates TP -> var and var -> TP conn
//	// next loop creates var <-> var connections
//
//	// go over TP nodes
//	for (i = 0; i < graph_tp_nodes; i++) {
//		if (ASSERT_ON)
//			assert(graph[MAX_JVARS_IN_QUERY + i].type == TP_NODE);
//		tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
//
////		printf("build_graph: [%d %d %d]\n", tp->sub, tp->pred, tp->obj);
//		//connect this with the join var in
//		//jvarlist
//
//		//TODO: take care for ?s :p1 ?s case
//		/////////////////////////////////////////////
//		// go over var nodes
//		for (j = 0; j < graph_jvar_nodes; j++) {
//
//			if (ASSERT_ON)
//				assert(graph[j].type == JVAR_NODE);
//
//			jvar = (JVAR *)graph[j].data;
//
////			printf("jvar nodenum %d\n", jvar->nodenum);
//
//			if (tp->sub == jvar->nodenum || tp->pred == jvar->nodenum || tp->obj == jvar->nodenum) {
////				printf("Found a match\n");
//
//				//add tp -> var edge
//				if (graph[MAX_JVARS_IN_QUERY + i].numadjvars == 0) {
//					graph[MAX_JVARS_IN_QUERY + i].nextadjvar = (LIST *) malloc (sizeof(LIST));
////					graph[MAX_JVARS_IN_QUERY + i].nextadjvar->data = jvar;
//					graph[MAX_JVARS_IN_QUERY + i].nextadjvar->gnode = &graph[j];
//				} else {
//					next = graph[MAX_JVARS_IN_QUERY + i].nextadjvar;
//					for (k = 0; k < graph[MAX_JVARS_IN_QUERY + i].numadjvars - 1; k++) {
//						next = next->next;
//					}
//					next->next = (LIST *) malloc (sizeof(LIST));
////					next->next->data = jvar;
//					next->next->gnode = &graph[j];
//				}
//				graph[MAX_JVARS_IN_QUERY + i].numadjvars++;
//
//				//add var -> tp edge
//				if (graph[j].numTPs == 0) {
//					graph[j].nextTP = (LIST *) malloc (sizeof(LIST));
////					graph[j].nextTP->data = tp;
//					graph[j].nextTP->gnode = &graph[MAX_JVARS_IN_QUERY + i];
//				} else  {
//					next = graph[j].nextTP;
//					for (k = 0; k < graph[j].numTPs - 1; k++) {
//						next = next->next;
//					}
//					next->next = (LIST *) malloc (sizeof(LIST));
////					next->next->data = tp;
//					next->next->gnode = &graph[MAX_JVARS_IN_QUERY + i];
//				}
//				graph[j].numTPs++;
//
//			}
//		}
//		//////////////////////////////////////////////////////////////
//	}
//
//	//code to add var -> var edge if they appear in the same TP
//
//	for (j = 0; j < graph_jvar_nodes; j++) {
//
//		jvar = (JVAR*)graph[j].data;
//
//		if (ASSERT_ON)
//			assert(graph[j].numTPs > 0);
//
//		int numTPs = graph[j].numTPs;
//		next = graph[j].nextTP;
//
//		while (numTPs) {
//			tp = (TP *)next->gnode->data;
//			int numvars = graph[tp->nodenum].numadjvars;
//			nextvar = graph[tp->nodenum].nextadjvar;
//			if (numvars != 1) {
//				while(numvars > 0) {
//					struct node *gnode2 = nextvar->gnode;
//					JVAR *jvar2 = (JVAR *)gnode2->data;
//					if (jvar->nodenum != jvar2->nodenum) {
//						if (graph[j].numadjvars == 0) {
//							graph[j].nextadjvar = (LIST *) malloc (sizeof(LIST));
////							graph[j].nextadjvar->data = jvar2;
//							graph[j].nextadjvar->gnode = gnode2;
//						} else {
//							LIST *varitr = graph[j].nextadjvar;
//							for (k = 0; k < graph[j].numadjvars - 1; k++) {
//								varitr = varitr->next;
//							}
//							varitr->next = (LIST *) malloc (sizeof(LIST));
////							varitr->next->data = jvar2;
//							varitr->next->gnode = gnode2;
//						}
//						graph[j].numadjvars++;
//
//					}
//					nextvar = nextvar->next;
//					numvars--;
//				}
//
//			}
//			//go on to next TP to which this jvar is conn
//			next = next->next;
//			numTPs--;
//		}
//
//
//	}
//
//	//add TP <-> TP edges if they share a join var
//	//TODO: might need a fix later for 2 triples sharing more than
//	//one join variable.. it's not commonly observed.. but needs a fix
//
//	for (i = 0; i < graph_tp_nodes; i++) {
//		tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
//
////		printf("build_graph: %d %d %d\n", tp->sub, tp->pred, tp->obj);
//
////		next = NULL;
//
//		for (j = i + 1; j < graph_tp_nodes; j++) {
//			TP *tp2 = (TP *)graph[MAX_JVARS_IN_QUERY + j].data;
////			printf("build_graph: for loop %d %d %d\n", tp2->sub, tp2->pred, tp2->obj);
//
//			int numTPs = graph[MAX_JVARS_IN_QUERY + i].numTPs;
////			printf("numTPs %d\n", numTPs);
//
//			//two triples share the same subject
//			if (tp->sub < 0 && tp->sub == tp2->sub) {
////				printf("build_graph: found match subject\n");
//				if (numTPs == 0) {
//					graph[MAX_JVARS_IN_QUERY + i].nextTP = (LIST *) malloc (sizeof(LIST));
//					next = graph[MAX_JVARS_IN_QUERY + i].nextTP;
//				} else {
//					next = graph[MAX_JVARS_IN_QUERY + i].nextTP;
//					for (k = 0; k < numTPs - 1; k++) {
//						next = next->next;
//					}
//					next->next = (LIST *) malloc (sizeof(LIST));
//					next = next->next;
//				}
//				next->gnode = &graph[MAX_JVARS_IN_QUERY + j];
//				next->edgetype = SUB_EDGE;
//				graph[MAX_JVARS_IN_QUERY + i].numTPs++;
//
//				//Add other way edge too
//				numTPs = graph[MAX_JVARS_IN_QUERY + j].numTPs;
//				if (numTPs == 0) {
//					graph[MAX_JVARS_IN_QUERY + j].nextTP = (LIST *) malloc (sizeof(LIST));
//					next = graph[MAX_JVARS_IN_QUERY + j].nextTP;
//				} else {
//					next = graph[MAX_JVARS_IN_QUERY + j].nextTP;
//					for (k = 0; k < numTPs - 1; k++) {
//						next = next->next;
//					}
//					next->next = (LIST *) malloc (sizeof(LIST));
//					next = next->next;
//				}
//				next->gnode = &graph[MAX_JVARS_IN_QUERY + i];
//				next->edgetype = SUB_EDGE;
//				graph[MAX_JVARS_IN_QUERY + j].numTPs++;
//
//				continue;
//			}
//
//			//two triples share the same predicate
//			if (tp->pred < 0 && tp->pred == tp2->pred) {
////				printf("build_graph: found match predicate\n");
//				if (numTPs == 0) {
//					graph[MAX_JVARS_IN_QUERY + i].nextTP = (LIST *) malloc (sizeof(LIST));
//					next = graph[MAX_JVARS_IN_QUERY + i].nextTP;
//				} else {
//					next = graph[MAX_JVARS_IN_QUERY + i].nextTP;
//					for (k = 0; k < numTPs - 1; k++) {
//						next = next->next;
//					}
//					next->next = (LIST *) malloc (sizeof(LIST));
//					next = next->next;
//				}
//				next->gnode = &graph[MAX_JVARS_IN_QUERY + j];
//				next->edgetype = PRED_EDGE;
//
//				graph[MAX_JVARS_IN_QUERY + i].numTPs++;
//
//				//Add other way edge too
//				numTPs = graph[MAX_JVARS_IN_QUERY + j].numTPs;
//				if (numTPs == 0) {
//					graph[MAX_JVARS_IN_QUERY + j].nextTP = (LIST *) malloc (sizeof(LIST));
//					next = graph[MAX_JVARS_IN_QUERY + j].nextTP;
//				} else {
//					next = graph[MAX_JVARS_IN_QUERY + j].nextTP;
//					for (k = 0; k < numTPs - 1; k++) {
//						next = next->next;
//					}
//					next->next = (LIST *) malloc (sizeof(LIST));
//					next = next->next;
//				}
//				next->gnode = &graph[MAX_JVARS_IN_QUERY + i];
//				next->edgetype = PRED_EDGE;
//				graph[MAX_JVARS_IN_QUERY + j].numTPs++;
//
//				continue;
//			}
//
//			//two triples share the same object
//			if (tp->obj < 0 && tp->obj == tp2->obj) {
////				printf("build_graph: found match objects\n");
//				if (numTPs == 0) {
//					graph[MAX_JVARS_IN_QUERY + i].nextTP = (LIST *) malloc (sizeof(LIST));
//					next = graph[MAX_JVARS_IN_QUERY + i].nextTP;
//				} else {
//					next = graph[MAX_JVARS_IN_QUERY + i].nextTP;
//					for (k = 0; k < numTPs - 1; k++) {
//						next = next->next;
//					}
//					next->next = (LIST *) malloc (sizeof(LIST));
//					next = next->next;
//				}
//				next->gnode = &graph[MAX_JVARS_IN_QUERY + j];
//				next->edgetype = OBJ_EDGE;
//
//				graph[MAX_JVARS_IN_QUERY + i].numTPs++;
//				//Add other way edge too
//				numTPs = graph[MAX_JVARS_IN_QUERY + j].numTPs;
//				if (numTPs == 0) {
//					graph[MAX_JVARS_IN_QUERY + j].nextTP = (LIST *) malloc (sizeof(LIST));
//					next = graph[MAX_JVARS_IN_QUERY + j].nextTP;
//				} else {
//					next = graph[MAX_JVARS_IN_QUERY + j].nextTP;
//					for (k = 0; k < numTPs - 1; k++) {
//						next = next->next;
//					}
//					next->next = (LIST *) malloc (sizeof(LIST));
//					next = next->next;
//				}
//				next->gnode = &graph[MAX_JVARS_IN_QUERY + i];
//				next->edgetype = OBJ_EDGE;
//				graph[MAX_JVARS_IN_QUERY + j].numTPs++;
//
//				continue;
//			}
//
//			//two triples share the same subject and object
//			if ((tp->sub < 0 && tp->sub == tp2->obj) || (tp->obj < 0 && tp->obj == tp2->sub)) {
////				printf("build_graph: found match SO\n");
//				if (numTPs == 0) {
//					graph[MAX_JVARS_IN_QUERY + i].nextTP = (LIST *) malloc (sizeof(LIST));
//					next = graph[MAX_JVARS_IN_QUERY + i].nextTP;
//				} else {
//					next = graph[MAX_JVARS_IN_QUERY + i].nextTP;
//					for (k = 0; k < numTPs - 1; k++) {
//						next = next->next;
//					}
//					next->next = (LIST *) malloc (sizeof(LIST));
//					next = next->next;
//				}
//				next->gnode = &graph[MAX_JVARS_IN_QUERY + j];
//				next->edgetype = SO_EDGE;
//
//				graph[MAX_JVARS_IN_QUERY + i].numTPs++;
//				//Add other way edge too
//				numTPs = graph[MAX_JVARS_IN_QUERY + j].numTPs;
//				if (numTPs == 0) {
//					graph[MAX_JVARS_IN_QUERY + j].nextTP = (LIST *) malloc (sizeof(LIST));
//					next = graph[MAX_JVARS_IN_QUERY + j].nextTP;
//				} else {
//					next = graph[MAX_JVARS_IN_QUERY + j].nextTP;
//					for (k = 0; k < numTPs - 1; k++) {
//						next = next->next;
//					}
//					next->next = (LIST *) malloc (sizeof(LIST));
//					next = next->next;
//				}
//				next->gnode = &graph[MAX_JVARS_IN_QUERY + i];
//				next->edgetype = SO_EDGE;
//				graph[MAX_JVARS_IN_QUERY + j].numTPs++;
//
//			}
//		}
//
//	}
//
////	printf("Done\n");
//
//
//	//for cycle detection
////	for (i = 0; i < graph_jvar_nodes; i++) {
////		graph[i].color = WHITE;
////		graph[i].parent = NULL;
////	}
////
////	printf("Cycle exists? %d\n", cycle_detect(&graph[0]));
///*
//	///////////////////
//	//DEBUG
//	///////////////////
//
//	printf("build_graph: GRAPH IS:\n");
//	for (i = 0; i < graph_tp_nodes; i++) {
//		tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
//		printf("[%d %d %d]\n", tp->sub, tp->pred, tp->obj);
//
//	}
//
//	for (i = 0; i < graph_jvar_nodes; i++) {
//		jvar = (JVAR *)graph[i].data;
//		printf("%d\n", jvar->nodenum);
//
//	}
//
//	///////////////////
//*/
//	
///*
//	printf("**** build_graph: GRAPH ****\n");
//
//	for (i = 0; i < graph_tp_nodes; i++) {
//		tp = (TP*)graph[MAX_JVARS_IN_QUERY + i].data;
//		printf("nodenum %d [%d %d %d] -> ", tp->nodenum, tp->sub, tp->pred, tp->obj);
//
//		assert(graph[MAX_JVARS_IN_QUERY + i].numadjvars > 0);
//
//		next = graph[MAX_JVARS_IN_QUERY + i].nextadjvar;
//
//		jvar = (JVAR*)next->data;
//		assert(jvar != NULL);
//		printf("[%d] ", jvar->nodenum);
//
//		for (j = 0; j < graph[MAX_JVARS_IN_QUERY + i].numadjvars-1; j++) {
//			next = next->next;
//			assert(next != NULL);
//			jvar = (JVAR*)next->data;
//			printf("[%d] ", jvar->nodenum);
//		}
//		printf("\n");
//	}
//
//	for (i = 0; i < graph_jvar_nodes; i++) {
//		jvar = (JVAR *)graph[i].data;
//		assert(jvar != NULL);
//		assert(graph[i].type == JVAR_NODE);
//
//		printf("nodenum %d -> ", jvar->nodenum);
//
//		assert(graph[i].numTPs > 0);
//
////		next = graph[i].nextTP;
////		assert(next != NULL);
////		tp = (TP *)next->data;
//
////		assert(tp != NULL);
//
////		printf("[%d %d %d] ", tp->sub, tp->pred, tp->obj);
//
//		for (j = 0; j < graph[i].numTPs; j++) {
//			if (j == 0) {
//				next = graph[i].nextTP;
//			} else {
//				next = next->next;
//			}
//			tp = (TP *)next->data;
//			printf("[%d %d %d] ", tp->sub, tp->pred, tp->obj);
//
//		}
//
//		printf("\n");
//	}
//
//	for (i = 0; i < graph_jvar_nodes; i++) {
//		jvar = (JVAR *)graph[i].data;
//		assert(jvar != NULL);
//		assert(graph[i].type == JVAR_NODE);
//
//		printf("nodenum %d -> ", jvar->nodenum);
//
//		assert(graph[i].numTPs > 0);
//
//		for (j = 0; j < graph[i].numadjvars; j++) {
//			if (j == 0) {
//				next = graph[i].nextadjvar;
//			} else {
//				next = next->next;
//			}
////			JVAR *jvar2 = (JVAR *)next->data;
//			JVAR *jvar2 = (JVAR *)next->gnode->data;
//			printf("[%d] ", jvar2->nodenum);
//
//		}
//
//		printf("\n");
//	}
//*/
//
//	for (i = 0; i < graph_tp_nodes; i++) {
//		tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
//
//		printf("[%d %d %d] -> ", tp->sub, tp->pred, tp->obj);
//
//		for (j = 0; j < graph[MAX_JVARS_IN_QUERY + i].numTPs; j++) {
//			if (j == 0) {
//				next = graph[MAX_JVARS_IN_QUERY + i].nextTP;
//			} else {
//				next = next->next;
//			}
//
//			TP *tp2 = (TP *)next->gnode->data;
//			printf("[(%d %d %d), %d] ", tp2->sub, tp2->pred, tp2->obj, next->edgetype);
//		}
//
//		printf("\n");
//	}
//
//	printf("build_graph exiting\n");
//
//}
////////////////////////////////////////////////////////////

string longest_common_prefix(string a, string b) 
{
	int alen = a.length(); int blen = b.length();
	int min = alen < blen ? alen:blen;
	string prefix;
	
	for (int i = 0; i < min; i += 2) {
		if (a[i] == b[i] && a[i+1] == b[i+1]) {
			prefix.append(1, a[i]); prefix.append(1, a[i+1]);
		} else {
			break;
		}
	}
	return prefix;
}

//////////////////////////////////////////////////////
string do_rollback(struct node *gnode, bool opt)
{
	TP *tp1 = (TP*)gnode->data;

	if (!opt || tp1->strlevel.find("s") == string::npos) {
		return string();
	}

	LIST *next = gnode->nextTP;
	LIST *prev = NULL;

	while (next != NULL) {
		struct node *neighb = next->gnode;
		TP *tp2 = (TP*)next->gnode->data;
		if (tp1->isSlave(tp2->strlevel) || tp2->isSlave(tp1->strlevel)) {
			prev = next;
			next = next->next;
			continue;
		}
		int jvar = -12345;
		if (tp1->sub == tp2->sub || tp1->sub == tp2->obj) {
			jvar = tp1->sub;
		} else if (tp1->obj == tp2->sub || tp1->obj == tp2->obj) {
			jvar = tp1->obj;
		} else if (tp1->pred == tp2->pred) {
			jvar = tp1->pred;
		}
		string lowestMaster1(1, 'a');
		string lowestMaster2(1, 'a');
		for (map<string, vector<struct node *> >::iterator tit = levelwise.begin(); tit != levelwise.end(); tit++) {
			string level = (*tit).first;
			vector<struct node *> tpvec = (*tit).second;
			for (vector<struct node *>::iterator it = tpvec.begin(); it != tpvec.end(); it++) {
//				cout << "Level " << level << " jvar " << jvar << " " << tp1->strlevel << " " << tp2->strlevel << endl;
//				cout << "isslave? " << tp1->isSlave(level) << endl;
				if (tp1->strlevel.compare(level) != 0 && tp1->isSlave(level)) {
//					cout << "TP1 here" << endl;
					TP *tp = (TP *)(*it)->data;
					if (tp->sub == jvar || tp->obj == jvar || tp->pred == jvar) {
						if (lowestMaster1.size() < level.size()) {
//							cout << "TP1 here2" << endl;
							lowestMaster1 = level;
						}
					}
				}
				if (tp2->strlevel.compare(level) != 0 && tp2->isSlave(level)) {
//					cout << "TP2 here" << endl;
					TP *tp = (TP *)(*it)->data;
					if (tp->sub == jvar || tp->obj == jvar || tp->pred == jvar) {
						if (lowestMaster2.size() < level.size()) {
//							cout << "TP2 here2" << endl;
							lowestMaster2 = level;
						}
					}
				}
			}
		}
//		cout << "do_rollback: lowestM1 " << lowestMaster1 << " lowestM2 " << lowestMaster2 << endl;
		//Now we have lowest master levels for tp1 and tp2 that have the same
		//variable as shared between tp1 and tp2
//		cout << "******* edge " << tp1->toString() << " " << tp2->toString() << " " << lowestMaster1 << " " << lowestMaster2 << endl;

		if ((lowestMaster1.compare("a") == 0 && lowestMaster2.compare("a") == 0) ||
				lowestMaster1.compare(lowestMaster2) == 0) {
			//keep the edge between the two TPs
			prev = next;
			next = next->next;
			continue;
		}
		//delete the edge
//		cout << "*******Deleting edge " << tp1->toString() << " " << tp2->toString() << endl;
		if (prev) {
			prev->next = next->next;
		} else {
			gnode->nextTP = next->next;
		}
		LIST *tmp = next;
		next = next->next;
		free(tmp);
		gnode->numTPs--;

		//Now delete the other node's conn too
		LIST *nextn = neighb->nextTP;
		LIST *prevn = NULL;
		while (nextn) {
			if (nextn->gnode == gnode) {
				if (prevn) {
					prevn->next = nextn->next;
				} else {
					neighb->nextTP = nextn->next;
				}
				tmp = nextn;
				nextn = nextn->next;
				free(tmp);
				neighb->numTPs--;
			} else {
				prevn = nextn;
				nextn = nextn->next;
			}
		}
	}

	next = gnode->nextTP;
	string prefix(50, 'a');

	while (next) {
		TP *tp2 = (TP*)next->gnode->data;
		//this means that there is an inner-join between two TPs
		//not in master-slave hierarchy and promotion of
		//nodes is required
		if (!tp1->isSlave(tp2->strlevel) && !tp2->isSlave(tp1->strlevel)) {
			string tmp = longest_common_prefix(tp2->strlevel, tp1->strlevel);
//				cout << "prefix=" << tmp << endl;
			if (prefix.size() > tmp.size())
				prefix = tmp;
		}

		next = next->next;
	}

	return prefix;

}

//////////////////////////////////////////////////////////
map<string, string> rollbkmap;

bool parse_query_and_build_graph(char *fname)
{
	FILE *fp;
	int spos = 0, ppos = 0, opos = 0, s_level = 0, m_level = 0;
	JVAR *jvar;
	unsigned int tplist_size = 0, jvarlist_size = 0, varlist_size = 0;
	int i, j;
	bool found = false, opt = false;
	LIST *next;
	
	fp = fopen(fname, "r");
	if (fp == NULL) {
		printf("Error opening file %s\n", fname);
		exit (-1);
	}

	int cnt = 1;

	deque<struct level> lstack, prev_master; //norm_lstack, opt_lstack, *curr_lstack = NULL, lstack;
	int last_closed_brace;
	deque<int> brace_stack;
//	map<string, string> two_opt;

	while (!feof(fp)) {
		char line[1024];
		char s[10], p[10], o[10];

		memset(line, 0, 50);
		if(fgets(line, sizeof(line), fp) != NULL) {
			char *c = NULL, *c2 = NULL;

			char *ptr = index(line, '#');

			if (ptr == line) {
				printf("End of query\n");
				break;
			}

			//parse selection variables
			if (cnt == 1) {
				ptr = strstr(line, "all");
				if (ptr != line) {
					assert(0);
					//parse all selected variables
					c = strchr(line, '\n');
					assert(c != NULL);
					*c = '\0';
					char *prev = line;
					while(1) {
						c = strchr(prev, ',');
						if (c == NULL) {
							//very small query or smth wrong.. skip
							cout << "WARNING just one var remaining "  << line << endl;
							selvars[atoi(prev)] = NULL;
							break;
						} 
						*c = '\0';
						selvars[atoi(prev)] = NULL;
						prev = c+1;
					}
				}
//				struct level l = {1, 'm'};
//				s_level = 0;
//				lstack.push_back(l);
				cnt++;
				continue;
			}

			ptr = strstr(line, "OPTIONAL{");
			if (ptr != NULL) {
				opt = true;
				brace_stack.push_back(OPT_BRACE);

				s_level++;
				struct level l = {s_level, 's'};
				lstack.push_back(l);
				l.level = 1;
				l.ch = 'm';
				lstack.push_back(l);
				cnt++;
				continue;
			}

			ptr = strstr(line, "{");
			if (ptr != NULL) {
				brace_stack.push_back(NORM_BRACE);
				m_level++;
				struct level l = {m_level, 'm'};
				lstack.push_back(l);
				continue;
			}

			ptr = strstr(line, "}");
			if (ptr != NULL) {
				int brace_top = brace_stack.back();
				brace_stack.pop_back();
				if (brace_top == OPT_BRACE) {
					lstack.pop_back(); lstack.pop_back();
				} else if (brace_top == NORM_BRACE) {
					lstack.pop_back();
				} else {
					assert(0);
				}
				cnt++;
				continue;
			}

			if (lstack.size() == 0) {
				assert(0);
			}

			char *strptr = line;
			while (isspace(*strptr) && *strptr != '\0') strptr++;
			if (*strptr == '\0') {
				//Empty line
				continue;
			}

			strcpy(line, strptr);

			c = strchr(line, ':');
			*c = '\0';
			strcpy(s, line);
			c2 = strchr(c+1, ':');
			*c2 = '\0';
			strcpy(p, c+1);
			c = strchr(c2+1, '\n');
			*c = '\0';
			strcpy(o, c2+1);
			spos = atoi(s); ppos = atoi(p); opos = atoi(o);

			cnt++;

			TP *tp = new TP;
//			tp = (TP *) malloc (sizeof (TP));
			tp->sub = spos;
			tp->pred = ppos;
			tp->obj = opos;
			tp->nodenum = tplist_size + MAX_JVARS_IN_QUERY;
			tp->bitmat.bm.clear();
			tp->bitmat2.bm.clear();
			tp->unfolded = 0;
			tp->rollback = false;

			tp->level = lstack;

			tp->numpass = 0;
			tp->numjvars = 0;
			tp->numvars = 0;
			if (tp->sub <= 0) {
				if (tp->sub < 0 && tp->sub > MAX_JVARS)
					tp->numjvars++;
				tp->numvars++;
			}
			if (tp->pred <= 0) {
				if (tp->pred < 0 && tp->pred > MAX_JVARS)
					tp->numjvars++;
				tp->numvars++;
			}
			if (tp->obj <= 0) {
				if (tp->obj < 0 && tp->obj > MAX_JVARS)
					tp->numjvars++;
				tp->numvars++;
			}

			graph[MAX_JVARS_IN_QUERY + tplist_size].type = TP_NODE;
			graph[MAX_JVARS_IN_QUERY + tplist_size].data = tp;
			tp->gtp = &graph[MAX_JVARS_IN_QUERY + tplist_size];
			graph[MAX_JVARS_IN_QUERY + tplist_size].nextTP = NULL;
			graph[MAX_JVARS_IN_QUERY + tplist_size].numTPs = 0;
			graph[MAX_JVARS_IN_QUERY + tplist_size].nextadjvar = NULL;
			graph[MAX_JVARS_IN_QUERY + tplist_size].numadjvars = 0;
			graph[MAX_JVARS_IN_QUERY + tplist_size].isNeeded = false;

			tplist_size++;

			found = false;

			if (spos < 0) {
				// subj join var
				for (i = 0; i < varlist_size; i++) {
					jvar = (JVAR *)graph[i].data;
					if (jvar->nodenum == spos) {
						found = true;
						break;
					}
				}
				if (!found) {
					jvar = new JVAR;
					jvar->nodenum = spos;

					if (spos > MAX_JVARS) {
						graph[varlist_size].type = JVAR_NODE;
						jvarlist_size++;
					}
					else 
						graph[varlist_size].type = NON_JVAR_NODE;
					graph[varlist_size].data = jvar;
					jvar->gjvar = &graph[varlist_size];
					graph[varlist_size].nextTP = NULL;
					graph[varlist_size].numTPs = 0;
					graph[varlist_size].nextadjvar = NULL;
					graph[varlist_size].numadjvars = 0;
					graph[varlist_size].isNeeded = true;

					if (selvars.find(jvar->nodenum) != selvars.end()) {
						selvars[jvar->nodenum] = &graph[varlist_size];
						if (graph[varlist_size].type == JVAR_NODE) {
							seljvars[jvar->nodenum] = &graph[varlist_size];
						}
					}

					varlist_size++;

				} else {
					found = false;
				}

			}

			if (ppos < 0) {
				// pred join var
				for (i = 0; i < varlist_size; i++) {
					jvar = (JVAR *)graph[i].data;
					if (jvar->nodenum == ppos) {
						found = true;
						break;
					}
				}
				if (!found) {
					jvar = new JVAR;
					jvar->nodenum = ppos;

					if (ppos > MAX_JVARS) {
						graph[varlist_size].type = JVAR_NODE;
						jvarlist_size++;
					}
					else
						graph[varlist_size].type = NON_JVAR_NODE;
					graph[varlist_size].data = jvar;
					graph[varlist_size].nextTP = NULL;
					graph[varlist_size].numTPs = 0;
					graph[varlist_size].nextadjvar = NULL;
					graph[varlist_size].numadjvars = 0;
					graph[varlist_size].isNeeded = true;

					if (selvars.find(jvar->nodenum) != selvars.end()) {
						selvars[jvar->nodenum] = &graph[varlist_size];
						if (graph[varlist_size].type == JVAR_NODE) {
							seljvars[jvar->nodenum] = &graph[varlist_size];
						}
					}

					varlist_size++;
				} else {
					found = false;
				}

			}

			if (opos < 0) {
				// obj join var
				for (i = 0; i < varlist_size; i++) {
					jvar = (JVAR *)graph[i].data;
					if (jvar->nodenum == opos) {
						found = true;
						break;
					}
				}
				if (!found) {
					jvar = new JVAR;
					jvar->nodenum = opos;

					if (opos > MAX_JVARS) {
						graph[varlist_size].type = JVAR_NODE;
						jvarlist_size++;
					}
					else
						graph[varlist_size].type = NON_JVAR_NODE;
					graph[varlist_size].data = jvar;
					graph[varlist_size].nextTP = NULL;
					graph[varlist_size].numTPs = 0;
					graph[varlist_size].nextadjvar = NULL;
					graph[varlist_size].numadjvars = 0;
					graph[varlist_size].isNeeded = true;

					if (selvars.find(jvar->nodenum) != selvars.end()) {
						selvars[jvar->nodenum] = &graph[varlist_size];
						if (graph[varlist_size].type == JVAR_NODE) {
							seljvars[jvar->nodenum] = &graph[varlist_size];
						}
					}

					varlist_size++;

				} else {
					found = false;
				}

			}

		}
	}

	if (brace_stack.size() != 0 || lstack.size() != 0) {
		cout << "brace_stack.size " << brace_stack.size() << " lstack.size " << lstack.size() << endl;
		assert(0);
	}

	fclose(fp);

	if (selvars.size() == varlist_size)
		selvars.erase(selvars.begin(), selvars.end());

	graph_tp_nodes = tplist_size;
	graph_jvar_nodes = jvarlist_size;
	graph_var_nodes = varlist_size;

	build_graph_new();

	for (int i = 0; i < graph_tp_nodes; i++) {
		TP *tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
		levelwise[tp->deque_to_string()].push_back(&graph[MAX_JVARS_IN_QUERY + i]);
	}
//	print_graph();

//	cout << "******************************" << endl;

	/*
	 * COMMENT: This code is redundant right now because we are not
	 * focusing on the non-well-designed queries.
	 * Or it can be assumed that non-well-designed queries
	 * are simplified outside the code and fed to the code
	 * in the simplified form.
	 */
//	map<string, string> promote;
//	for (int i = 0; i < graph_tp_nodes; i++) {
//		TP *tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
//		if (tp->strlevel.find("s") == string::npos) {
//			tp->rollback = true;
//			rollbkmap[tp->strlevel] = string();
//			continue;
//		}
////		tp->print();
//		map<string, string>::iterator rbkit = rollbkmap.find(tp->strlevel);
//		map<string, string>::iterator pit = promote.find(tp->strlevel);
//
////		if (rbkit != rollbkmap.end()) {
////			cout << " Found TP in rollbkmap ";
////		} else {
////			cout << " Did not find TP in rollbkmap ";
////		}
////		if (pit != promote.end()) {
////			cout << " Found TP in promote";
////		} else {
////			cout << " Did not find TP in promote";
////		}
////		cout << endl;
//
////		if (rollbkmap.find(tp->strlevel) == rollbkmap.end() && promote.find(tp->strlevel) == promote.end()) {
//			//do_rollback returns the highest level of master
//			//on whom the TP CANNOT rollback
////			tp->print(); cout << endl;
//		bool changed = false;
//		string rollbk = do_rollback(&graph[MAX_JVARS_IN_QUERY + i], opt);
//		if (rbkit == rollbkmap.end() || (rbkit != rollbkmap.end() && rollbk.size() < rbkit->second.size())) {
//			changed = true;
//			rollbkmap[tp->strlevel] = rollbk;
//		}
////		tp->print();  cout << " " << rollbkmap[tp->strlevel] << " "; cout << endl;
//
//		if (!changed) {
//			continue;
//		}
//
//		if (rollbkmap[tp->strlevel].size() == 0) {
//			size_t pos = tp->strlevel.find("s");
//			assert(pos < tp->strlevel.size());
//			string newlvl(tp->strlevel, 0, pos);
////			tp->print();
////			cout << " newlevel.size " << newlvl.size() << endl;
//			if (newlvl.size() != 0) {
//				promote[tp->strlevel] = newlvl;
//				//tp->rollback is true ONLY if this TP has achieved absolute master level
//				tp->rollback = true;
//				for (map<string, vector<struct node *> >::iterator it = levelwise.begin(); it != levelwise.end(); it++) {
//					string level = (*it).first;
//					TP *elem = (TP*)(((*it).second)[0])->data;
//					// if the level is same as 'tp' under consideration or same as the
//					// new-level of tp then continue
//					if (tp->strlevel.compare(level) == 0 || level.compare(promote[tp->strlevel]) == 0)
//						continue;
//
//					if (tp->isSlave(level) && elem->isSlave(promote[tp->strlevel])) {
//						promote[level] = promote[tp->strlevel];
//					}
//				}
//			}
//		} else if (rollbkmap[tp->strlevel].compare(string(50, 'a'))== 0) {
//			//No promotion necessary
//			tp->rollback = false;
//		} else {
//			tp->rollback = false;
//			string prefix = rollbkmap[tp->strlevel];
//
//			TP *highmaster = NULL;
//			for (map<string, vector<struct node *> >::iterator it = levelwise.begin(); it != levelwise.end(); it++) {
//				string slevel = (*it).first;
//				TP *elem = (TP*)(((*it).second)[0])->data;
//				assert(slevel.compare(elem->strlevel) == 0);
//				if (elem->strlevel.compare(tp->strlevel) == 0 || elem->strlevel.compare(prefix) == 0)
//					continue;
//				if (elem->isSlave(prefix) && tp->isSlave(elem->strlevel)) {
//					if (highmaster == NULL ||
//						(highmaster->strlevel.compare(elem->strlevel) !=0 && highmaster->isSlave(elem->strlevel))) {
//						highmaster = elem;
//					}
//				}
//			}
//			if (highmaster != NULL) {
//				promote[tp->strlevel] = highmaster->strlevel;
//				//Promote intermediate masters
//				for (map<string, vector<struct node *> >::iterator it = levelwise.begin(); it != levelwise.end(); it++) {
//					string level = (*it).first;
//					TP *elem = (TP*)(((*it).second)[0])->data;
//					// if the level is same as 'tp' under consideration or same as the
//					// new-level of tp then continue
//					if (tp->strlevel.compare(level) == 0 || level.compare(promote[tp->strlevel]) == 0)
//						continue;
//
//					if (tp->isSlave(level) && elem->isSlave(promote[tp->strlevel])) {
//						promote[level] = promote[tp->strlevel];
//					}
//				}
//			}
//
//		}
////		}
//
//	}
//
//	for (map<string, vector<struct node *> >::iterator it = levelwise.begin(); it != levelwise.end(); it++) {
//		string level = (*it).first;
//		if (promote.find(level) == promote.end()) {
//			promote[level] = level;
//		}
//	}
//
//	//Sanity check -- to ensure that all intermediate masters got promoted correctly as well
//	for (map<string, vector<struct node *> >::iterator it = levelwise.begin(); it != levelwise.end(); it++) {
//		string level = (*it).first;
//		if (promote[level].compare(level) != 0) {
//			for (map<string, vector<struct node *> >::iterator itr = levelwise.begin();
//					itr != levelwise.end(); itr++) {
//				string level2 = (*itr).first;
//				if (level.compare(level2) == 0)
//					continue;
//				//level was slave of level2, promote[level] became master of level2,
//				//but promote[level2] did not get promoted
//				if (level.find(level2) == 0 &&
//					level2.compare(promote[level]) != 0 && level2.find(promote[level]) == 0 &&
//					promote[level2].compare(promote[level]) != 0 && promote[level2].find(promote[level]) == 0
//				) {
//					cerr << "Master " << level2 << " of " << level << " has not got promoted to "
//							<< promote[level] << ". It's still at " << promote[level2] << endl;
//					assert(0);
//				}
//			}
//		}
//
//	}
//
//	bool changed = true;
//	while (changed) {
//		changed = false;
//		for (map<string, vector<struct node *> >::iterator it = levelwise.begin(); it != levelwise.end(); it++) {
//			string level = (*it).first;
//			vector<struct node *> lvl_tps = (*it).second;
//			if (promote[level].compare(level) != 0) {
//				for (vector<struct node *>::iterator tit = lvl_tps.begin(); tit != lvl_tps.end(); tit++) {
//					struct node *gnode = (*tit);
//					TP *tp = (TP *)gnode->data;
//
//					LIST *next = gnode->nextTP;
//
//					while (next) {
//						TP *tpn = (TP *)next->gnode->data;
//						//tpn and tp were not in master-slave hierarchy earlier
//						//but the promotion of tp has now put them in the master-slave
//						//hierarchy, so we need to equate their promotion hierarchies
//
////						cout << "TP "; tp->print(); cout << " "; tpn->print(); cout << endl;
////						cout << "promote[tp->strlevel] "<< promote[tp->strlevel] << " promote[tpn->strlevel] "
////								<< promote[tpn->strlevel] << endl;
//
//						if (!tpn->isSlave(tp->strlevel) && !tp->isSlave(tpn->strlevel) &&
//							promote[tpn->strlevel].compare(promote[tp->strlevel]) != 0 &&
//							promote[tpn->strlevel].find(promote[tp->strlevel]) == 0) {
//
//							promote[tpn->strlevel] = promote[tp->strlevel];
//							changed = true;
//						}
//						next = next->next;
//					}
//				}
//			}
//		}
//	}
//
////	cout << "******************************" << endl;
//
//	for (int i = 0; i < graph_tp_nodes; i++) {
//		TP *tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
//		tp->old_level = tp->strlevel;
////		if (tp->strlevel.find("s") == string::npos || promote.find(tp->strlevel) == promote.end()) {
////			continue;
////		}
//		tp->strlevel = promote[tp->strlevel];
//		if (tp->strlevel.find("s") == string::npos)
//			tp->rollback = true;
//	}
//
//	for (int i = 0; i < graph_tp_nodes; i++) {
//		TP *tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
//
//		if (tp->real_masters.size() > 0) {
//			tp->pseudomaster.insert(tp->pseudomaster.begin(), tp->real_masters.begin(), tp->real_masters.end());
//			tp->real_masters.clear();
//		}
//	}
//	//TODO: add a small fix here for queries with multi-level OPTIONAL patterns
//	for (int i = 0; i < graph_tp_nodes; i++) {
//		TP *tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
//		if (tp->strlevel.find("s") == string::npos || tp->strlevel.at(0) == 'm')
//			continue;
//
//		struct node *gnode = &graph[MAX_JVARS_IN_QUERY + i];
//
//		LIST *prev = NULL;
//
//		for (LIST *next = graph[MAX_JVARS_IN_QUERY + i].nextTP; next; ) {
//			TP *other = (TP *)next->gnode->data;
//			struct node *neighb = next->gnode;
//
//			if (other->strlevel.at(0) == 'm') {
//				prev = next;
//				next = next->next;
//				continue;
//			}
//			//Both adj nodes have 's' char at the beginning, so delete this connection
//			if (prev) {
//				prev->next = next->next;
//			} else {
//				gnode->nextTP = next->next;
//			}
//			LIST *tmp = next;
//			next = next->next;
//			free(tmp);
//			graph[MAX_JVARS_IN_QUERY + i].numTPs--;
//
//			//Now delete the other node's conn too
//			LIST *nextn = neighb->nextTP;
//			LIST *prevn = NULL;
//			while (nextn) {
//				if (nextn->gnode == gnode) {
//					if (prevn) {
//						prevn->next = nextn->next;
//					} else {
//						neighb->nextTP = nextn->next;
//					}
//					tmp = nextn;
//					nextn = nextn->next;
//					free(tmp);
//					neighb->numTPs--;
//				} else {
//					prevn = nextn;
//					nextn = nextn->next;
//				}
//			}
//		}
//	}
//
	levelwise.clear();
	leveljvars.clear();
	jvarlevels.clear();

	for (int i = 0; i < graph_tp_nodes; i++) {
		TP *tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
		levelwise[tp->strlevel].push_back(&graph[MAX_JVARS_IN_QUERY + i]);
		LIST *next = graph[MAX_JVARS_IN_QUERY + i].nextadjvar;
		for (; next; next = next->next) {
			if (((JVAR *)next->gnode->data)->nodenum > MAX_JVARS) {
				leveljvars[tp->strlevel].insert(next->gnode);
				jvarlevels[next->gnode].insert(tp->strlevel);
			}
		}
	}

	return opt;

	////////////////////
	///DEBUG:TODO: remove later
	////////////////////
	/*
	cout << "***********************************" << endl;
	for (int i = 0; i < graph_tp_nodes; i++) {
		TP *tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
		for (int j = i+1; j < graph_tp_nodes; j++) {
			TP *tp2 = (TP *)graph[MAX_JVARS_IN_QUERY + j].data;
			tp->print();
			tp2->print();
			cout << " " << tp->isSlave(tp2->strlevel) << " ";
			cout << tp2->isSlave(tp->strlevel) << endl;
		}
	}
	cout << "***********************************" << endl;
	*/

//	print_graph();
}

void mark_needed_tps(vector<vector<struct node*>> &cycles)
{
	set<string> covered_set;

	for (vector<vector<struct node*> >::iterator itr = cycles.begin(); itr != cycles.end(); itr++) {
		vector<struct node *> cycle = *itr;
		cycle.push_back(cycle[0]);
		for (int i = 0; i < cycle.size()-1; i++) {
			struct node *jvar1 = cycle[i];
			struct node *jvar2 = cycle[i+1];
			bool found_tp = false;
			for (int j = 0; j < graph_tp_nodes; j++) {
				TP *tp = (TP*)graph[j+MAX_JVARS_IN_QUERY].data;
				if (tp->containsJvar(((JVAR *)jvar1->data)->nodenum) &&
						tp->containsJvar(((JVAR *)jvar2->data)->nodenum)) {
					found_tp = true;
					graph[j + MAX_JVARS_IN_QUERY].isNeeded = true;
					char str[10];
					sprintf(str, "%d:%d", ((JVAR *)jvar1->data)->nodenum, ((JVAR *)jvar2->data)->nodenum);
					covered_set.insert(string(str));
					sprintf(str, "%d:%d", ((JVAR *)jvar2->data)->nodenum, ((JVAR *)jvar1->data)->nodenum);
					covered_set.insert(string(str));
				}
			}
			if (!found_tp) assert(0);
		}
		cycle.pop_back();
	}

	for (int i = 0; i < graph_var_nodes; i++) {
		if (graph[i].type == NON_JVAR_NODE)
			continue;

		JVAR *jvar = (JVAR *)graph[i].data;

		for (LIST *next = graph[i].nextadjvar; next; next = next->next) {
			JVAR *nbr = (JVAR *)next->gnode->data;

			char str[10]; string st1, st2;
			sprintf(str, "%d:%d", jvar->nodenum, nbr->nodenum);
			st1.append(str);
			sprintf(str, "%d:%d", nbr->nodenum, jvar->nodenum);
			st2.append(str);
			set<string>::iterator sit = covered_set.find(st1);
			if (sit == covered_set.end()) {
				sit = covered_set.find(st2);
				if (sit != covered_set.end())
					continue;
				//The jvar pair is not found in covered_set
				//See if it covered by cycle
				bool jvar_found = false, nbr_found = false;
				for (vector<vector<struct node*> >::iterator itr = cycles.begin(); itr != cycles.end(); itr++) {
					jvar_found = false; nbr_found = false;
					vector<struct node *> cycle = *itr;
					for (int i = 0; i < cycle.size(); i++) {
						if (cycle[i] == &graph[i])
							jvar_found = true;
						if (cycle[i] == next->gnode)
							nbr_found = true;
					}
					if (jvar_found && nbr_found)
						break;
				}
				// jvar and its nbr not found in any cycle
				if (!jvar_found || !nbr_found) {
					bool found_tp = false;
					for (int j = 0; j < graph_tp_nodes; j++) {
						TP *tp = (TP*)graph[j+MAX_JVARS_IN_QUERY].data;
						if (tp->containsJvar(jvar->nodenum) && tp->containsJvar(nbr->nodenum)) {
							found_tp = true;
							graph[j + MAX_JVARS_IN_QUERY].isNeeded = true;
						}
					}
					if (!found_tp) assert(0);
				}
			}
		}
	}
}

bool is_allslave_bottomup_pass(void)
{
	for (map<string, vector<struct node *> >::iterator it = levelwise.begin(); it != levelwise.end(); it++) {
		string level = (*it).first;
		if (level.find("s") == string::npos)
			continue;
		vector<struct node *> tps = (*it).second;

		set<int> jnums;
		set<struct node *> s;
		//Collect all jvars appearing in level S, such that they have a join at this level too
		//that is, don't collect jvars which appearing in this level because they are joining
		//over it with another level
		/*
		 * -1:9:-2
		 * OPTIONAL{
		 *   -2:10:-3
		 *   -3:11:-4
		 *   -4:12:-101
		 * }
		 * then collect only -3 and -4 for the slave level
		 */
		for (vector<struct node *>::iterator vit = tps.begin(); vit != tps.end(); vit++) {
			for (vector<struct node *>::iterator vit2 = tps.begin(); vit2 != tps.end(); vit2++) {
				if (*vit == *vit2)
					continue;

				TP *tp1 = (TP *)(*vit)->data;
				TP *tp2 = (TP *)(*vit2)->data;

				if (tp1->sub < 0 && (tp1->sub == tp2->sub || tp1->sub == tp2->obj)) {
					jnums.insert(tp1->sub);
				}
				if (tp1->pred < 0 && tp1->pred == tp2->pred) {
					jnums.insert(tp1->pred);
				}
				if (tp1->obj < 0 && (tp1->obj == tp2->obj || tp1->obj == tp2->sub)) {
					jnums.insert(tp1->obj);
				}
			}
		}

		for (int i = 0; i < graph_var_nodes; i++) {
			if (graph[i].type == NON_JVAR_NODE)
				continue;
			JVAR *jvar = (JVAR *)graph[i].data;

			if (jnums.find(jvar->nodenum) == jnums.end())
				continue;

			s.insert(&graph[i]);
		}

		for (int i = 0; i < graph_jvar_nodes && s.size() > 1; i++) {
			if (s.find(jvarsitr2[i]) == s.end()) {
				continue;
			}
			s.erase(jvarsitr2[i]);
			bool isconn = false;
			for (LIST *nextadjvar = jvarsitr2[i]->nextadjvar; nextadjvar && s.size() > 0; nextadjvar = nextadjvar->next) {
				if (s.find(nextadjvar->gnode) == s.end())
					continue;
				isconn = true;
				break;
			}
			if (!isconn) {
				return false;
			}
		}

		s.clear();
	}
	return true;
}

bool get_longest_cycles(vector<vector<struct node*>> &cycles)
{
	for (int i = 0; i < graph_var_nodes; i++) {
		if (graph[i].type == JVAR_NODE) {
			vector<struct node *>path; set<struct node *> visited;
			path.push_back(&graph[i]);
			enumerate_cycles(&graph[i], NULL, path, visited, cycles);
			path.clear(); visited.clear();
		}
	}
//	cout << "----------- Cycles ------------" << endl;
	unsigned int longest_cycle = 0;

	for (vector<vector<struct node*> >::iterator itr = cycles.begin(); itr != cycles.end(); ) {
		vector<struct node *> cycle = *itr;
		struct node *first = cycle[0];
		struct node *last = cycle[cycle.size()-1];

		bool found = false;

		for (LIST *next = first->nextadjvar; next; next = next->next) {
			if (last == next->gnode) {
				found = true;
				break;
			}
		}
		//Check if found a proper cycle
		if (!found) {
			itr = cycles.erase(itr);
			continue;
		}

		if (longest_cycle < cycle.size()) {
			longest_cycle = cycle.size();
		}
//		cout << "[";
//		for (vector<struct node *>::iterator cit = cycle.begin(); cit != cycle.end(); cit++) {
//			cout << ((JVAR *)(*cit)->data)->nodenum << " ";
//		}
//		cout << "]" << endl;
		itr++;
	}

//	cout << "Longest cycle " << longest_cycle << endl;

	vector<set<struct node *> > cycle_node_set;
	//detect and keep unique cycles, erase permutations of the same cycle
	for (vector<vector<struct node*> >::iterator itr = cycles.begin(); itr != cycles.end(); ) {
		vector<struct node *> cycle = *itr;

		set<struct node *> node_set; node_set.insert(cycle.begin(), cycle.end());
		bool found_dupl = false;

		for (vector<set<struct node *> >::iterator vit = cycle_node_set.begin(); vit != cycle_node_set.end(); vit++) {
			set<struct node *> nset = *vit;
			for (set<struct node *>::iterator sit = nset.begin(); sit != nset.end(); sit++) {
				if (node_set.find(*sit) != node_set.end()) {
					node_set.erase(*sit);
				}
			}

			if (node_set.empty()) {
				//a permutation of this cycle already exists so delete it
				found_dupl = true;
				break;
			}
		}
		if (found_dupl) {
			itr = cycles.erase(itr);
			continue;
		}

		node_set.clear();
		node_set.insert(cycle.begin(), cycle.end());
		cycle_node_set.push_back(node_set);

		itr++;
	}

	//now cycles have all UNIQUE cycles with all lenghts of cycles.. not just longest

	//Now detect if a slave has cycle
	bool slave_cycle = false;
	for (vector<vector<struct node*> >::iterator itr = cycles.begin(); itr != cycles.end(); itr++) {
		vector<struct node *> cycle = *itr;

		set<struct node *> node_set; node_set.insert(cycle.begin(), cycle.end());

		for (vector<struct node *>::iterator cit = cycle.begin(); cit != cycle.end(); cit++) {
			for (LIST *nextadjTP = (*cit)->nextTP; nextadjTP; nextadjTP = nextadjTP->next) {
				TP *tp = (TP *)nextadjTP->gnode->data;
				if (tp->strlevel.find("s") != string::npos) {
					//found slave TP
					node_set.erase(*cit);
					break;
				}
			}
		}
		//this means that each jvar in the cycle has at least one slave
		//tp associated with it.. indicating that there is a cycle in a slave
		//supernode.
		if (node_set.empty()) {
			slave_cycle = true;
			break;
		}
	}

	//If there is a cycle among absolute master and slave, then only best-match is reqd
	for (vector<vector<struct node*> >::iterator itr = cycles.begin(); itr != cycles.end() && !slave_cycle; itr++) {
		vector<struct node *> cycle = *itr;

		set<struct node *> node_set; node_set.insert(cycle.begin(), cycle.end());

		for (vector<struct node *>::iterator cit = cycle.begin(); cit != cycle.end(); cit++) {
			for (LIST *nextadjTP = (*cit)->nextTP; nextadjTP; nextadjTP = nextadjTP->next) {
				TP *tp = (TP *)nextadjTP->gnode->data;
				if (tp->strlevel.find("s") == string::npos) {
					//found master TP
					node_set.erase(*cit);
					break;
				}
			}
		}
		//this means that the cycle is not only among the master nodes
		if (!node_set.empty()) {
			slave_cycle = true;
			break;
		}
	}

	for (vector<vector<struct node*> >::iterator itr = cycles.begin(); itr != cycles.end(); ) {
		vector<struct node *> cycle = *itr;
		//erase shorter cycles
		if (cycle.size() < longest_cycle) {
			itr = cycles.erase(itr);
			continue;
		}

		itr++;
	}

//	cout << "Longest unique cycles" << endl;
//
//	for (vector<vector<struct node*> >::iterator itr = cycles.begin(); itr != cycles.end(); ) {
//		vector<struct node *> cycle = *itr;
//		cout << "[";
//		for (vector<struct node *>::iterator cit = cycle.begin(); cit != cycle.end(); cit++) {
//			cout << ((JVAR *)(*cit)->data)->nodenum << " ";
//		}
//		cout << "]" << endl;
//		itr++;
//	}

	return slave_cycle;
}

////////////////////////////////////////////////////////////
void identify_true_jvars()
{
	cout << "************** TRUE JVARS *****************" << endl;

	for (map<string, set<struct node *> >::iterator it = leveljvars.begin(); it != leveljvars.end(); it++) {
		set<struct node *> sjvars = (*it).second;
		string level = (*it).first;

		cout << "*********** level " << (*it).first << " ************" << endl;
		for (set<struct node *>::iterator sit = sjvars.begin(); sit != sjvars.end(); sit++) {
			LIST *next = (*sit)->nextTP;
			TP *sampleTP = (TP *)levelwise[level][0]->data;
			// some tp at the same level as 'level'
			int occur = 0;
			
			for (; next; next = next->next) {
				TP *tp = (TP *)next->gnode->data;
				if (tp->strlevel.compare(level) == 0 ||
					(sampleTP->isSlave(tp->strlevel)) ||
					(!sampleTP->isSlave(tp->strlevel) && !tp->isSlave(sampleTP->strlevel)) ) {
					occur++;
				}
			}

			if (occur > 1) {
				true_jvars[level].insert(*sit);
			}
		}
	}
	////////////////////
	//DEBUG
	///////////////////
	for (map<string, set<struct node *> >::iterator it = true_jvars.begin(); it != true_jvars.end(); it++) {
		cout << "*********** level2 " << (*it).first << " ************" << endl;
		for (set<struct node *>::iterator sit = (*it).second.begin(); sit != (*it).second.end(); sit++) {
			cout << ((JVAR *)(*sit)->data)->nodenum << " ";
		}

		cout << endl;
	}
}

////////////////////////////////////////////////////////////

void build_graph_new(void)
{
	int i, j, k;
	TP *tp;
	JVAR *jvar;
	LIST *next = NULL;
	LIST *nextvar = NULL;

	//build graph's adjacency list representation
	//this loop only creates TP -> var and var -> TP conn
	
	// go over TP nodes
	for (i = 0; i < graph_tp_nodes; i++) {
		if (ASSERT_ON)
			assert(graph[MAX_JVARS_IN_QUERY + i].type == TP_NODE);
		tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;

		//connect this with the join var in
		//jvarlist
		//TODO: take care for ?s :p1 ?s case
		/////////////////////////////////////////////
		// go over var nodes
		for (j = 0; j < graph_var_nodes; j++) {

			jvar = (JVAR *)graph[j].data;

			if (tp->sub == jvar->nodenum || tp->pred == jvar->nodenum || tp->obj == jvar->nodenum) {
				//add tp -> var edge
				if (graph[MAX_JVARS_IN_QUERY + i].numadjvars == 0) {
					graph[MAX_JVARS_IN_QUERY + i].nextadjvar = (LIST *) malloc (sizeof(LIST));
					graph[MAX_JVARS_IN_QUERY + i].nextadjvar->gnode = &graph[j];
					graph[MAX_JVARS_IN_QUERY + i].nextadjvar->next = NULL;
				} else {
					next = graph[MAX_JVARS_IN_QUERY + i].nextadjvar;
					for (k = 0; k < graph[MAX_JVARS_IN_QUERY + i].numadjvars - 1; k++) {
						next = next->next;
					}
					next->next = (LIST *) malloc (sizeof(LIST));
					next->next->gnode = &graph[j];
					next->next->next = NULL;
				}
				graph[MAX_JVARS_IN_QUERY + i].numadjvars++;
				if (jvar->nodenum >= MAX_JVARS) {
					graph[MAX_JVARS_IN_QUERY + i].numadjjvars++;
				}

				//add var -> tp edge
				if (graph[j].numTPs == 0) {
					graph[j].nextTP = (LIST *) malloc (sizeof(LIST));
					graph[j].nextTP->gnode = &graph[MAX_JVARS_IN_QUERY + i];
					graph[j].nextTP->next = NULL;
				} else  {
					next = graph[j].nextTP;
					for (k = 0; k < graph[j].numTPs - 1; k++) {
						next = next->next;
					}
					next->next = (LIST *) malloc (sizeof(LIST));
					next->next->gnode = &graph[MAX_JVARS_IN_QUERY + i];
					next->next->next = NULL;
				}
				graph[j].numTPs++;
				
			}
		}
		//////////////////////////////////////////////////////////////
	}

	//code to add var -> var edge if they appear in the same TP
	
	for (i = 0; i < graph_var_nodes; i++) {
		jvar = (JVAR*)graph[i].data;

		for (j = i+1; j < graph_var_nodes; j++) {
			JVAR *jvar2 = (JVAR*)graph[j].data;

			for (k = 0; k < graph_tp_nodes; k++) {
				tp = (TP *)graph[MAX_JVARS_IN_QUERY + k].data;
				if (jvar->nodenum != jvar2->nodenum &&
						tp->containsJvar(jvar->nodenum) && tp->containsJvar(jvar2->nodenum)) {
					if (graph[i].numadjvars == 0) {
						graph[i].nextadjvar = (LIST *) malloc (sizeof(LIST));
						graph[i].nextadjvar->gnode = &graph[j];
						graph[i].nextadjvar->next = NULL;
					} else {
						LIST *varitr = graph[i].nextadjvar;
						for (int m = 0; m < graph[i].numadjvars - 1; m++) {
							varitr = varitr->next;
						}
						assert(varitr != NULL);
						varitr->next = (LIST *) malloc (sizeof(LIST));
						varitr->next->gnode = &graph[j];
						varitr->next->next = NULL;
					}
					graph[i].numadjvars++;
					if (jvar2->nodenum >= MAX_JVARS) {
						graph[i].numadjjvars++;
					}

					if (graph[j].numadjvars == 0) {
						graph[j].nextadjvar = (LIST *) malloc (sizeof(LIST));
						graph[j].nextadjvar->gnode = &graph[i];
						graph[j].nextadjvar->next = NULL;
					} else {
						LIST *varitr = graph[j].nextadjvar;
						for (int m = 0; m < graph[j].numadjvars - 1; m++) {
							varitr = varitr->next;
						}
						assert(varitr != NULL);
						varitr->next = (LIST *) malloc (sizeof(LIST));
						varitr->next->gnode = &graph[i];
						varitr->next->next = NULL;
					}
					graph[j].numadjvars++;
					if (jvar->nodenum >= MAX_JVARS) {
						graph[j].numadjjvars++;
					}

				}
			}
		}
	}

//	for (j = 0; j < graph_var_nodes; j++) {
//		jvar = (JVAR*)graph[j].data;
//
//		if (ASSERT_ON)
//			assert(graph[j].numTPs > 0);
//
//		int numTPs = graph[j].numTPs;
//		next = graph[j].nextTP;
//
//		while (numTPs) {
//			assert(next);
//			tp = (TP *)next->gnode->data;
//			int numvars = graph[tp->nodenum].numadjvars;
////			cout << "HERE [" << tp->sub << " " << tp->pred << " " << tp->obj << " " << numvars << " " << graph[tp->nodenum].numadjvars
////				<< " " << tp->numvars << endl;
//			assert(numvars == tp->numvars);
//			nextvar = graph[tp->nodenum].nextadjvar;
//			if (numvars != 1) {
//				while(numvars > 0) {
//					struct node *gnode2 = nextvar->gnode;
//					JVAR *jvar2 = (JVAR *)gnode2->data;
//					if (jvar->nodenum != jvar2->nodenum) {
//						if (graph[j].numadjvars == 0) {
//							graph[j].nextadjvar = (LIST *) malloc (sizeof(LIST));
//							graph[j].nextadjvar->gnode = gnode2;
//							graph[j].nextadjvar->next = NULL;
//						} else {
//							LIST *varitr = graph[j].nextadjvar;
//							for (k = 0; k < graph[j].numadjvars - 1; k++) {
//								varitr = varitr->next;
//							}
//							assert(varitr != NULL);
//							varitr->next = (LIST *) malloc (sizeof(LIST));
//							varitr->next->gnode = gnode2;
//							varitr->next->next = NULL;
//						}
//						graph[j].numadjvars++;
//
//					}
//					nextvar = nextvar->next;
//					numvars--;
//				}
//
//			}
//			//go on to next TP to which this jvar is conn
//			next = next->next;
//			numTPs--;
//		}
//	}

	//add TP <-> TP edges if they share a join var
	//TODO: might need a fix later for 2 triples sharing more than
	//one join variable.. it's not commonly observed.. but needs a fix

	for (i = 0; i < graph_tp_nodes; i++) {
		tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;

		for (j = i + 1; j < graph_tp_nodes; j++) {
			TP *tp2 = (TP *)graph[MAX_JVARS_IN_QUERY + j].data;

			int numTPs = graph[MAX_JVARS_IN_QUERY + i].numTPs;

			//two triples share the same subject
			if (tp->sub < 0 && tp->sub == tp2->sub) {
				if (numTPs == 0) {
					graph[MAX_JVARS_IN_QUERY + i].nextTP = (LIST *) malloc (sizeof(LIST));
					graph[MAX_JVARS_IN_QUERY + i].nextTP->next = NULL;
					next = graph[MAX_JVARS_IN_QUERY + i].nextTP;
				} else {
					next = graph[MAX_JVARS_IN_QUERY + i].nextTP;
					for (k = 0; k < numTPs - 1; k++) {
						next = next->next;
					}
					assert(next != NULL);
					next->next = (LIST *) malloc (sizeof(LIST));
					next->next->next = NULL;
					next = next->next;
				}
				next->gnode = &graph[MAX_JVARS_IN_QUERY + j];
				next->edgetype = SUB_EDGE;
				graph[MAX_JVARS_IN_QUERY + i].numTPs++;

				//Add other way edge too
				numTPs = graph[MAX_JVARS_IN_QUERY + j].numTPs;
				if (numTPs == 0) {
					graph[MAX_JVARS_IN_QUERY + j].nextTP = (LIST *) malloc (sizeof(LIST));
					graph[MAX_JVARS_IN_QUERY + j].nextTP->next = NULL;
					next = graph[MAX_JVARS_IN_QUERY + j].nextTP;
				} else {
					next = graph[MAX_JVARS_IN_QUERY + j].nextTP;
					for (k = 0; k < numTPs - 1; k++) {
						next = next->next;
					}
					assert(next != NULL);
					next->next = (LIST *) malloc (sizeof(LIST));
					next->next->next = NULL;
					next = next->next;
				}
				next->gnode = &graph[MAX_JVARS_IN_QUERY + i];
				next->edgetype = SUB_EDGE;
				graph[MAX_JVARS_IN_QUERY + j].numTPs++;

				continue;
			}

			//two triples share the same predicate
			if (tp->pred < 0 && tp->pred == tp2->pred) {
				if (numTPs == 0) {
					graph[MAX_JVARS_IN_QUERY + i].nextTP = (LIST *) malloc (sizeof(LIST));
					graph[MAX_JVARS_IN_QUERY + i].nextTP->next = NULL;
					next = graph[MAX_JVARS_IN_QUERY + i].nextTP;
				} else {
					next = graph[MAX_JVARS_IN_QUERY + i].nextTP;
					for (k = 0; k < numTPs - 1; k++) {
						next = next->next;
					}
					assert(next != NULL);
					next->next = (LIST *) malloc (sizeof(LIST));
					next->next->next = NULL;
					next = next->next;
				}
				next->gnode = &graph[MAX_JVARS_IN_QUERY + j];
				next->edgetype = PRED_EDGE;
				graph[MAX_JVARS_IN_QUERY + i].numTPs++;

				//Add other way edge too
				numTPs = graph[MAX_JVARS_IN_QUERY + j].numTPs;
				if (numTPs == 0) {
					graph[MAX_JVARS_IN_QUERY + j].nextTP = (LIST *) malloc (sizeof(LIST));
					graph[MAX_JVARS_IN_QUERY + j].nextTP->next = NULL;
					next = graph[MAX_JVARS_IN_QUERY + j].nextTP;
				} else {
					next = graph[MAX_JVARS_IN_QUERY + j].nextTP;
					for (k = 0; k < numTPs - 1; k++) {
						next = next->next;
					}
					assert(next != NULL);
					next->next = (LIST *) malloc (sizeof(LIST));
					next->next->next = NULL;
					next = next->next;
				}
				next->gnode = &graph[MAX_JVARS_IN_QUERY + i];
				next->edgetype = PRED_EDGE;
				graph[MAX_JVARS_IN_QUERY + j].numTPs++;

				continue;
			}

			//two triples share the same object
			if (tp->obj < 0 && tp->obj == tp2->obj) {
				if (numTPs == 0) {
					graph[MAX_JVARS_IN_QUERY + i].nextTP = (LIST *) malloc (sizeof(LIST));
					graph[MAX_JVARS_IN_QUERY + i].nextTP->next = NULL;
					next = graph[MAX_JVARS_IN_QUERY + i].nextTP;
				} else {
					next = graph[MAX_JVARS_IN_QUERY + i].nextTP;
					for (k = 0; k < numTPs - 1; k++) {
						next = next->next;
					}
					assert(next != NULL);
					next->next = (LIST *) malloc (sizeof(LIST));
					next->next->next = NULL;
					next = next->next;
				}
				next->gnode = &graph[MAX_JVARS_IN_QUERY + j];
				next->edgetype = OBJ_EDGE;
				graph[MAX_JVARS_IN_QUERY + i].numTPs++;

				//Add other way edge too
				numTPs = graph[MAX_JVARS_IN_QUERY + j].numTPs;
				if (numTPs == 0) {
					graph[MAX_JVARS_IN_QUERY + j].nextTP = (LIST *) malloc (sizeof(LIST));
					graph[MAX_JVARS_IN_QUERY + j].nextTP->next = NULL;
					next = graph[MAX_JVARS_IN_QUERY + j].nextTP;
				} else {
					next = graph[MAX_JVARS_IN_QUERY + j].nextTP;
					for (k = 0; k < numTPs - 1; k++) {
						next = next->next;
					}
					assert(next != NULL);
					next->next = (LIST *) malloc (sizeof(LIST));
					next->next->next = NULL;
					next = next->next;
				}
				next->gnode = &graph[MAX_JVARS_IN_QUERY + i];
				next->edgetype = OBJ_EDGE;
				graph[MAX_JVARS_IN_QUERY + j].numTPs++;

				continue;
			}

			//two triples share the same subject and object
			if ((tp->sub < 0 && tp->sub == tp2->obj) || (tp->obj < 0 && tp->obj == tp2->sub)) {
				if (numTPs == 0) {
					graph[MAX_JVARS_IN_QUERY + i].nextTP = (LIST *) malloc (sizeof(LIST));
					graph[MAX_JVARS_IN_QUERY + i].nextTP->next = NULL;
					next = graph[MAX_JVARS_IN_QUERY + i].nextTP;
				} else {
					next = graph[MAX_JVARS_IN_QUERY + i].nextTP;
					for (k = 0; k < numTPs - 1; k++) {
						next = next->next;
					}
					assert(next != NULL);
					next->next = (LIST *) malloc (sizeof(LIST));
					next->next->next = NULL;
					next = next->next;
				}
				next->gnode = &graph[MAX_JVARS_IN_QUERY + j];
				next->edgetype = SO_EDGE;
				graph[MAX_JVARS_IN_QUERY + i].numTPs++;

				//Add other way edge too
				numTPs = graph[MAX_JVARS_IN_QUERY + j].numTPs;
				if (numTPs == 0) {
					graph[MAX_JVARS_IN_QUERY + j].nextTP = (LIST *) malloc (sizeof(LIST));
					graph[MAX_JVARS_IN_QUERY + j].nextTP->next = NULL;
					next = graph[MAX_JVARS_IN_QUERY + j].nextTP;
				} else {
					next = graph[MAX_JVARS_IN_QUERY + j].nextTP;
					for (k = 0; k < numTPs - 1; k++) {
						next = next->next;
					}
					assert(next != NULL);
					next->next = (LIST *) malloc (sizeof(LIST));
					next->next->next = NULL;
					next = next->next;
				}
				next->gnode = &graph[MAX_JVARS_IN_QUERY + i];
				next->edgetype = SO_EDGE;
				graph[MAX_JVARS_IN_QUERY + j].numTPs++;

			}
		}
	}
}

////////////////////////////////////////////////////////////

bool init_tp_nodes_new(bool bushy)
{

#if MMAPFILES
	mmap_all_files();
#endif

	for (int i = 0; i < graph_tp_nodes; i++) {

		TP *tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
		if (tp->isSlaveOfAnyone(null_perm)) {
			null_perm.insert(tp->strlevel);
			tp->triplecnt = 0;
			tp->bitmat.num_triples = 0;
			continue;
		}

		if (tp->pred > 0) {
			if (tp->sub > 0) {
				//Case :s :p ?o 
				//load only one row from PSO bitmat

				char dumpfile[1024];
				sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_PSO")].c_str());
				//Now find
//				cout << "Loading from dumpfile1 " << dumpfile << " for " << tp->sub << " " << tp->pred << " " << tp->obj << endl;
				shallow_init_bitmat(&tp->bitmat, gnum_preds, gnum_subs, gnum_objs, gnum_comm_so, PSO_BITMAT);
//				cout << "After new bitmat" << endl;
				if (!add_row(&tp->bitmat, dumpfile, tp->sub, PSO_BITMAT, tp->pred, true)) {
					if (tp->strlevel.find("s") == string::npos) {
						cout << "0 results1 for tp [" << tp->sub << ", " << tp->pred << ", " << tp->obj << "]" << endl;
						return false;
					} else {
						cout << "0 results1.1 for tp [" << tp->sub << ", " << tp->pred << ", " << tp->obj << "]" << endl;
						null_perm.insert(tp->strlevel);
						tp->triplecnt = 0;
						tp->bitmat.num_triples = 0;
						continue;
					}
				}
				tp->triplecnt = count_triples_in_bitmat(&tp->bitmat);

			} else if (tp->obj > 0) {
				//Case ?s :p :o 
				//load only one row from POS bitmat
				char dumpfile[1024];
				sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_POS")].c_str());
//				cout << "Loading from dumpfile2 " << dumpfile << " for " << tp->sub << " " << tp->pred << " " << tp->obj << endl;
				shallow_init_bitmat(&tp->bitmat, gnum_preds, gnum_objs, gnum_subs, gnum_comm_so, POS_BITMAT);

				if (!add_row(&tp->bitmat, dumpfile, tp->obj, POS_BITMAT, tp->pred, true)) {
					if (tp->strlevel.find("s") == string::npos) {
						cout << "0 results2 for tp [" << tp->sub << ", " << tp->pred << ", " << tp->obj << "]" << endl;
						return false;
					} else {
						cout << "0 results2.1 for tp [" << tp->sub << ", " << tp->pred << ", " << tp->obj << "]" << endl;
						null_perm.insert(tp->strlevel);
						tp->triplecnt = 0;
						tp->bitmat.num_triples = 0;
						continue;
					}
				}
				tp->triplecnt = count_triples_in_bitmat(&tp->bitmat);

			} else {
				// ?s :p ?o
				//the whole bitmat
				//Bushy is irrelevant with new changes now
				if (bushy) {
					char dumpfile[1024];
					sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_SPO")].c_str());

					unsigned long offset = get_offset(dumpfile, tp->pred);
					
//					cout << "Loading from dumpfile7 " << dumpfile << " for " << tp->sub << " " << tp->pred << " " << tp->obj << endl;

					shallow_init_bitmat(&tp->bitmat, gnum_subs, gnum_preds, gnum_objs, gnum_comm_so, SPO_BITMAT);
					init_bitmat_rows(&tp->bitmat, false, true);

					int fd = open(dumpfile, O_RDONLY | O_LARGEFILE);
					assert(fd != -1);

					if (offset > 0)
						lseek(fd, offset, SEEK_CUR);

					read(fd, &tp->bitmat.num_triples, sizeof(unsigned int));

					if (comp_folded_arr) {
						unsigned int comp_arr_size = 0;
						read(fd, &comp_arr_size, ROW_SIZE_BYTES);
						unsigned char *comp_arr = (unsigned char *) malloc (comp_arr_size);
						read(fd, comp_arr, comp_arr_size);
						dgap_uncompress(comp_arr, comp_arr_size, tp->bitmat.subfold, tp->bitmat.subject_bytes);

						free(comp_arr);
						
						comp_arr_size = 0;
						read(fd, &comp_arr_size, ROW_SIZE_BYTES);
						comp_arr = (unsigned char *) malloc (comp_arr_size);
						read(fd, comp_arr, comp_arr_size);
						dgap_uncompress(comp_arr, comp_arr_size, tp->bitmat.objfold, tp->bitmat.object_bytes);

						free(comp_arr);

					} else {
						read(fd, tp->bitmat.subfold, tp->bitmat.subject_bytes);
						read(fd, tp->bitmat.objfold, tp->bitmat.object_bytes);
					}
					
					close(fd);

//					tp->bitmat.bm = NULL;

					continue;
				}
				if (tp->sub < 0 && tp->sub > MAX_JVARS && tp->obj < 0 && tp->obj > MAX_JVARS) {
					//?s :p ?o with ?s and ?o both jvars
					//load later
					char dumpfile[1024];
					sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_SPO")].c_str());

					unsigned long offset = get_offset(dumpfile, tp->pred);

#if MMAPFILES
					char *fpos = mmap_table[string(dumpfile)];
					memcpy(&tp->bitmat.num_triples, &fpos[offset], sizeof(unsigned int));
					tp->triplecnt = tp->bitmat.num_triples;
#else
					int fd = open(dumpfile, O_RDONLY);
					assert(fd != -1);
					if (offset > 0)
						lseek(fd, offset, SEEK_CUR);
					read(fd, &tp->bitmat.num_triples, sizeof(unsigned int));
					close(fd);
#endif

//					tp->bitmat.bm = NULL;

				} else if (tp->sub < 0 && tp->sub > MAX_JVARS) {
//					cout << "init_tp_nodes_new: ?s :p ?o with ?s as jvar" << endl;
					//Case ?s :p ?o
					//only ?s as jvar
					//load whole SPO bitmat
					char dumpfile[1024];
					sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_SPO")].c_str());

					unsigned long offset = get_offset(dumpfile, tp->pred);
					
//					cout << "Loading from dumpfile3 " << dumpfile << " for " << tp->sub << " " << tp->pred << " " << tp->obj << endl;
#if MMAPFILES
					char *fpos = mmap_table[string(dumpfile)];
					memcpy(&tp->bitmat.num_triples, &fpos[offset], sizeof(unsigned int));
					tp->triplecnt = tp->bitmat.num_triples;
#else
					int fd = open(dumpfile, O_RDONLY);
					assert(fd != -1);
					if (offset > 0)
						lseek(fd, offset, SEEK_CUR);
					read(fd, &tp->bitmat.num_triples, sizeof(unsigned int));
					close(fd);
#endif
					
//					init_bitmat(&tp->bitmat, gnum_subs, gnum_preds, gnum_objs, gnum_comm_so, SPO_BITMAT);
//					if (wrapper_load_from_dump_file(dumpfile, tp->pred, &graph[MAX_JVARS_IN_QUERY+i], true, true) == 0) {
//						if (tp->strlevel.find("s") == string::npos) {
//							cout << "0 results3 for tp [" << tp->sub << ", " << tp->pred << ", " << tp->obj << "]" << endl;
//							return false;
//						} else {
//							cout << "0 results3.1 for tp [" << tp->sub << ", " << tp->pred << ", " << tp->obj << "]" << endl;
//							null_perm.insert(tp->strlevel);
//							tp->triplecnt = 0;
//							tp->bitmat.num_triples = 0;
//						}
//
//					}

				} else if (tp->obj < 0 && tp->obj > MAX_JVARS) {
					//?s :p ?o
					//with O as jvar
					//load whole OPS bitmat
					char dumpfile[1024];
					sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_OPS")].c_str());

					//TODO: memset objfold array
					//Get the offset
					unsigned long offset = get_offset(dumpfile, tp->pred);

//					cout << "Loading from dumpfile4 " << dumpfile << " for " << tp->sub << " " << tp->pred << " " << tp->obj << endl;
#if MMAPFILES
					char *fpos = mmap_table[string(dumpfile)];
					memcpy(&tp->bitmat.num_triples, &fpos[offset], sizeof(unsigned int));
					tp->triplecnt = tp->bitmat.num_triples;
#else
					int fd = open(dumpfile, O_RDONLY);
					assert(fd != -1);
					if (offset > 0)
						lseek(fd, offset, SEEK_CUR);
					read(fd, &tp->bitmat.num_triples, sizeof(unsigned int));
					close(fd);
#endif

//					init_bitmat(&tp->bitmat, gnum_objs, gnum_preds, gnum_subs, gnum_comm_so, OPS_BITMAT);
//					if (wrapper_load_from_dump_file(dumpfile, tp->pred, &graph[MAX_JVARS_IN_QUERY+i], true, true) == 0) {
//						if (tp->strlevel.find("s") == string::npos) {
//							cout << "0 results4 for tp [" << tp->sub << ", " << tp->pred << ", " << tp->obj << "]" << endl;
//							return false;
//						} else {
//							cout << "0 results4.1 for tp [" << tp->sub << ", " << tp->pred << ", " << tp->obj << "]" << endl;
//							null_perm.insert(tp->strlevel);
//							tp->triplecnt = 0;
//							tp->bitmat.num_triples = 0;
//						}
//
//					}
				} else {
					//Case P
					//You should NOT reach here actually
					cout << "******FATAL error in init_tp_nodes_new*********" << endl;
					assert(0);
				}
			}

		} else {
			//Case P variable
			if (tp->sub > 0) {
				if (tp->obj > 0) {
					//case of :s ?p :o

					//Find out quickly by calculating offset
					//inside each *_sdump and *_odump files
					//which BM has least num of triples
					//and load that

					/////////////////////////////////////////////
					//TODO: write a small function to do this business
					char dumpfile[1024];
					sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_PSO")].c_str());

					unsigned long soffset = get_offset(dumpfile, tp->sub);

					int sfd = 0; int ofd = 0;
					char *sfpos = NULL; char *ofpos = NULL;
					unsigned int striples = 0; unsigned int otriples = 0;

#if MMAPFILES
					sfpos = mmap_table[string(dumpfile)];
					memcpy(&striples, &sfpos[soffset], sizeof(unsigned int));
					sfpos += (soffset + sizeof(unsigned int));
#else
					sfd = open(dumpfile, O_RDONLY);
					if (sfd < 0) {
						cout << "*** ERROR opening " << dumpfile << endl;
						assert (0);
					}

					if (soffset > 0)
						lseek(sfd, soffset, SEEK_CUR);
					read(sfd, &striples, sizeof(unsigned int));
#endif

					////////////////////////////////////////////////

					sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_POS")].c_str());

					unsigned long ooffset = get_offset(dumpfile, tp->obj);

#if MMAPFILES
					ofpos = mmap_table[string(dumpfile)];
					memcpy(&otriples, &ofpos[ooffset], sizeof(unsigned int));
					ofpos += (ooffset + sizeof(unsigned int));
#else
					ofd = open(dumpfile, O_RDONLY);
					if (ofd < 0) {
						cout << "*** ERROR opening " << dumpfile << endl;
						assert(0);
					}

					if (ooffset > 0)
						lseek(ofd, ooffset, SEEK_CUR);
					read(ofd, &otriples, sizeof(unsigned int));
#endif

					////////////////////////////////////////////////
					bool resexist = false;
//					unsigned char and_array[1+3*GAP_SIZE_BYTES];
					unsigned char *and_array = NULL;
					unsigned int and_array_size = 0;

					if (striples < otriples) {
						//Load the PSO bitmat

//						and_array_size = get_and_array(&tp->bitmat, and_array, tp->obj);
						and_array_size = get_mask_array(and_array, tp->obj);
//						cout << "Loading from PSO dumpfile for :s ?p :o case" << " for " << tp->sub << " " << tp->pred << " " << tp->obj << endl;
						shallow_init_bitmat(&tp->bitmat, gnum_preds, gnum_subs, gnum_objs, gnum_comm_so, PSO_BITMAT);
//						init_bitmat_rows(&tp->bitmat, true, false);
						resexist = filter_and_load_bitmat(&tp->bitmat, sfd, sfpos, and_array, and_array_size);

					} else {
						//Load the POS bitmat

//						and_array_size = get_and_array(&tp->bitmat, and_array, tp->sub);
						and_array_size = get_mask_array(and_array, tp->sub);
//						cout << "Loading from POS dumpfile for :s ?p :o case" << " for " << tp->sub << " " << tp->pred << " " << tp->obj << endl;
						shallow_init_bitmat(&tp->bitmat, gnum_preds, gnum_objs, gnum_subs, gnum_comm_so, POS_BITMAT);
//						init_bitmat_rows(&tp->bitmat, true, false);
						resexist = filter_and_load_bitmat(&tp->bitmat, ofd, ofpos, and_array, and_array_size);
					}

					if (!resexist) {
						if (tp->strlevel.find("s") == string::npos) {
							cout << "0 results5 for tp [" << tp->sub << ", " << tp->pred << ", " << tp->obj << "]" << endl;
							return false;
						} else {
							cout << "0 results5.1 for tp [" << tp->sub << ", " << tp->pred << ", " << tp->obj << "]" << endl;
							null_perm.insert(tp->strlevel);
							tp->triplecnt = 0;
							tp->bitmat.num_triples = 0;
						}
					}

				} else {
					//:s ?p ?o
					//Case S
					//Load PSO bitmat
					char dumpfile[1024];
					sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_PSO")].c_str());

//					unsigned long offset = get_offset(dumpfile, tp->sub);
					
//					cout << "Loading from dumpfile5 " << dumpfile << " for " << tp->sub << " " << tp->pred << " " << tp->obj << endl;

					init_bitmat(&tp->bitmat, gnum_preds, gnum_subs, gnum_objs, gnum_comm_so, PSO_BITMAT);

					if (wrapper_load_from_dump_file(dumpfile, tp->sub, &graph[MAX_JVARS_IN_QUERY+i], true, true) == 0) {
						if (tp->strlevel.find("s") == string::npos) {
							cout << "0 results6 for tp [" << tp->sub << ", " << tp->pred << ", " << tp->obj << "]" << endl;
							return false;
						} else {
							cout << "0 results6.1 for tp [" << tp->sub << ", " << tp->pred << ", " << tp->obj << "]" << endl;
							null_perm.insert(tp->strlevel);
							tp->triplecnt = 0;
							tp->bitmat.num_triples = 0;
							continue;
						}
					}
					//Now populate objfold arr
					populate_objfold(&tp->bitmat);
				}
			} else {
				if (tp->obj > 0) {
					//?s ?p :o
					//Case O
					//Load POS bitmat
					char dumpfile[1024];
					sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_POS")].c_str());

//					unsigned long offset = get_offset(dumpfile, tp->obj);

//					cout << "Loading from dumpfile6 " << dumpfile << " for " << tp->sub << " " << tp->pred << " " << tp->obj << endl;

					init_bitmat(&tp->bitmat, gnum_preds, gnum_objs, gnum_subs, gnum_comm_so, POS_BITMAT);

					if (wrapper_load_from_dump_file(dumpfile, tp->obj, &graph[MAX_JVARS_IN_QUERY+i], true, true) == 0) {
						if (tp->strlevel.find("s") == string::npos) {
							cout << "0 results7 for tp [" << tp->sub << ", " << tp->pred << ", " << tp->obj << "]" << endl;
							return false;
						} else {
							cout << "0 results7.1 for tp [" << tp->sub << ", " << tp->pred << ", " << tp->obj << "]" << endl;
							null_perm.insert(tp->strlevel);
							tp->triplecnt = 0;
							tp->bitmat.num_triples = 0;
							continue;
						}
					}
					//Now populate objfold arr
					populate_objfold(&tp->bitmat);
				} else {
					//Case ?s ?p ?o
					cout << "******* ERROR -- ?s ?p ?o is not handled right now *********" << endl;
					exit(-1);
				}
			}
		}

	}

	return true;
}

///////////////////////////////////////////////////////////
/*
void spanning_tree(struct node *n, int curr_node)
{
	if (curr_node == graph_tp_nodes)
		return;

	n->color = BLACK;

	LIST *q_neighbor = n->nextTP;
	for (int i = 0; i < n->numTPs; i++) {

		if (q_neighbor->gnode->color == WHITE) {
			q_neighbor->gnode->color = BLACK;
			if (tree_edges_map.find(n) == tree_edges_map.end()) {
				vector<struct node *> q_n_nodes;
				q_n_nodes.push_back(q_neighbor->gnode);
				tree_edges_map[n] = q_n_nodes;
			} else {
				tree_edges_map[n].push_back(q_neighbor->gnode);
			}


			if (tree_edges_map.find(q_neighbor->gnode) == tree_edges_map.end()) {
				vector<struct node *> q_n_nodes2;
				q_n_nodes2.push_back(n);
				tree_edges_map[q_neighbor->gnode] = q_n_nodes2;
			} else {
				tree_edges_map[q_neighbor->gnode].push_back(n);
			}

			spanning_tree(q_neighbor->gnode, curr_node+1);
		}

		q_neighbor = q_neighbor->next;
	}

}
*/
////////////////////////////////////////////////////////////

bool update_graph(FILE *outfile)
{
	for (int i = 0; i < graph_tp_nodes; i++) {
		TP * tp = (TP*)graph[MAX_JVARS_IN_QUERY + i].data;
		if (tp->isSlaveOfAnyone(null_perm)) {
			null_perm.insert(tp->strlevel);
		}
	}

	if (isCycle || seljvars.size() == 0) {
		//all vars selected
		for (int i = 0; i < graph_var_nodes; i++) {
			if (graph[i].type == NON_JVAR_NODE) {
				continue;
			}
			JVAR *jvar = (JVAR*)graph[i].data;
			LIST *nextTP = graph[i].nextTP;
			map<string, vector<struct node *> > slavetps;
			map<string, vector<struct node *> > mastertps;

			for (; nextTP; nextTP = nextTP->next) {
				string level = ((TP*)nextTP->gnode->data)->strlevel;
				if (level.find("s") == string::npos || ((TP*)nextTP->gnode->data)->rollback) {
					mastertps[level].push_back(nextTP->gnode);
				} else {
					slavetps[level].push_back(nextTP->gnode);
				}
			}

			assert(mastertps.size() > 0 || slavetps.size() > 0);

//			bool morethanone = false;
			for (map<string, vector<struct node *> >::iterator it = mastertps.begin(); it != mastertps.end(); it++) {
				string level = (*it).first;
				vector<struct node *> lvltps = (*it).second;
				bool morethanone = false;

				for (vector<struct node *>::iterator vit = lvltps.begin(); vit != lvltps.end(); vit++) {
					TP *tp = (TP*)(*vit)->data;
					int vars = 0;
					if (tp->sub < 0)
						vars++;
					if (tp->pred < 0)
						vars++;
					if (tp->obj < 0)
						vars++;
					if (vars > 1) {
						(*vit)->isNeeded = true;
						morethanone = true;
					}
				}
				//None of the master TPs conn to this jvar node have > 1 vars
				//so select only one node for result generation
				if (!morethanone) {
//					vector<struct node *> lvltps = mastertps.begin()->second;
					struct node *tmpnode = lvltps[0];
					tmpnode->isNeeded = true;
				}
			}

//			//None of the master TPs conn to this jvar node have > 1 vars
//			//so select only one node for result generation
//			if (!morethanone && mastertps.size() > 0) {
//				vector<struct node *> lvltps = mastertps.begin()->second;
//				struct node *tmpnode = lvltps[0];
//				tmpnode->isNeeded = true;
//			}

			for (map<string, vector<struct node *> >::iterator it = slavetps.begin(); it != slavetps.end(); it++) {
				bool morethanone = false;
				string level = (*it).first;
				vector<struct node *> lvltps = (*it).second;
				for (vector<struct node *>::iterator vit = lvltps.begin(); vit != lvltps.end(); vit++) {
					TP *tp = (TP*)(*vit)->data;
					int vars = 0;
					if (tp->sub < 0)
						vars++;
					if (tp->pred < 0)
						vars++;
					if (tp->obj < 0)
						vars++;
					if (vars > 1) {
						(*vit)->isNeeded = true;
						morethanone = true;
					}
				}
				//For slave TPs, you have to select at least one at each
				//slave level
				if (!morethanone) {
					struct node *tmpnode = lvltps[0];
					tmpnode->isNeeded = true;
				}
			}

		}

	}

	////////////
	//DEBUG : remove later
	/////////////

//	cout << "update_graph: needed nodes -------> " << endl;
//
//	for (int i = 0; i < graph_tp_nodes; i++) {
//		if (!graph[MAX_JVARS_IN_QUERY + i].isNeeded)
//			continue;
//		TP *tp = (TP*)graph[MAX_JVARS_IN_QUERY + i].data;
//		cout << tp->toString() << endl;
//	}
	////////////////////////////////////////////

	//remove the nodes from linked lists
	//So that you don't go over them again and again
	//in the match_query_graph function
	for (int i = 0; i < graph_tp_nodes; i++) {
		if (!graph[MAX_JVARS_IN_QUERY + i].isNeeded)
			continue;
		LIST *next = graph[MAX_JVARS_IN_QUERY + i].nextTP;
		LIST *prev = NULL, *tmp = NULL;
		while (next) {
			if (!next->gnode->isNeeded) {
				if (prev) {
					prev->next = next->next;
				} else {
					graph[MAX_JVARS_IN_QUERY + i].nextTP = next->next;
				}
				tmp = next;
				next = next->next;
				free(tmp);
				graph[MAX_JVARS_IN_QUERY + i].numTPs--;
			} else {
				prev = next;
				next = next->next;
			}
		}
	}


//	print_graph();
	//TODO: if performance suffers, omit this.
	for (int i = 0; i < graph_tp_nodes; i++) {
		if (graph[MAX_JVARS_IN_QUERY + i].isNeeded) {
			continue;
		}
		TP *tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;

//		cout << "Deleting unwanted BitMat [" << tp->sub << " " << tp->pred << " " << tp->obj << "]" << endl;

		assert(tp != NULL);

		tp->bitmat.freebm();
		tp->triplecnt = 0;

		LIST *adjtp = graph[MAX_JVARS_IN_QUERY + i].nextTP;

		while (adjtp) {
			LIST *tmp = adjtp->next;
			free(adjtp);
			adjtp = tmp;
		}

		graph[MAX_JVARS_IN_QUERY + i].nextTP = NULL;

		graph[MAX_JVARS_IN_QUERY + i].numTPs = 0;
	}
	return false;

	//	else if (seljvars.size() == 1 && selvars.size() > 1) {
	//
	//		assert(0);
	//		//NOTE: it cannot happen in this case that jvarsitr2.size > seljvars.size() because
	//		//then that implies that you have selected non-jvars from different TP nodes
	//		//having other jvars.. but if that was the case, appropriate jvars would have been
	//		//selected in seljvars while parsing the graph
	//		for (int i = 0; i < graph_tp_nodes; i++) {
	//			TP *tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
	//			if (tp->numvars == 1) {
	//				//This TP has only a VAR.. so no need to use that
	////				graph[MAX_JVARS_IN_QUERY + i].isNeeded = false;
	//				continue;
	//			}
	////			map<int, struct node *>::iterator mit;
	//			if (tp->sub < 0 && tp->sub < MAX_JVARS && selvars.find(tp->sub) != selvars.end()) {
	//				graph[MAX_JVARS_IN_QUERY + i].isNeeded = true;
	//				continue;
	//			}
	//			if (tp->pred < 0 && tp->pred < MAX_JVARS && selvars.find(tp->pred) != selvars.end()) {
	//				graph[MAX_JVARS_IN_QUERY + i].isNeeded = true;
	//				continue;
	//			}
	//			if (tp->obj < 0 && tp->obj < MAX_JVARS && selvars.find(tp->obj) != selvars.end()) {
	//				graph[MAX_JVARS_IN_QUERY + i].isNeeded = true;
	//				continue;
	//			}
	//
	//		}
	//
	//	} else if (seljvars.size() == 1 && selvars.size() == 1) {
	//
	//		assert(0);
	//		//You should enumerate results here itself and not go to
	//		//the match_query phase if the graph is ACYCLIC.
	//		//Doesn't matter if selvar is different from seljvar
	//		//You need only one BM to enumerate results
	//
	//		cout << "Came here1 " << (*seljvars.begin()).first << endl;

	//		unsigned char *resarr = NULL; unsigned int resarr_size = 0;
	//
	//		if ((*seljvars.begin()).first == (*selvars.begin()).first) {
	//			//Simply enumerate from the joinres
	////			cout << "Came here2" << endl;
	//
	//			JVAR *jvar = (JVAR *)((*seljvars.begin()).second)->data;
	//			assert(jvar->nodenum == (*seljvars.begin()).first);
	//			resarr = jvar->joinres; resarr_size = jvar->joinres_size;
	//
	//			unsigned int count = 1;
	//			for (unsigned int i = 0; i < resarr_size; i++) {
	//				if (resarr[i] == 0x00) {
	//					count += 8;
	//					continue;
	//				}
	//				for (unsigned short j = 0; j < 8; j++, count++) {
	//					if (resarr[i] & (0x80 >> j)) {
	//						fprintf(outfile, "%u\n", count);
	//					}
	//				}
	//			}
	//
	//			return true;
	//
	//		}
	//
	//		assert(((*selvars.begin()).second)->numTPs == 1);
	//
	//		TP *tp = (TP *)((*selvars.begin()).second)->nextTP->gnode->data;
	//		((*selvars.begin()).second)->nextTP->gnode->isNeeded = true;
	//
	////		for (int i = 0; i < graph_tp_nodes; i++) {
	////			tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
	////			//TODO: possibility for optimization --
	////			//select only the tpnode having only that jvar and other positions
	////			//bound
	////			if (tp->sub == (*selvars.begin()).first) {
	////				graph[MAX_JVARS_IN_QUERY + i].isNeeded = true;
	////				break;
	////			} else if (tp->pred == (*selvars.begin()).first) {
	////				graph[MAX_JVARS_IN_QUERY + i].isNeeded = true;
	////				break;
	////			} else if (tp->obj == (*selvars.begin()).first) {
	////				graph[MAX_JVARS_IN_QUERY + i].isNeeded = true;
	////				break;
	////			}
	////		}
	//
	//		//The VAR and JVAR MUST be in same TP otherwise the protocol is wrong
	//		JVAR *jvar = (JVAR *)(*seljvars.begin()).second->data;
	//
	//		cout << "Unfolding " << jvar->nodenum  << " on " << tp->sub << ":" << tp->pred << ":" << tp->obj << endl;
	//
	//		if (tp->sub == jvar->nodenum) {
	//			subject_dim_unfold(jvar, tp, true);
	//			tp->unfolded = true;
	//		} else if (tp->pred == jvar->nodenum) {
	//			predicate_dim_unfold(jvar, tp, true);
	//			tp->unfolded = true;
	//		} else if (tp->obj == jvar->nodenum) {
	//			object_dim_unfold(jvar, tp, true);
	//			tp->unfolded = true;
	//		} else
	//			assert(0);
	//
	//
	//		//Now fold the BM and enumerate results
	////		TP *tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
	//
	//		if (tp->sub == (*selvars.begin()).first) {
	//			if (tp->bitmat.dimension == SPO_BITMAT) {
	//				//Fold by retaining subject dimension
	//				simple_fold(&tp->bitmat, SUB_DIMENSION, tp->bitmat.subfold);
	//				resarr = tp->bitmat.subfold; resarr_size = tp->bitmat.subject_bytes;
	//			} else if (tp->bitmat.dimension == OPS_BITMAT) {
	//				simple_fold(&tp->bitmat, OBJ_DIMENSION, tp->bitmat.objfold);
	//				resarr = tp->bitmat.objfold; resarr_size = tp->bitmat.object_bytes;
	//			} else
	//				assert(0);
	//
	//		} else if (tp->pred == (*selvars.begin()).first) {
	//			if (tp->bitmat.dimension == PSO_BITMAT || tp->bitmat.dimension == POS_BITMAT) {
	//				simple_fold(&tp->bitmat, SUB_DIMENSION, tp->bitmat.subfold);
	//				resarr = tp->bitmat.subfold; resarr_size = tp->bitmat.subject_bytes;
	//			} else
	//				assert(0);
	//		} else if (tp->obj == (*selvars.begin()).first) {
	//			if (tp->bitmat.dimension == SPO_BITMAT) {
	//				//Fold by retaining subject dimension
	//				simple_fold(&tp->bitmat, OBJ_DIMENSION, tp->bitmat.objfold);
	//				resarr = tp->bitmat.objfold; resarr_size = tp->bitmat.object_bytes;
	//			} else if (tp->bitmat.dimension == OPS_BITMAT) {
	//				simple_fold(&tp->bitmat, SUB_DIMENSION, tp->bitmat.subfold);
	//				resarr = tp->bitmat.subfold; resarr_size = tp->bitmat.subject_bytes;
	//			} else
	//				assert(0);
	//
	//		}
	//
	//		unsigned int count = 1;
	//		for (unsigned int i = 0; i < resarr_size; i++) {
	//			if (resarr[i] == 0x00) {
	//				count += 8;
	//				continue;
	//			}
	//			for (unsigned short j = 0; j < 8; j++, count++) {
	//				if (resarr[i] & (0x80 >> j)) {
	//					fprintf(outfile, "%u\n", count);
	//				}
	//			}
	//		}
	//
	//		return true;
	//
	//	} else if (seljvars.size() > 1) {
	//
	//		assert(0);
	//		//NOTE: here it can happen that jvarsitr2.size > seljvars.size()
	//		for (int i = 0; i < graph_tp_nodes; i++) {
	//			TP *tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
	//
	//			if (tp->numvars == 1)
	//				continue;
	//
	//			if (tp->sub < 0 && tp->sub < MAX_JVARS && selvars.find(tp->sub) != selvars.end()) {
	//				graph[MAX_JVARS_IN_QUERY + i].isNeeded = true;
	//				continue;
	//			}
	//			if (tp->pred < 0 && tp->pred < MAX_JVARS && selvars.find(tp->pred) != selvars.end()) {
	//				graph[MAX_JVARS_IN_QUERY + i].isNeeded = true;
	//				continue;
	//			}
	//			if (tp->obj < 0 && tp->obj < MAX_JVARS && selvars.find(tp->obj) != selvars.end()) {
	//				graph[MAX_JVARS_IN_QUERY + i].isNeeded = true;
	//				continue;
	//			}
	//
	//			int cnt = 0;
	////			for (int j = 0; j < graph_var_nodes; j++)
	//			for (map<int, struct node *>::iterator itr = seljvars.begin(); itr != seljvars.end(); itr++) {
	//				//When you come here jvarsitr2 has already been updated through
	//				//get_limited_subtree called in prune_triples_new
	////				if (jvarsitr2[j] == NULL)
	////					break;
	//
	//				JVAR *jvar = (JVAR *)itr->second->data;
	//				if (tp->sub == jvar->nodenum || tp->pred == jvar->nodenum || tp->obj == jvar->nodenum) {
	//					cnt++;
	//				}
	//			}
	//			if (cnt > 1) {
	//				graph[MAX_JVARS_IN_QUERY + i].isNeeded = true;
	//				continue;
	//			}
	//
	//		}
	//
	//	}

//	cout << "Updating all VARS joinres to make sure they are populated" << endl;
	//////////////////
	//TODO: Right now it's a dead code
	/////////////////

//	for (map<int, struct node *>::iterator it = selvars.begin(); it != selvars.end(); it++) {
//		if ((*it).second->type == JVAR_NODE) {
//			continue;
//		}
//
//		//You may have to unfold first
//
//		cout << "update_graph: Populating non-jvar variable's joinres" << endl;
//
//		assert((*it).second->numTPs == 1);
//
//		JVAR *jvar = (JVAR *)((*it).second)->data;
//		TP *tp = (TP *)((*it).second)->nextTP->gnode->data;
//
//		assert(tp->unfolded == false);
//
//		//NOTE: Each TP MUST have at least one jvar
//		//NO TP can have all vars
//		//Hence if a TP has one non-jvar out of the remaining two positions
//		// -> One MUST be jvar
//		// -> The other one MUST be a fixed val.
//		if (!tp->unfolded) {
//			//This non-var node's adj TP's adjvar.. and among that we want to find a jvar
//			LIST *nextj = ((*it).second)->nextTP->gnode->nextadjvar;
//			while (nextj) {
//				if (nextj->gnode->type == JVAR_NODE)
//					break;
//				nextj = nextj->next;
//			}
//			if (tp->sub == ((JVAR *)nextj->gnode->data)->nodenum) {
//				//SUB MUST be a jvar
//				subject_dim_unfold((JVAR *)nextj->gnode->data, tp, true);
//				tp->unfolded = true;
//			} else if (tp->pred == ((JVAR *)nextj->gnode->data)->nodenum) {
//				predicate_dim_unfold((JVAR *)nextj->gnode->data, tp, true);
//				tp->unfolded = true;
//			} else if (tp->obj == ((JVAR *)nextj->gnode->data)->nodenum) {
//				object_dim_unfold((JVAR *)nextj->gnode->data, tp, true);
//				tp->unfolded = true;
//			} else
//				assert(0);
//		}
//
//		if (resbitvec) {
//			cout << "Folding to populate joinres bitarr for non-jvar.. needed only for resbitvec" << endl;
//
//			if ((tp->sub == jvar->nodenum && tp->bitmat.dimension == SPO_BITMAT)
//				|| (tp->pred == jvar->nodenum && (tp->bitmat.dimension == PSO_BITMAT || tp->bitmat.dimension == POS_BITMAT))
//				|| (tp->obj == jvar->nodenum && tp->bitmat.dimension == OPS_BITMAT)
//					) {
//					
//				//Fold by retaining subject dimension
//				simple_fold(&tp->bitmat, SUB_DIMENSION, tp->bitmat.subfold);
//				jvar->joinres = (unsigned char *) malloc (tp->bitmat.subject_bytes);
//				memcpy(jvar->joinres, tp->bitmat.subfold, tp->bitmat.subject_bytes);
//			} else if (
//			 (tp->sub == jvar->nodenum && tp->bitmat.dimension == OPS_BITMAT)
//			 || (tp->obj == jvar->nodenum && tp->bitmat.dimension == SPO_BITMAT)) {
//				simple_fold(&tp->bitmat, OBJ_DIMENSION, tp->bitmat.objfold);
//				jvar->joinres = (unsigned char *) malloc (tp->bitmat.object_bytes);
//				memcpy(jvar->joinres, tp->bitmat.objfold, tp->bitmat.object_bytes);
//			} else
//				assert(0);
//		}
//
//	}
////////////////////////////////////////////////////////////////////
	//TODO: Right now it's a dead code

//	if (resbitvec) {
//		cout << "Now populating distresult" << endl;
//
//		distresult_size = 1;
//
//		for (map<int, struct node *>::iterator it = selvars.begin(); it != selvars.end(); it++) {
//			unsigned char *joinres = ((JVAR *)(*it).second->data)->joinres;
//			unsigned int joinres_size = ((JVAR *)(*it).second->data)->joinres_size;
//
//			unsigned int i = 0;
//
//			for (i = 0; i < joinres_size; i++) {
//				if (joinres[i] != 0x00) {
//					break;
//				}
//			}
//
//			unsigned short cnt = 0;
//			for (cnt = 0; cnt < 8; cnt++) {
//				if (joinres[i] & (0x80 >> cnt))
//					break;
//			}
//
//			((JVAR *)(*it).second->data)->lowerlim = (8*i) + cnt + 1;
//
//			for (i = joinres_size - 1; i >= 0; i--) {
//				if (joinres[i] != 0x00)
//					break;
//			}
//			cnt = 0;
//			for (cnt = 7; cnt >= 0; cnt--) {
//				if (joinres[i] & (0x80 >> cnt))
//					break;
//			}
//			((JVAR *)(*it).second->data)->upperlim = (8*i) + cnt + 1;
//
//			((JVAR *)(*it).second->data)->range = ((JVAR *)(*it).second->data)->upperlim - ((JVAR *)(*it).second->data)->lowerlim + 1;
//
//
//			distresult_size *= ((JVAR *)(*it).second->data)->range;
//
//			cout << "Range lower= " << ((JVAR *)(*it).second->data)->lowerlim << " upper= "
//				<< ((JVAR *)(*it).second->data)->upperlim << " range=" << ((JVAR *)(*it).second->data)->range << 
//				" distresult_size= " << distresult_size << endl;
//		}
//
//		if (selvars.size() > 0) {
//			distresult_size = (distresult_size/8) + 1;
//
//			assert(distresult_size < 0x6400000);
//			//TODO: you have to switch over to hashtable here
//
//		//	distresult_size = gsubject_bytes >gobject_bytes ? gsubject_bytes: gobject_bytes;
//
//			cout << "Distresult_size = " << distresult_size << endl;
//
//			distresult = (unsigned char *) malloc(distresult_size);
//
//			memset(distresult, 0, distresult_size);
//		}
//	}
////////////////////////////////////////////////////////////////////
}

///////////////////////////////////////////////////////////////////
// Deletes connections between any two TPs not in a master-slave
// relationship, if the common var between them also appears in one
// or both of their masters
//////////////////////////////////////////////////////////////////
bool update_graph_for_final()
{
	//Sanity check.. ensure that any null_perm level joining
	//with disjoint level is consistent

	for (int i = 0; i < graph_tp_nodes; i++) {
		if (!graph[MAX_JVARS_IN_QUERY + i].isNeeded) {
			continue;
		}
		LIST *next = graph[MAX_JVARS_IN_QUERY + i].nextTP;
		TP *tp1 = (TP*)graph[MAX_JVARS_IN_QUERY + i].data;
		while (next) {
			TP *tp2 = (TP*)next->gnode->data;
			if (!tp1->isSlave(tp2->strlevel) && !tp2->isSlave(tp1->strlevel)) {
				if ((null_perm.find(tp1->strlevel) != null_perm.end() &&
						null_perm.find(tp2->strlevel) == null_perm.end())
						||
					(null_perm.find(tp2->strlevel) != null_perm.end() &&
						null_perm.find(tp1->strlevel) == null_perm.end())
				) {
					null_perm.insert(tp1->strlevel);
					null_perm.insert(tp2->strlevel);
					if (tp1->rollback || tp2->rollback)
						return false;
				}
			}
			next = next->next;
		}
	}
	return true;
}
///////////////////////////////////////////////////////

vector<struct node*> rank_master_nodes()
{
//	cout << "Inside rank nodes" << endl;
	unsigned int min_triples = 0;
	vector<struct node *> rankvec;

//	cout << "null_perm set -- " << endl;
//
//	for (set<string>::iterator it = null_perm.begin(); it != null_perm.end(); it++) {
//		cout << *it << " ";
//	}
//	cout << endl;

	for (int i = 0; i < graph_tp_nodes; i++) {

		TP *tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
		string tplevel = tp->strlevel;

		if (!graph[MAX_JVARS_IN_QUERY + i].isNeeded || tplevel.find("s") < tplevel.size())
			continue;

		//this should never happen. this should get detected during prune_for_jvar first round
		if (null_perm.find(tplevel) != null_perm.end())
			assert(0);

		tp->triplecnt = tp->bitmat.num_triples;// count_triples_in_bitmat(&tp->bitmat);
		assert(tp->triplecnt);

		rankvec.push_back(&graph[MAX_JVARS_IN_QUERY + i]);
	}

	stable_sort(rankvec.begin(), rankvec.end(), sort_master_tp_nodes);

	return rankvec;

}

///////////////////////////////////////////////////////
struct retval rank_slave_nodes(string level)
{
	//count_triples_in_bitmat on all the TPs and sort them.
	//then call "match_query" on the Query node has least
	//num of triples associated with it
	unsigned int min_triples = 0;
	struct node *start_q_node = NULL;
	bool first = true;

//	cout << "HERE " << level << endl;
	struct retval ret = {NULL, false};

	for (int i = 0; i < graph_tp_nodes; i++) {
		
		TP *tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
		string tplevel = tp->strlevel;

		if (!graph[MAX_JVARS_IN_QUERY + i].isNeeded || tplevel != level)
			continue;

		tp->triplecnt = tp->bitmat.num_triples;// count_triples_in_bitmat(&tp->bitmat);

		bool conn_nbr = false;

		LIST *nbr = graph[MAX_JVARS_IN_QUERY + i].nextTP;
		for (; nbr ; nbr = nbr->next) {
			if (nbr->gnode->color == BLACK) {
				conn_nbr = true;
				break;
			}
		}

		if (!conn_nbr)
			continue;

		assert(conn_nbr);

		if (first && conn_nbr) {
			min_triples = tp->bitmat.num_triples;
			start_q_node = &graph[MAX_JVARS_IN_QUERY + i];
			first = false;
		} else if (min_triples > tp->triplecnt && conn_nbr) {
			min_triples = tp->bitmat.num_triples;
			start_q_node = &graph[MAX_JVARS_IN_QUERY + i];
		}
		ret.q_node = start_q_node;
		ret.atLeastOne = true;
		if (min_triples == 0 && conn_nbr) {
			ret.q_node = NULL;
			ret.atLeastOne = true;
			return ret;
		}
	}
	return ret;

}
////////////////////////////////////////////////////////////
void tree_bfs(struct node *n)
{
	int cnt = 1;
	queue<struct node *> bfs_nodes;
	map<struct node *, int> nodes_map;
	set<struct node *> n_same_lev;

	bfs_nodes.push(n);
	nodes_map[n] = 1;

	while(!bfs_nodes.empty()) {

		struct node *elem = bfs_nodes.front();
		bfs_nodes.pop();
		vector<struct node *> neighbors = tree_edges_map[elem];
		bool leaf = true;

//		cout << "tree_bfs: printing root node info " << endl; 
//		print_node_info(elem);
		for (int i = 0; i < neighbors.size(); i++) {
//			cout << "tree_bfs: printing neighbor node info " << endl;
//			print_node_info(neighbors[i]);
			if (nodes_map.find(neighbors[i]) == nodes_map.end()) {
//				cout << "tree_bfs: printing unvisited neighbor info " << endl;
//				print_node_info(neighbors[i]);
				//This node has not been visited yet
				nodes_map[neighbors[i]] = 1;
				//Do not order here.. collect nodes and do ordering in
				//the ones at the same level
				n_same_lev.insert(neighbors[i]);
//				jvarsitr2[cnt] = neighbors[i];
//				cnt++;
				bfs_nodes.push(neighbors[i]);
				//If you find at least one neighbor
				//which is unvisited then it's not a leaf node
				leaf = false;
			}
		}

		//Now relist in the increasing order of #of triples
		assert(graph_tp_nodes == gtpnodes.size());

		for (vector<struct node *>::iterator itr = gtpnodes.begin(); itr != gtpnodes.end() && n_same_lev.size() > 0; itr++) {
			struct node *tpnode = (*itr);

			LIST *adjvar = tpnode->nextadjvar;

			for (; adjvar && n_same_lev.size() > 0; adjvar = adjvar->next) {
				struct node *jvarn = adjvar->gnode;
				if (n_same_lev.find(jvarn) != n_same_lev.end()) {
					jvarsitr2[cnt] = jvarn;
					cnt++;
					n_same_lev.erase(jvarn);
				}
			}
		}

		if (leaf) {
//			cout << "Added to leaf node" << endl;
			leaf_nodes.push_back(elem);
		}
	}

}
///////////////////////////////////////
map<struct node *, set<struct node *> > tree_edges;
bool cycle_detect(struct node *node)
{
	int i;
	struct node *gnode = NULL;
	LIST *next = NULL;
	
//	cout << "cycle_detect: nodenum " << ((JVAR *)node->data)->nodenum << endl;

	node->color = GREY;

	next = node->nextadjvar;

	for (i = 0; i < node->numadjvars; i++) {

		gnode = next->gnode;

		if (gnode->color == GREY) {
			if (tree_edges[gnode].find(node) == tree_edges[gnode].end())
				return true;

//			if (gnode->parent != NULL) {
//				JVAR *jvar2 = (JVAR *)gnode->parent->data;
//				JVAR *jvar = (JVAR *)node->data;
//				cout << "jvar->nodenum " << jvar->nodenum << " jvar2->nodenum " << jvar2->nodenum << endl;
//				if (jvar->nodenum != jvar2->nodenum)
//					return true;
//			}
		} else if (gnode->color == WHITE && gnode->type == JVAR_NODE) {
			tree_edges[node].insert(gnode) ;
			tree_edges[gnode].insert(node) ;
//			gnode->parent = node;
			if (cycle_detect(gnode))
				return true;
		}

		next = next->next;
	}
	node->color = BLACK;
	return false;
}

//////////////////////////////////////////////////
//TODO: fix for OPTIONAL queries
void build_jvar_tree(bool best_match_reqd, bool iscylic, bool allslaves_one_jvar)
{
	struct node *tp_min = NULL, *tp_min2 = NULL, *jvartmp = NULL;
	unsigned int min_triples = 0;
	int jvarcnt = 0;

	for (int i = 0; i < graph_var_nodes; i++) {
		graph[i].color = WHITE;
		if (jvartmp == NULL && graph[i].type == JVAR_NODE) {
			jvartmp = &graph[i];
		}
	}

	//Can be ommitted as it doesn't make any difference
	//TODO: Cycle detect should happen on the hypergraph and not
	//jvar graph
	isCycle = cycle_detect(jvartmp);

//	isCycle = true;

//	cout << "isCycle " << isCycle << endl;

//	if (selvars.size() > 0) {
//	cout << "selvars.size " << selvars.size() << endl;
//	cout << "seljvars.size " << seljvars.size() << endl;
	//Order TP nodes acc to #triples in them
	///////
	//DEBUG
	//////

//	cout << "**** GTPnodes sorted ****" << endl;
//	for (int i = 0; i < gtpnodes.size(); i++) {
//		TP *tpnode = (TP *)gtpnodes[i]->data;
//		cout << tpnode->sub << " " << tpnode->pred << " " << tpnode->obj 
//		<< 	" " << tpnode->bitmat.num_triples << " " << tpnode->bitmat.bm.size() << endl;
////		assert(tp-node->bitmat.bm.size() > 0);
//	}

	if (selvars.size() == 1 && !isCycle) {
		if (selvars.begin()->second->type == JVAR_NODE)
			jvarsitr2[0] = selvars.begin()->second;
		else {
			struct node *var = (struct node *)selvars.begin()->second;
			LIST *nextadjvar = var->nextadjvar;
			while (nextadjvar->gnode->type == NON_JVAR_NODE) {
				nextadjvar = nextadjvar->next;
			}
			seljvars[((JVAR *)nextadjvar->gnode->data)->nodenum] = nextadjvar->gnode;
			jvarsitr2[0] = nextadjvar->gnode;
		}
//		cout << "Root of the tree " << ((JVAR *)jvarsitr2[0]->data)->nodenum << endl;
		spanning_tree_bfs(jvarsitr2[0], JVAR_NODE);
//		print_spanning_tree(JVAR_NODE);
		tree_bfs(jvarsitr2[0]);

		////////
		//DEBUG
		////////
//		cout << "**** jvarsitr2 ****" << endl;
//		for (int i = 0; i < graph_jvar_nodes; i++) {
//			cout << ((JVAR *)jvarsitr2[i]->data)->nodenum << endl;
//		}

		return;

	} else {
		//Can be mix of jvars and non-jvars
		//Designate the maximal jvar as the root.. so that
		//you can do a bottomup pass
		for (int i = 0; i < graph_tp_nodes; i++) {
			gtpnodes.push_back(&graph[i + MAX_JVARS_IN_QUERY]);
		}

		for (map<int, struct node *>::iterator it = selvars.begin(); it != selvars.end(); it++) {
			if (it->second->type == JVAR_NODE) {
				seljvars[it->first] = it->second;
			} else {
				struct node *var = (struct node *)it->second;
				LIST *nextadjvar = var->nextadjvar;
				while (nextadjvar->gnode->type == NON_JVAR_NODE) {
					nextadjvar = nextadjvar->next;
				}
				seljvars[((JVAR *)nextadjvar->gnode->data)->nodenum] = nextadjvar->gnode;
			}
		}
//		if (seljvars.size() == 0 || isCycle) {

		vector<struct node *> p = spanning_tree_jvar();

//		cout << "After spanning_tree_jvar" << endl;
//
//		for (vector<struct node *>::iterator it = p.begin(); it != p.end(); it++) {
//			cout << ((JVAR *)(*it)->data)->nodenum << endl;
//		}

		assert(p.size() == graph_jvar_nodes);

		if (best_match_reqd || isCycle || allslaves_one_jvar) {
			int i = 0;

			for (vector<struct node *>::iterator it = p.begin(); it != p.end(); it++, i++) {
				jvarsitr2[i] = *it;
			}
		} else {
			//You need to take special care of ensuring bottom-up pass.
//			cout << "build_jvar_tree: ********** Doing special processing" << endl;
			set<struct node *> absmaster_jvars;

			for (map<string, set<struct node *>>::iterator jit = leveljvars.begin(); jit != leveljvars.end(); jit++) {
				if ((*jit).first.find("s") != string::npos)
					continue;

				set<struct node *> jset = (*jit).second;
				absmaster_jvars.insert(jset.begin(), jset.end());
			}

			///////////////////////
			///DEBUG
//			for (set<struct node *>::iterator jit = absmaster_jvars.begin(); jit != absmaster_jvars.end(); jit++) {
//				cout << "abs master jvar " << ((JVAR *)(*jit)->data)->nodenum << endl;
//			}
			///////////////////////

			struct node *first = NULL;

			for (vector<struct node *>::iterator it = p.begin(); it != p.end() && absmaster_jvars.size() > 1; it++) {
				struct node *curr = *it;
//				cout << "curr " << ((JVAR *)(*it)->data)->nodenum << endl;
				if (absmaster_jvars.find(curr) != absmaster_jvars.end()) {
					int absm = 0;
					for (LIST *nextadjvar = curr->nextadjvar; nextadjvar; nextadjvar = nextadjvar->next) {
						//Check if curr has only ONE abs master jvar conn to it
						//all_levels are ones in which the neighbor jvar appears
						set<string> all_levels = jvarlevels[nextadjvar->gnode];
						for (set<string>::iterator sit = all_levels.begin(); sit != all_levels.end(); sit++) {
							if ((*sit).find("s") == string::npos)
								absm++;
						}
					}
					if (absm == 1) {
						absmaster_jvars.erase(curr);
						first = curr;
						break;
					}
				}
			}

			if (first == NULL) {
				//meaning there is only one abs master jvar
				assert(absmaster_jvars.size() == 1);
				first = *(absmaster_jvars.begin());
				absmaster_jvars.clear();
			}

			vector<struct node *> desired_order;
			desired_order.push_back(first);

			while (absmaster_jvars.size() > 0) {
				for (vector<struct node *>::iterator it = p.begin(); it != p.end(); it++) {
					struct node *curr = *it;
					if (absmaster_jvars.find(curr) == absmaster_jvars.end())
						continue;

					//# of adj jvars that are already visited
					int visited_nbr = 0;
					//# of adj jvars that appear in abs masters
					int absm = 0;
					for (LIST *nextadjvar = curr->nextadjvar; nextadjvar; nextadjvar = nextadjvar->next) {
						set<string> all_levels = jvarlevels[nextadjvar->gnode];
						for (set<string>::iterator sit = all_levels.begin(); sit != all_levels.end(); sit++) {
							if ((*sit).find("s") == string::npos)
								absm++;
						}
					}

					for (LIST *nextadjvar = curr->nextadjvar; nextadjvar; nextadjvar = nextadjvar->next) {
						//Check if the neighbor is already listed in desired_order.
						if (find(desired_order.begin(), desired_order.end(), nextadjvar->gnode) == desired_order.end())
							continue;

						set<string> all_levels = jvarlevels[nextadjvar->gnode];
						for (set<string>::iterator sit = all_levels.begin(); sit != all_levels.end(); sit++) {
							if ((*sit).find("s") == string::npos)
								visited_nbr++;
						}
					}

					if (visited_nbr >= absm - 1) {
						desired_order.push_back(curr);
						absmaster_jvars.erase(curr);
					}
				}
			}

			//////////////////
			//DEBUG
//			cout << "Desired order " << desired_order.size() << " ---------->" << endl;
//			for (vector<struct node *>::iterator it = desired_order.begin(); it != desired_order.end(); it++) {
//				assert(*it != NULL);
//				cout << ((JVAR *)(*it)->data)->nodenum << endl;
//			}
//			cout << "Desired order done" << endl;
			///////////////////////////

			for (map<string, set<struct node *>>::iterator lit = leveljvars.begin();
																lit != leveljvars.end(); lit++) {
				if ((*lit).first.find("s") == string::npos)
					continue;

				set<struct node *> jset = (*lit).second;
				set<struct node *> slave_jvars;
				slave_jvars.insert(jset.begin(), jset.end());

				/////////////////
				//DEBUG
//				cout << "Slave_jvars --------->" << endl;
//				for (set<struct node *>::iterator sit = slave_jvars.begin(); sit != slave_jvars.end(); sit++) {
//					cout << ((JVAR *)(*sit)->data)->nodenum << endl;
//				}
				////////////////

				for (vector<struct node *>::reverse_iterator rit = desired_order.rbegin(); rit != desired_order.rend(); rit++) {
					if (slave_jvars.find(*rit) != slave_jvars.end()) {
						first = *rit;
						slave_jvars.erase(*rit);
						break;
					}
				}

				assert(first != NULL);

				vector<struct node *> slave_jorder;
				slave_jorder.push_back(first);

				while (slave_jvars.size() > 0) {
					for (vector<struct node *>::reverse_iterator rit = p.rbegin(); rit != p.rend(); rit++) {
						struct node *curr = *rit;
						if (slave_jvars.find(curr) == slave_jvars.end())
							continue;

						for (LIST *nextadjvar = curr->nextadjvar; nextadjvar; nextadjvar = nextadjvar->next) {
							//Check if the neighbor is already listed in desired_order.
							if (find(slave_jorder.begin(), slave_jorder.end(), nextadjvar->gnode) != slave_jorder.end()) {
								slave_jorder.push_back(curr);
								slave_jvars.erase(curr);
							}
						}
					}
				}
				//Now slave_jorder has top-down pass, so reverse its order
				for (vector<struct node *>::reverse_iterator rit = slave_jorder.rbegin(); rit != slave_jorder.rend(); rit++) {
					if (find(desired_order.begin(), desired_order.end(), *rit) == desired_order.end()) {
						desired_order.push_back(*rit);
					}
				}
			}

			assert(desired_order.size() == graph_jvar_nodes);

			int i = 0;

			for (vector<struct node *>::iterator it = desired_order.begin(); it != desired_order.end(); it++, i++) {
				jvarsitr2[i] = *it;
			}
		}
//			cout << "*********** Printing jvarsitr2 queue ************" << endl;
//			for (int i = 0; i < graph_jvar_nodes; i++) {
//				print_node_info(jvarsitr2[i]);
//			}
//		cout << "Root of the tree " << ((JVAR *)jvarsitr2[0]->data)->nodenum << endl;

		////////
		//DEBUG
		////////
//		cout << "**** jvarsitr2 ****" << endl;
//		for (int i = 0; i < graph_jvar_nodes; i++) {
//			cout << ((JVAR *)jvarsitr2[i]->data)->nodenum << endl;
//		}
		return;

//		}
		//Reverse ordering
		sort(gtpnodes.begin(), gtpnodes.end(), sort_tp_nodes_rev);

		//Ensures that the code outside loop doesn't get executed.
		for (vector<struct node *>::iterator it = gtpnodes.begin(); it != gtpnodes.end(); it++) {
			map<int, struct node *>::iterator mit = seljvars.end();

			TP *tpnode = (TP *)(*it)->data;

			if (tpnode->sub < 0 && tpnode->sub > MAX_JVARS) {
				mit = seljvars.find(tpnode->sub);
			} 
			if (mit == seljvars.end() && tpnode->pred < 0 && tpnode->pred > MAX_JVARS) {
				mit = seljvars.find(tpnode->pred);
			}
			if (mit == seljvars.end() && tpnode->obj < 0 && tpnode->obj > MAX_JVARS) {
				mit = seljvars.find(tpnode->obj);
			}

			if (mit != seljvars.end()) {
				//Designate this jvar as the root node
				jvarsitr2[0] = mit->second;
				break;
			}

		}

	}

//	cout << "Root of the tree " << ((JVAR *)jvarsitr2[0]->data)->nodenum << endl;

	spanning_tree_bfs(jvarsitr2[0], JVAR_NODE);
	print_spanning_tree(JVAR_NODE);
	tree_bfs(jvarsitr2[0]);
	////////
	//DEBUG
	////////
//	cout << "**** jvarsitr2 ****" << endl;
//	for (int i = 0; i < graph_jvar_nodes; i++) {
//		cout << ((JVAR *)jvarsitr2[i]->data)->nodenum << endl;
//	}

////	if (DEBUG) {
//		cout << "*********** Printing jvarsitr2 queue ************" << endl;
//		for (int i = 0; i < graph_jvar_nodes; i++) {
//			print_node_info(jvarsitr2[i]);
//		}
////	}

////	if (DEBUG) {
//		cout << "********** Leaf nodes **********" << endl;
//		for (int i = 0; i < leaf_nodes.size(); i++) {
//			print_node_info(leaf_nodes[i]);
//		}
////	}

}
/////////////////////////////////////////////////////////
bool populate_all_tp_bitmats()
{
	gtpnodes.clear();
	for (int i = 0; i < graph_tp_nodes; i++) {
		gtpnodes.push_back(&graph[i + MAX_JVARS_IN_QUERY]);
	}
	stable_sort(gtpnodes.begin(), gtpnodes.end(), sort_tpnodes_jvar_tree);

	for (int j = 0; j < graph_tp_nodes; j++) {
		struct node *gnode = gtpnodes[j];
		TP *tp = (TP *)gnode->data;
		if (tp->bitmat.bm.size() > 0) {
//			cout << "Already loaded " << tp->toString() << endl;
			continue;
		}
		if (tp->isSlaveOfAnyone(null_perm)) {
//			for (set<string>::iterator sit = null_perm.begin(); sit != null_perm.end(); sit++) {
//				cout << *sit << " ";
//			}
//			cout << endl;
			tp->triplecnt = 0;
			tp->bitmat.num_triples = 0;
			continue;
		}

		for (int i = 0; i < graph_jvar_nodes; i++) {
			JVAR *jvar = (JVAR *)jvarsitr2[i]->data;
			if (tp->sub == jvar->nodenum) {

				//Load S-dimension BM
				char dumpfile[1024];
				sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_SPO")].c_str());

//				unsigned long offset = get_offset(dumpfile, tp->pred);

//				cout << "populate_all_tp_bitmats: Loading from dumpfile1 " << dumpfile << " for " << tp->sub << " " << tp->pred << " " << tp->obj << endl;
				init_bitmat(&tp->bitmat, gnum_subs, gnum_preds, gnum_objs, gnum_comm_so, SPO_BITMAT);
				if (wrapper_load_from_dump_file(dumpfile, tp->pred, gnode, true, true) == 0) {
					if (tp->strlevel.find("s") == string::npos) {
						cout << "0 results8 for tp [" << tp->sub << ", " << tp->pred << ", " << tp->obj << "]" << endl;
						return false;
					} else {
						cout << "0 results8.1 for tp [" << tp->sub << ", " << tp->pred << ", " << tp->obj << "]" << endl;
						null_perm.insert(tp->strlevel);
						tp->triplecnt = 0;
						tp->bitmat.num_triples = 0;
					}

				}
//				cout << "\nSleeping........" << endl;
//				sleep(10);
				break;
//					assert(count_bits_in_row(tp->bitmat.subfold, tp->bitmat.subject_bytes) != 0);
//					assert(count_bits_in_row(tp->bitmat.objfold, tp->bitmat.object_bytes) != 0);

			} else if (tp->obj == jvar->nodenum) {

				char dumpfile[1024];
				sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_OPS")].c_str());

				//Get the offset
//				unsigned long offset = get_offset(dumpfile, tp->pred);

//				cout << "populate_all_tp_bitmats: Loading from dumpfile2 " << dumpfile << " for " << tp->sub << " " << tp->pred << " " << tp->obj << endl;
				init_bitmat(&tp->bitmat, gnum_objs, gnum_preds, gnum_subs, gnum_comm_so, OPS_BITMAT);
				if (wrapper_load_from_dump_file(dumpfile, tp->pred, gnode, true, true) == 0) {
					if (tp->strlevel.find("s") == string::npos) {
						cout << "0 results9 for tp [" << tp->sub << ", " << tp->pred << ", " << tp->obj << "]" << endl;
						return false;
					} else {
						cout << "0 results9.1 for tp [" << tp->sub << ", " << tp->pred << ", " << tp->obj << "]" << endl;
						null_perm.insert(tp->strlevel);
						tp->triplecnt = 0;
						tp->bitmat.num_triples = 0;
					}

				}
//				cout << "\nSleeping........" << endl;
//				sleep(10);
				break;

			} else if (tp->pred == jvar->nodenum)
				assert(0);
		}
	}

	return true;

//	for (int i = graph_jvar_nodes - 1; i >= 0; i--) {
//		//Go over all adj TP nodes
//		//If one is unpopulated
//		//Populate optimally
//		JVAR *jvar = (JVAR *)jvarsitr2[i]->data;
//		LIST *nextTP = jvarsitr2[i]->nextTP;
//
//		for (; nextTP; nextTP = nextTP->next) {
//			TP *tp = (TP *)nextTP->gnode->data;
//			if (tp->bitmat.bm.size() > 0) {
//				continue;
//			}
//			if (tp->isSlaveOfAnyone(null_perm)) {
//				tp->triplecnt = 0;
//				tp->bitmat.num_triples = 0;
//				continue;
//			}
//
//			if (tp->sub == jvar->nodenum) {
//
//				//Load S-dimension BM
//				char dumpfile[1024];
//				sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_SPO")].c_str());
//
////				unsigned long offset = get_offset(dumpfile, tp->pred);
//
//				cout << "populate_all_tp_bitmats: Loading from dumpfile1 " << dumpfile << " for " << tp->sub << " " << tp->pred << " " << tp->obj << endl;
//				init_bitmat(&tp->bitmat, gnum_subs, gnum_preds, gnum_objs, gnum_comm_so, SPO_BITMAT);
//				if (wrapper_load_from_dump_file(dumpfile, tp->pred, nextTP->gnode, true, true) == 0) {
//					if (tp->strlevel.find("s") == string::npos) {
//						cout << "0 results8 for tp [" << tp->sub << ", " << tp->pred << ", " << tp->obj << "]" << endl;
//						return false;
//					} else {
//						cout << "0 results8.1 for tp [" << tp->sub << ", " << tp->pred << ", " << tp->obj << "]" << endl;
//						null_perm.insert(tp->strlevel);
//						tp->triplecnt = 0;
//						tp->bitmat.num_triples = 0;
//					}
//
//				}
////					assert(count_bits_in_row(tp->bitmat.subfold, tp->bitmat.subject_bytes) != 0);
////					assert(count_bits_in_row(tp->bitmat.objfold, tp->bitmat.object_bytes) != 0);
//
//			} else if (tp->obj == jvar->nodenum) {
//
//				char dumpfile[1024];
//				sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_OPS")].c_str());
//
//				//Get the offset
////				unsigned long offset = get_offset(dumpfile, tp->pred);
//
//				cout << "populate_all_tp_bitmats: Loading from dumpfile2 " << dumpfile << " for " << tp->sub << " " << tp->pred << " " << tp->obj << endl;
//				init_bitmat(&tp->bitmat, gnum_objs, gnum_preds, gnum_subs, gnum_comm_so, OPS_BITMAT);
//				if (wrapper_load_from_dump_file(dumpfile, tp->pred, nextTP->gnode, true, true) == 0) {
//					if (tp->strlevel.find("s") == string::npos) {
//						cout << "0 results9 for tp [" << tp->sub << ", " << tp->pred << ", " << tp->obj << "]" << endl;
//						return false;
//					} else {
//						cout << "0 results9.1 for tp [" << tp->sub << ", " << tp->pred << ", " << tp->obj << "]" << endl;
//						null_perm.insert(tp->strlevel);
//						tp->triplecnt = 0;
//						tp->bitmat.num_triples = 0;
//					}
//
//				}
//			} else if (tp->pred == jvar->nodenum)
//				assert(0);
////			nextTP = nextTP->next;
//		}
//	}
//
//	return true;

}
///////////////////////////
vector<struct node *> spanning_tree_jvar()
{
	stable_sort(gtpnodes.begin(), gtpnodes.end(), sort_tpnodes_jvar_tree);
	list<struct node *> ranklist(gtpnodes.begin(), gtpnodes.end());
//	gtpnodes.clear();
	queue<struct node *> bfsq;
	vector<struct node *> bfsvec;

	for (int i = 0; i < graph_tp_nodes; i++) {
		graph[MAX_JVARS_IN_QUERY + i].color = WHITE;
	}

//	cout << "spanning_tree_jvar --> gtpnodes" << endl;
//	for (int i = 0; i < graph_tp_nodes; i++) {
//		TP * tp = (TP*)gtpnodes[i]->data;
//		cout << tp->toString() << " " << tp->triplecnt << endl;
//	}
	//TODO: Fix here, the logic fails for well-designed queries with
	//multiple masters
	//////////////
	//New
	struct node *front = ranklist.front();
	front->color = BLACK;
	vector<struct node *> visitq;
	visitq.push_back(front);
	ranklist.pop_front();
	vector<int> auxbfsv;
	while (1) {
		struct node *nextn = NULL;
		//Find a node from ranklist such that one of its neighbors is 'visited' already
		for (list<struct node *>::iterator it = ranklist.begin(); it != ranklist.end(); ) {
			LIST *q_neighbor = (*it)->nextTP;
			for (; q_neighbor ; q_neighbor = q_neighbor->next) {
				if (q_neighbor->gnode->color == BLACK) {
					break;
				}
			}
			if (q_neighbor) {
				//nextn is a node which is not visited yet, but one of its
				//neighbors is visited.
				nextn = *it;
				it = ranklist.erase(it);
				break;
			}
			it++;
		}
		//No node exists such that it's in ranklist
		//and its neibour/s is/are visited.
		if (nextn == NULL) break;

		visitq.push_back(nextn);

		nextn->color = BLACK;
		TP *tp = (TP *)nextn->data;
		for (vector<struct node *>::iterator it = visitq.begin(); it != visitq.end(); it++) {
			LIST *q_neighbor = (*it)->nextTP;
			for (; q_neighbor ; q_neighbor = q_neighbor->next) {
				if (q_neighbor->gnode == nextn) {
					TP *tpn = (TP *)(*it)->data;
					if (tp->sub == tpn->sub || tp->sub == tpn->obj)
						auxbfsv.push_back(tp->sub);
					if (tp->pred == tpn->pred)
						auxbfsv.push_back(tp->pred);
					if (tp->obj == tpn->obj || tp->obj == tpn->sub)
						auxbfsv.push_back(tp->obj);
				}
			}
		}
	}

//	for (int i = 0; i < visitq.size(); i++) {
//		TP *tp = (TP *)visitq[i]->data;
//		cout << "tp "; tp->print(); cout << " -->";
//
//		LIST *q_neighbor = visitq[i]->nextTP;
//		for (; q_neighbor ; q_neighbor = q_neighbor->next) {
//		}
//
//
//		for (int j = i+1; j < visitq.size(); j++) {
//			LIST *q_neighbor = visitq[i]->nextTP;
//			for (; q_neighbor ; q_neighbor = q_neighbor->next) {
//				if (q_neighbor->gnode == visitq[j] && q_neighbor->gnode->color == BLACK) {
//					TP *tpn = (TP *)q_neighbor->gnode->data;
//					cout << " tpn "; tpn->print(); cout << endl;
//					if (tp->sub == tpn->sub || tp->sub == tpn->obj)
//						auxbfsv.push_back(tp->sub);
//					if (tp->pred == tpn->pred)
//						auxbfsv.push_back(tp->pred);
//					if (tp->obj == tpn->obj || tp->obj == tpn->sub)
//						auxbfsv.push_back(tp->obj);
//				}
//			}
//		}
//	}

//	//////////////
//	cout << "******1 *********";
//	for (vector<int>::iterator vit = auxbfsv.begin(); vit != auxbfsv.end(); vit++) {
//		cout << *vit << endl;
//	}
//	//////////////

	for (vector<int>::iterator it = auxbfsv.begin(); it != auxbfsv.end(); it++) {
		for (int i = 0; i < graph_var_nodes; i++) {
			JVAR *jvar = (JVAR *)graph[i].data;
			if (jvar->nodenum == *it) {
				bfsvec.push_back(&graph[i]);
			}
		}
	}

//	//////////////
//	cout << "****** 2 *********";
//	for (vector<struct node *>::iterator it = bfsvec.begin(); it != bfsvec.end(); it++) {
//		JVAR *jvar = (JVAR *)(*it)->data;
//		cout << jvar->nodenum << endl;
//	}
//	//////////////

//	deque<struct node *> jnodes;

	unique(bfsvec.begin(), bfsvec.end());

	//Remove duplicates from bfsvec
//	for (vector<struct node *>::iterator it = bfsvec.begin(); it != bfsvec.end(); it++) {
//		if (find(jnodes.begin(), jnodes.end(), *it) == jnodes.end()) {
//			jnodes.push_back(*it);
//		}
//		else {
//			JVAR *jvar = (JVAR *)(*rit)->data;
//			cout << "DEBUG " << jvar->nodenum << endl;
//			bfsvec.erase(--(rit.base()));
//		}
//	}
//	bfsvec.erase(bfsvec.begin(), bfsvec.end());
//	bfsvec.insert(bfsvec.end(), jnodes.begin(), jnodes.end());
	//////////////////////////////////////
	if (bfsvec.size() != graph_jvar_nodes) {
		cout << bfsvec.size() << " " << graph_jvar_nodes << endl;
		for (vector<struct node *>::iterator it = bfsvec.begin(); it != bfsvec.end(); it++) {
			cout << ((JVAR*)(*it)->data)->nodenum << endl;
		}
		assert(0);
	}

	return bfsvec;

//	bfsq.push(ranklist.front());
//	string level = ((TP*)ranklist.front()->data)->strlevel;
//
//	set<string> donelvls;
//
//	ranklist.pop_front();
//
//	while (1) {
//		while (bfsq.size() > 0) {
//			struct node *qfront = bfsq.front();
//
//			qfront->color = BLACK;
//			bfsq.pop();
//			LIST *q_neighbor = qfront->nextTP;
//			vector<struct node *> q_neighborvec;
//
//			for (; q_neighbor ; q_neighbor = q_neighbor->next) {
//				q_neighborvec.push_back(q_neighbor->gnode);
//			}
//			stable_sort(q_neighborvec.begin(), q_neighborvec.end(), sort_tpnodes_jvar_tree);
//
//			for (vector<struct node *>::iterator it = q_neighborvec.begin(); it != q_neighborvec.end(); it++) {
//				struct node *grnode = *it;
//				if (((TP*)grnode->data)->strlevel.compare(level) != 0 && grnode->numTPs > 1) {
//					continue;
//				}
//
//				LIST *nextadjvar = qfront->nextadjvar;
//				for (; nextadjvar; nextadjvar = nextadjvar->next) {
//					struct node *jvar = nextadjvar->gnode;
//
//					//TODO: needs a fix here to make sure all jvars get listed
//					LIST * nextadjvar2 = grnode->nextadjvar;
//					for (; nextadjvar2; nextadjvar2 = nextadjvar2->next) {
//						struct node *jvar2 = nextadjvar2->gnode;
//
//						if (jvar == jvar2) {
////							if (find(bfsvec.begin(), bfsvec.end(), jvar) == bfsvec.end()) {
//							bfsvec.push_back(jvar);
////							}
//						}
//					}
//				}
//				if (((TP*)grnode->data)->strlevel.compare(level) != 0 && grnode->numTPs == 1) {
//					donelvls.insert(((TP*)grnode->data)->strlevel);
//				}
//
//				if (grnode->color == BLACK)
//					continue;
//				grnode->color = BLACK;
//				bfsq.push(grnode);
//			}
//		}
//
//		donelvls.insert(level);
//
//		//Update level to new master
//		for (list<struct node *>::iterator it = ranklist.begin(); it != ranklist.end(); ) {
//			string slevel = ((TP*)(*it)->data)->strlevel;
//			if (donelvls.find(slevel) != donelvls.end()) {
//				ranklist.erase(it);
//				continue;
//			}
//
//			LIST *q_neighbor = (*it)->nextTP;
//			for (; q_neighbor ; q_neighbor = q_neighbor->next) {
//				if (q_neighbor->gnode->color == BLACK) {
//					break;
//				}
//			}
//			if (q_neighbor) {
//				level = slevel;
//				bfsq.push(*it);
//				ranklist.erase(it);
//				break;
//			}
//			it++;
//		}
//		if (bfsq.size() == 0) {
//			break;
//		}
//	}
//
//	//Remove duplicates from bfsvec
//	set<struct node *> jnodes;
//	for (vector<struct node *>::reverse_iterator rit = bfsvec.rbegin(); rit != bfsvec.rend(); ) {
//		if (jnodes.find(*rit) == jnodes.end()) {
//			jnodes.insert(*rit);
//			rit++;
//		} else {
//			bfsvec.erase(--(rit.base()));
//		}
//	}
//
//	assert(donelvls.size() == levelwise.size());
}
///////////////////////////
vector<struct node *> final_res_gen_tp_seq() {
	vector<struct node *> tpvec;
	vector<struct node *> tpvec2;
	deque<struct node *> bfsq;

	for (int i = 0; i < graph_tp_nodes; i++) {
		tpvec.push_back(&graph[MAX_JVARS_IN_QUERY+i]);
	}
	stable_sort(tpvec.begin(), tpvec.end(), sort_tp_nodes_prune);

	struct node *gnode = tpvec.front();
	tpvec.erase(tpvec.begin());
	bfsq.push_back(gnode);

	while (bfsq.size() > 0 || tpvec.size() > 0) {
		if (bfsq.size() > 0) {
			gnode = bfsq.front();
			bfsq.pop_front();
		} else {
			gnode = tpvec.front();
			tpvec.erase(tpvec.begin());
		}
		tpvec2.push_back(gnode);

		for (vector<struct node *>::iterator it = tpvec.begin(); it != tpvec.end(); ) {
			struct node *ngb = *it;
			LIST *next = gnode->nextTP;
			bool found = false;

			while (next) {
				if (next->gnode == ngb) {
					tpvec2.push_back(ngb);
					bfsq.push_back(ngb);
					found = true;
					it = tpvec.erase(it);
					break;
				}
				next = next->next;
			}
			if (!found) it++;
			if (found) break;
		}
	}

	assert(tpvec2.size() == graph_tp_nodes);

	return tpvec2;
}

vector<struct node *> spanning_tree_tpnodes()
{
//	cout << "Inside spanning_tree_tpnodes" << endl;
	vector<struct node *> rankvec = rank_master_nodes();
	list<struct node *> ranklist(rankvec.begin(), rankvec.end());
	rankvec.clear();
	queue<struct node *> bfsq;
	vector<struct node *> bfsvec;

	for (int i = 0; i < graph_tp_nodes; i++) {
		graph[MAX_JVARS_IN_QUERY + i].color = WHITE;
	}

	bfsq.push(ranklist.front());

	string level = ((TP*)ranklist.front()->data)->strlevel;

	assert(level.find("s") == string::npos);

	set<string> donelvls;

	ranklist.pop_front();

	while (1) {
		while (bfsq.size() > 0) {
			struct node *qfront = bfsq.front();
			if (qfront->color != BLACK)
				bfsvec.push_back(qfront);

			qfront->color = BLACK;
			bfsq.pop();
			LIST *q_neighbor = qfront->nextTP;
//			cout << "Qfront " << ((TP*)qfront->data)->sub << " " << ((TP*)qfront->data)->pred << " " << ((TP*)qfront->data)->obj << endl;
			vector<struct node *> q_neighborvec;

			for (; q_neighbor ; q_neighbor = q_neighbor->next) {
				q_neighborvec.push_back(q_neighbor->gnode);
			}

			stable_sort(q_neighborvec.begin(), q_neighborvec.end(), sort_master_tp_nodes);

			for (vector<struct node *>::iterator it = q_neighborvec.begin(); it != q_neighborvec.end(); it++) {
				struct node *grnode = *it;
				if (grnode->color == BLACK || !grnode->isNeeded)
					continue;
				if (
						((TP*)qfront->data)->strlevel.compare(((TP*)grnode->data)->strlevel) == 0
						||
						((TP*)grnode->data)->strlevel.find("s", 0) == string::npos
						||
						grnode->numTPs == 1
					) {

					bfsvec.push_back(grnode);
					//A node is marked visited as soon as it is enqued in the
					//bfsq
					grnode->color = BLACK;
					bfsq.push(grnode);

					if (grnode->numTPs == 1) {
						donelvls.insert(((TP*)grnode->data)->strlevel);
					}
				}
			}

			donelvls.insert(((TP*)qfront->data)->strlevel);

		}

		donelvls.insert(level);

		//Go over all the slaves of 'level'
		//levelwise is sorted according to the alphabetical order
		//hence it ensures that slaves are ordered according to their rank
		for (map<string, vector<struct node *> >::iterator it = levelwise.begin(); it != levelwise.end(); it++) {
			string slevel = (*it).first;
			//Any node from a given level can do because all nodes
			//at a given level have same master-slave relationship with others
			TP *tptmp = (TP*)(((*it).second)[0])->data;

			//Disjoint level OR already done level
			//Once a node is enqued in the bfsq, its level
			//gets done by the rule of connectivity of the query graph
			if (!tptmp->isSlave(level) || donelvls.find(slevel) != donelvls.end())
				continue;

			if (tptmp->isSlaveOfAnyone(null_perm)) {
//				cout << "spanning_tree_tpnodes: null_perm ----> "<< tptmp->toString() << endl;
				null_perm.insert(slevel);
				donelvls.insert(slevel);
				continue;
			}

			struct retval greturn = rank_slave_nodes(slevel);
			struct node *start_node = greturn.q_node;
//			cout << "Start_node " << start_node << " atleast " << greturn.atLeastOne << endl;
			if (start_node == NULL && greturn.atLeastOne) {
				null_perm.insert(slevel);
				donelvls.insert(slevel);
				continue;
			} else if (start_node == NULL && !greturn.atLeastOne) {
				continue;
			}
			bfsq.push(start_node);

			while (bfsq.size() > 0) {
				struct node *qfront = bfsq.front();
				if (qfront->color != BLACK)
					bfsvec.push_back(qfront);

				qfront->color = BLACK;
				bfsq.pop();
				LIST *q_neighbor = qfront->nextTP;
				vector<struct node *> q_neighborvec;

				for (; q_neighbor ; q_neighbor = q_neighbor->next) {
					q_neighborvec.push_back(q_neighbor->gnode);
				}

				stable_sort(q_neighborvec.begin(), q_neighborvec.end(), sort_master_tp_nodes);

				for (vector<struct node *>::iterator it = q_neighborvec.begin(); it != q_neighborvec.end(); it++) {
					struct node *grnode = *it;
					if (grnode->color == BLACK || !grnode->isNeeded)
						continue;
					if (
						((TP*)qfront->data)->strlevel.compare(((TP*)grnode->data)->strlevel) == 0
						||
						grnode->numTPs == 1
						||
						(!((TP*)qfront->data)->isSlave(((TP*)grnode->data)->strlevel) &&
						 !((TP*)grnode->data)->isSlave(((TP*)qfront->data)->strlevel))
						) {
						if (grnode->numTPs == 1) {
							donelvls.insert(((TP*)grnode->data)->strlevel);
						}
						grnode->color = BLACK;
						bfsq.push(grnode);
						bfsvec.push_back(grnode);
					}
				}
				donelvls.insert(((TP *)qfront->data)->strlevel);
			}

			donelvls.insert(slevel);
		}

		//Update 'level' to new master
		for (list<struct node *>::iterator it = ranklist.begin(); it != ranklist.end(); ) {
			string mlevel = ((TP*)(*it)->data)->strlevel;
			TP *dbgtp = (TP*)(*it)->data;
			if (donelvls.find(mlevel) != donelvls.end()) {
				it = ranklist.erase(it);
				continue;
			}
//			cout << "spanning_tree_tp_nodes [" << dbgtp->sub << " " << dbgtp->pred << " " << dbgtp->obj << "]" << endl;

			LIST *q_neighbor = (*it)->nextTP;
			for (; q_neighbor ; q_neighbor = q_neighbor->next) {
				if (q_neighbor->gnode->color == BLACK) {
					break;
				}
			}
			if (q_neighbor) {
				level = mlevel;
				bfsq.push(*it);
				it = ranklist.erase(it);
				break;
			}
			it++;
		}
//		cout << "Done with ranklist loop" << endl;
		if (bfsq.size() > 0) {
			continue;
		}
		for (map<string, vector<struct node *> >::iterator it = levelwise.begin(); it != levelwise.end(); it++) {
			string slevel = (*it).first;
			TP *tptmp = (TP*)(((*it).second)[0])->data;

			if (donelvls.find(slevel) != donelvls.end())
				continue;

			if (tptmp->isSlaveOfAnyone(null_perm)) {
				null_perm.insert(slevel);
				donelvls.insert(slevel);
				continue;
			}

			struct retval greturn = rank_slave_nodes(slevel);
			struct node *start_node = greturn.q_node;
			if (start_node == NULL && greturn.atLeastOne) {
				null_perm.insert(slevel);
				donelvls.insert(slevel);
				continue;
			} else if (start_node == NULL && !greturn.atLeastOne) {
				continue;
			}
			bfsq.push(start_node);

			while (bfsq.size() > 0) {
				struct node *qfront = bfsq.front();
				if (qfront->color != BLACK)
					bfsvec.push_back(qfront);

				qfront->color = BLACK;
				bfsq.pop();
				LIST *q_neighbor = qfront->nextTP;
				vector<struct node *> q_neighborvec;

				for (; q_neighbor ; q_neighbor = q_neighbor->next) {
					q_neighborvec.push_back(q_neighbor->gnode);
				}

//				if (pruning)
//					stable_sort(q_neighborvec.begin(), q_neighborvec.end(), sort_tp_nodes_rev);
//				else
				stable_sort(q_neighborvec.begin(), q_neighborvec.end(), sort_master_tp_nodes);

				for (vector<struct node *>::iterator it = q_neighborvec.begin(); it != q_neighborvec.end(); it++) {
					struct node *grnode = *it;
					if (grnode->color == BLACK || !grnode->isNeeded)
						continue;
					if (
							((TP*)qfront->data)->strlevel.compare(((TP*)grnode->data)->strlevel) == 0
							||
							grnode->numTPs == 1
						) {

						grnode->color = BLACK;
						bfsq.push(grnode);
						bfsvec.push_back(grnode);

					}
				}
				donelvls.insert(((TP*)qfront->data)->strlevel);
			}

			donelvls.insert(slevel);
		}
		break;
	}

	//Sanity check -- at a given position 'i' node, none of the nodes
	//between 0 - (i-1) positions should be strict slaves of 'i'th node.
	for (int i = 1; i < bfsvec.size(); i++) {
		string level_i =  ((TP *)bfsvec[i]->data)->strlevel;
		for (int j = 0; j < i; j++) {
			string level_j =  ((TP *)bfsvec[j]->data)->strlevel;

			if (level_i != level_j && level_j.find(level_i) == 0)
				assert(0);
		}

	}

//	assert(donelvls.size() == levelwise.size());

	//This assertion may come true for queries
	//which have 0 triple bitmats at slave levels

//	null_perm.insert(null_lvls.begin(), null_lvls.end());
//	for (vector<struct node *>::iterator it = bfsvec.begin(); it != bfsvec.end(); ) {
//		if (((TP*)(*it)->data)->isSlaveOfAnyone(null_perm)) {
//			bfsvec.erase(it);
//			continue;
//		}
//		it++;
//	}

	assert(bfsvec.size() > 0);

	return bfsvec;
}
///////////////////////////
void spanning_tree_bfs(struct node *n, int nodetype)
{
//	cout << "Inside spanning_tree" << endl;
	int totalnodecnt = graph_var_nodes;

	for (int i = 0; i < totalnodecnt; i++) {
		graph[i].color = WHITE;
	}

	queue<struct node *> bfsq;

	bfsq.push(n);
	while (bfsq.size() > 0 && nodetype != TP_NODE) {
		LIST *q_neighbor;

		struct node *qfront = bfsq.front();

		qfront->color = BLACK;
		bfsq.pop();

		q_neighbor = qfront->nextadjvar;

//		int numAdjNodes = (nodetype == TP_NODE) ? n->numTPs : n->numadjvars;

		for (; q_neighbor; q_neighbor = q_neighbor->next) {
			//If you are doing spanning_tree for TP nodes make sure to put
			//all master nodes before slaves

			if (q_neighbor->gnode->color == BLACK || q_neighbor->gnode->type == NON_JVAR_NODE || !q_neighbor->gnode->isNeeded) {
				continue;
			}
			q_neighbor->gnode->color = BLACK;
			if (tree_edges_map.find(qfront) == tree_edges_map.end()) {
				vector<struct node *> q_n_nodes;
				q_n_nodes.push_back(q_neighbor->gnode);
				tree_edges_map[qfront] = q_n_nodes;
			} else {
				tree_edges_map[qfront].push_back(q_neighbor->gnode);
			}

			//Only add a 2way edge if it's a JVAR_NODE as for TP_NODE
			//you have a modified algo now
			if (tree_edges_map.find(q_neighbor->gnode) == tree_edges_map.end()) {
				vector<struct node *> q_n_nodes2;
				q_n_nodes2.push_back(qfront);
				tree_edges_map[q_neighbor->gnode] = q_n_nodes2;
			} else {
				tree_edges_map[q_neighbor->gnode].push_back(qfront);
			}

			q_neighbor->gnode->level = qfront->level + 1;

			bfsq.push(q_neighbor->gnode);
		}

	}


//	cout << "numAdjNodes " << numAdjNodes << endl; 


//	cout << "Exiting spanning_tree" << endl;
}

////////////////////////////////////////////////////////////
//This is nothing but DFS algorithm
void spanning_tree(struct node *n, int curr_node, int nodetype)
{
//	cout << "Inside spanning_tree" << endl;
	int offset = (nodetype == TP_NODE) ? MAX_JVARS_IN_QUERY : 0;
	int nodecnt = (nodetype == TP_NODE) ? graph_tp_nodes : graph_jvar_nodes;
	int totalnodecnt = (nodetype == TP_NODE) ? graph_tp_nodes : graph_var_nodes;

	if (curr_node == nodecnt)
		return;

	if (curr_node == 1) {
		for (int i = 0; i < totalnodecnt; i++) {
			graph[offset + i].color = WHITE;
		}
	}

	if (n == NULL) {
//		cout << "Returning" << endl;
		return;
	}

	n->color = BLACK;

	LIST *q_neighbor;

	if (nodetype == TP_NODE)
		q_neighbor = n->nextTP;
	else 
		q_neighbor = n->nextadjvar;

	int numAdjNodes = (nodetype == TP_NODE) ? n->numTPs : n->numadjvars;

//	cout << "numAdjNodes " << numAdjNodes << endl; 

	for (int i = 0; i < numAdjNodes; i++) {

		if (q_neighbor->gnode->color == WHITE && q_neighbor->gnode->type != NON_JVAR_NODE) {
			q_neighbor->gnode->color = BLACK;
			if (tree_edges_map.find(n) == tree_edges_map.end()) {
				vector<struct node *> q_n_nodes;
				q_n_nodes.push_back(q_neighbor->gnode);
				tree_edges_map[n] = q_n_nodes;
			} else {
				tree_edges_map[n].push_back(q_neighbor->gnode);
			}


			if (tree_edges_map.find(q_neighbor->gnode) == tree_edges_map.end()) {
				vector<struct node *> q_n_nodes2;
				q_n_nodes2.push_back(n);
				tree_edges_map[q_neighbor->gnode] = q_n_nodes2;
			} else {
				tree_edges_map[q_neighbor->gnode].push_back(n);
			}

			spanning_tree(q_neighbor->gnode, curr_node+1, nodetype);
		}

		q_neighbor = q_neighbor->next;
	}

//	cout << "Exiting spanning_tree" << endl;
}

//////////////////////////////////////
void print_spanning_tree(int nodetype)
{

//	cout << "********* Printing spanning tree edges **********" <<endl;
	
	for (std::map<struct node*, vector<struct node*> >::iterator it=tree_edges_map.begin(); it != tree_edges_map.end(); it++) {
		if (nodetype == TP_NODE) {
			cout << ((TP *)(it->first)->data)->sub << " " << ((TP *)(it->first)->data)->pred << " " << ((TP *)(it->first)->data)->obj << " -> ";
			for (std::vector<struct node *>::iterator it2 = it->second.begin(); it2 != it->second.end(); it2++) {
	//			cout << ((TP *)(it2->first)->data)->sub << " " << ((TP *)(it2->first)->data)->pred << " " << ((TP *)(it2->first)->data)->obj << endl;
				cout << "[" << ((TP*)(*it2)->data)->sub << " " << ((TP*)(*it2)->data)->pred << " " << ((TP*)(*it2)->data)->obj << "], ";
			}
			cout << endl;
		} else if (nodetype == JVAR_NODE) {
			cout << ((JVAR *)(it->first)->data)->nodenum << " -> ";
			for (std::vector<struct node *>::iterator it2 = it->second.begin(); it2 != it->second.end(); it2++) {
				cout << ((TP*)(*it2)->data)->nodenum << ", ";
			}
			cout << endl;

		} else {
			cout << "**** ERROR print_spanning_tree: unregonized nodetype" << endl;
			exit (-1);
		}
	}

	cout << "********* Done printing spanning tree edges **********" <<endl;
}

////////////////////////

void print_graph(void)
{
	TP *tp = NULL;
	JVAR *jvar = NULL;
	LIST *next = NULL;

	cout << "**** Selected variables ****" << endl;

	if (selvars.size() == 0)
		cout << "ALL" << endl;
	else {
		for (map<int, struct node *>::iterator it = selvars.begin(); it != selvars.end(); it++) {
			cout << it->first << " type:" << it->second->type << " ";
		}
		cout << endl;
	}

	cout << "**** Adjacency list of TP -> JVAR ****" << endl;

	for (int i = 0; i < graph_tp_nodes; i++) {
		tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
		cout << "nodenum " << tp->nodenum << " [" << tp->sub << " " << tp->pred << " " << tp->obj << "]: " << tp->strlevel;
		tp->printPseudomasters();
		cout << " -> ";

//		assert(graph[MAX_JVARS_IN_QUERY + i].numadjvars > 0);

		next = graph[MAX_JVARS_IN_QUERY + i].nextadjvar;
		
		for (; next; next = next->next) {
			jvar = (JVAR *)next->gnode->data;
			printf("[%d %d] ", jvar->nodenum, next->gnode->type);
		}
		printf("\n");
	}

	cout << "**** Adjacency list of JVAR -> TP ****" << endl;

	for (int i = 0; i < graph_var_nodes; i++) {
		jvar = (JVAR *)graph[i].data;
//		assert(jvar != NULL);
//		assert(graph[i].type == JVAR_NODE);
		
		printf("nodenum %d %d -> ", jvar->nodenum, graph[i].type);

		LIST *next = graph[i].nextTP;

		for (; next; next = next->next) {
			tp = (TP *)next->gnode->data;
			printf("[%d %d %d] ", tp->sub, tp->pred, tp->obj);
		}
		
		printf("\n");
	}

	cout << "**** Adjacency list of JVAR -> JVAR ****" << endl;

	for (int i = 0; i < graph_var_nodes; i++) {
		jvar = (JVAR *)graph[i].data;
		assert(jvar != NULL);
//		assert(graph[i].type == JVAR_NODE);

		printf("nodenum %d -> ", jvar->nodenum);

//		assert(graph[i].numTPs > 0);
		LIST *next = graph[i].nextadjvar;

		for (; next; next=next->next) {
			JVAR *jvar2 = (JVAR *)next->gnode->data;
			printf("[%d] ", jvar2->nodenum);
		}
		
		printf("\n");
	}

	cout << "**** Adjacency list of TP -> TP ****" << endl;

	for (int i = 0; i < graph_tp_nodes; i++) {
		tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
		
		cout << "[" << tp->sub << " " << tp->pred << " " << tp->obj << "]: " << tp->strlevel;
		tp->printPseudomasters();
		tp->printRealMasters();
		cout << " rollback " << tp->rollback;
		cout << " -> ";

		LIST *next = graph[MAX_JVARS_IN_QUERY + i].nextTP;

		for (; next; next = next->next) {
			TP *tp2 = (TP *)next->gnode->data;
			cout << "[(" << tp2->sub << " " << tp2->pred << " " << tp2->obj << "), " <<
					next->edgetype << ", " << tp2->strlevel << "] " ;
		}

		cout << "\n" << endl;
	}
	cout << "Done print graph" << endl;

}

//////////////////////////////////////
/*
void print_tp_bitmats(void)
{
	TP *tp;

	for (int i = 0; i < graph_tp_nodes; i++) {
		tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
		printf("[%d %d %d] -> \n", tp->sub, tp->pred, tp->obj);
		tp->bitmat.num_triples = tp->bitmat.count_triples_in_bitmat();
		tp->bitmat->print_bitmat_info();
		if (tp->bitmat_ops != NULL) {
			tp->bitmat_ops->num_triples = tp->bitmat_ops->count_triples_in_bitmat();
			tp->bitmat_ops->print_bitmat_info();
		}

	}
}
*/
//////////////////////////////////////
/*
void cleanup(void)
{
	TP *tp;
	JVAR *jvar;
	LIST *next, *prev;

	cout << "**** Cleaning up ConstraintGraph ****" << endl;

	for (int i = 0; i < graph_tp_nodes; i++) {
		prev = graph[MAX_JVARS_IN_QUERY + i].nextadjvar;
		next = prev->next;

		for (int j = 0; j < graph[MAX_JVARS_IN_QUERY + i].numadjvars; j++) {
			free(prev);
			if (next == NULL)
				break;
			prev = next;
			next = prev->next;
		}

		if (graph[MAX_JVARS_IN_QUERY + i].nextTP == NULL)
			continue;

		prev = graph[MAX_JVARS_IN_QUERY + i].nextTP;
		next = prev->next;

		for (int j = 0; j < graph[MAX_JVARS_IN_QUERY + i].numTPs; j++) {
			free(prev);
			if (next == NULL)
				break;
			prev = next;
			next = prev->next;
		}

	}

	for (int i = 0; i < graph_jvar_nodes; i++) {
		
		prev = graph[i].nextTP;
		next = prev->next;

		for (int j = 0; j < graph[i].numTPs; j++) {
			free(prev);
			if (next == NULL)
				break;
			prev = next;
			next = prev->next;
		}

		if (graph[i].nextadjvar == NULL)
			continue;
		prev = graph[i].nextadjvar;
		next = prev->next;

		for (int j = 0; j < graph[i].numadjvars; j++) {
			free(prev);
			if (next == NULL)
				break;
			prev = next;
			next = prev->next;
		}

	}

	for (int i = 0; i < graph_tp_nodes; i++) {
		tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
		clean_up(tp->bitmat);
		free(tp);
	}

	for (int i = 0; i < graph_jvar_nodes; i++) {
		jvar = (JVAR *)graph[i].data;
		if (jvar->joinres != NULL)
			free(jvar->joinres);
		if (jvar->joinres_so != NULL)
			free(jvar->joinres_so);

		free(jvar);

	}

	graph_tp_nodes = 0;
	graph_jvar_nodes = 0;

	q_to_gr_node_map.erase(q_to_gr_node_map.begin(), q_to_gr_node_map.end());
	tree_edges_map.erase(tree_edges_map.begin(), tree_edges_map.end());
	unvisited.erase(unvisited.begin(), unvisited.end());

}
*/

/////////////////////////////////////////////////
void print_node_info(struct node *n)
{
	cout << "*********** Printing node info **************" << endl;
	
	if (n->type == TP_NODE) {
		TP *tp = (TP *)n->data;
		cout << "nodeid:" << tp->nodenum << " sub:" << tp->sub << " pred:"
			<< tp->pred << " obj:" << tp->obj;
		cout << " neighbor_jvars: " << n->numadjvars << " neighbor_tps:" << n->numTPs << endl;

		LIST *nextjvar = n->nextadjvar;

		cout << "Adjacant jvars --> "<< endl;

		for (int i = 0; i < n->numadjvars; i++) {
			JVAR *jvar = (JVAR *)nextjvar->gnode->data;
			cout << jvar->nodenum << " ";
			nextjvar = nextjvar->next;
		}

		cout << endl;

		LIST *nextTP = n->nextTP;

		cout << "Adjacant TPs --> "<< endl;

		for (int i = 0; i < n->numTPs; i++) {
			TP *tp2 = (TP *)nextTP->gnode->data;
			cout << "nodeid:" << tp2->nodenum << " " << tp2->sub << " "<<tp2->pred << " " << tp2->obj << endl;
			nextTP = nextTP->next;
		}

		cout << endl;

	} else if (n->type == JVAR_NODE) {
		JVAR *jvar = (JVAR *)n->data;
		cout << "nodeid:" << jvar->nodenum;
		cout << " neighbor_jvars: " << n->numadjvars << " neighbor_tps:" << n->numTPs << endl;

		LIST *nextjvar = n->nextadjvar;

		cout << "Adjacant jvars --> "<< endl;

		for (int i = 0; i < n->numadjvars; i++) {
			JVAR *jvar2 = (JVAR *)nextjvar->gnode->data;
			cout << jvar2->nodenum << " ";
			nextjvar = nextjvar->next;
		}

		cout << endl;

		LIST *nextTP = n->nextTP;

		cout << "Adjacant TPs --> "<< endl;

		for (int i = 0; i < n->numTPs; i++) {
			TP *tp2 = (TP *)nextTP->gnode->data;
			cout << "nodeid:" << tp2->nodenum << " " << tp2->sub << " "<<tp2->pred << " " << tp2->obj << endl;
			nextTP = nextTP->next;
		}

		cout << endl;

	} else {
		cout << "**** ERROR: no node type specified in the field" << endl;
		exit (-1);
	}

	cout << "*********** Done printing node info **************" << endl;
}

////////////////////////////////////////////////////////////
void makeNullJoinres(JVAR *jvar, TP *tp)
{
	BitVecJoinres *jvar_join = new BitVecJoinres;

	if (tp->sub == jvar->nodenum) {
		if (tp->bitmat.dimension == SPO_BITMAT) {
			jvar_join->joinres_size = tp->bitmat.subject_bytes;
		} else if (tp->bitmat.dimension == OPS_BITMAT || tp->bitmat.dimension == POS_BITMAT) {
			jvar_join->joinres_size = tp->bitmat.object_bytes;
		}
		jvar_join->joinres_dim = SUB_DIMENSION;
	} else if (tp->obj == jvar->nodenum) {
		if (tp->bitmat.dimension == OPS_BITMAT) {
			jvar_join->joinres_size = tp->bitmat.subject_bytes;
		} else if (tp->bitmat.dimension == SPO_BITMAT || tp->bitmat.dimension == PSO_BITMAT) {
			jvar_join->joinres_size = tp->bitmat.object_bytes;
		}
		jvar_join->joinres_dim = OBJ_DIMENSION;
	} else if (tp->pred == jvar->nodenum) {
		if (tp->bitmat.dimension == PSO_BITMAT || tp->bitmat.dimension == POS_BITMAT) {
			jvar_join->joinres_size = tp->bitmat.subject_bytes;
		} else
			assert(0);
		jvar_join->joinres_dim = PRED_DIMENSION;
	}
	assert(jvar_join->joinres_size > 0);
	jvar_join->joinres = (unsigned char *) malloc (jvar_join->joinres_size);
	memset(jvar_join->joinres, 0x00, jvar_join->joinres_size);
	jvar_join->empty = true;
	jvar_join->strlevel = tp->strlevel;
	jvar->joinres.push_back(jvar_join);

}
////////////////////////////////////////////////

bool prune_for_jvar(struct node *jvarnode, bool bushy, int numpass)
{
//	cout << "prune_for_jvar: numpass " << numpass << endl;
	JVAR *jvar = (JVAR *)jvarnode->data;
	struct timeval start_time, stop_time;
	double curr_time;
	double st, en;

	vector<struct node *> tpvec;
	LIST *nextTP = jvarnode->nextTP;

	for (; nextTP; nextTP = nextTP->next) {
		tpvec.push_back(nextTP->gnode);
	}

	stable_sort(tpvec.begin(), tpvec.end(), sort_tp_nodes_prune);
	//As per current sorting procedure all the master nodes appear
	//before any slave nodes. Slave nodes are sorted alphabetically
	//which ensures that nested slave ordering is preserved 

	for (vector<struct node *>::iterator it = tpvec.begin(); it != tpvec.end(); it++) {
		TP *tp = (TP*)(*it)->data;
		struct node *tpgnode = *it;

		if (tp->isSlaveOfAnyone(null_perm)) {
			null_perm.insert(tp->strlevel);
		}

		if (null_perm.find(tp->strlevel) != null_perm.end()) {
			if (tp->rollback)
				return false;
			else {
				BitVecJoinres *jvar_join = NULL;
				for (vector<BitVecJoinres*>::iterator it = jvar->joinres.begin(); it != jvar->joinres.end(); it++) {
					if ((*it)->strlevel.compare(tp->strlevel) == 0) {
						jvar_join = (*it); break;
					}
				}
				if (jvar_join != NULL) {
					if (jvar_join->joinres != NULL) {
						free(jvar_join->joinres);
						jvar_join->joinres = NULL;
					}
					jvar_join->joinres_size = 0;
					jvar_join->empty = true;
				} else {
					//This is necessary to ensure that any disjoint-level joins are
					//propagated correctly
//					makeNullJoinres(jvar, tp);
					jvar_join = new BitVecJoinres;
					jvar_join->joinres_size = 0;
					jvar_join->empty = true;
					jvar_join->joinres = NULL;
				}

				tp->bitmat.freebm();
				tp->triplecnt = 0;
				continue;
			}
		}

		if (tp->sub == jvar->nodenum) {
//			cout << "Subject_dim_join " << jvar->nodenum << " [" << tp->sub << " " << tp->pred << " " << tp->obj << "]" << endl;
			if (!subject_dim_join(jvarnode, tpgnode, false)) {
				cout << "join var " << jvar->nodenum << " has 0 res" << endl;
				return false;
			}

		} else if (tp->obj == jvar->nodenum) {
//			cout << "Object_dim_join " << jvar->nodenum << " [" << tp->sub << " " << tp->pred << " " << tp->obj << "]" << endl;
			if (!object_dim_join(jvarnode, tpgnode, false)) {
				cout << "join var " << jvar->nodenum << " has 0 res" << endl;
				return false;
			}
		} else if (tp->pred == jvar->nodenum) {
//			cout << "Predicate_dim_join " << jvar->nodenum << " [" << tp->sub << " " << tp->pred << " " << tp->obj << "]" << endl;
			if (!predicate_dim_join(jvarnode, tpgnode, false)) {
				cout << "join var " << jvar->nodenum << " has 0 res" << endl;
				return false;
			}

		}
		/////////
		//DEBUG
		//TODO: remove later
		///////
//		for (vector<BitVecJoinres*>::iterator it = jvar->joinres.begin(); it != jvar->joinres.end(); it++) {
//			BitVecJoinres *jvar_join = *it;
//			if (jvar_join->strlevel != tp->strlevel)
//				continue;
//			cout << "-----prune_for_jvar: jvar " << jvar->nodenum << " for tp "; tp->print(); cout << endl;
//			print_set_bits_in_row(jvar_join->joinres, jvar_join->joinres_size);
//
//		}
		//////////////////////////
	}

	/////////
	//DEBUG
	//TODO: remove later
	///////
//	for (vector<BitVecJoinres*>::iterator it = jvar->joinres.begin(); it != jvar->joinres.end(); it++) {
//		BitVecJoinres *jvar_join = *it;
//		cout << "---------- BEFORE pass_results_across_levels prune_for_jvar: jvar " << jvar->nodenum << " for level " << jvar_join->strlevel << endl;
//		print_set_bits_in_row(jvar_join->joinres, jvar_join->joinres_size);
//	}
	//////////////////////////
//	cout << "prune_for_jvar: calling pass_results_across_levels" << endl;

	if (!pass_results_across_levels(jvarnode)) {
		cout << "join var " << jvar->nodenum << " has 0 res" << endl;
		return false;
	}
//	cout << "prune_for_jvar: done pass_results_across_levels" << endl;

	/////////
	//DEBUG
	//TODO: remove later
	///////
//	for (vector<BitVecJoinres*>::iterator it = jvar->joinres.begin(); it != jvar->joinres.end(); it++) {
//		BitVecJoinres *jvar_join = *it;
//		cout << "---------- AFTER pass_results_across_levels prune_for_jvar: jvar " << jvar->nodenum << " for level " << jvar_join->strlevel << endl;
//		print_set_bits_in_row(jvar_join->joinres, jvar_join->joinres_size);
//	}
	//////////////////////////

	for (vector<struct node *>::iterator it = tpvec.begin(); it != tpvec.end(); it++) {
		TP *tp = (TP*)(*it)->data;
		struct node *tpgnode = *it;

		if (tp->isSlaveOfAnyone(null_perm)) {
			null_perm.insert(tp->strlevel);
			tp->numpass = numpass;
			tp->bitmat.freebm();
			tp->triplecnt = 0;
			continue;
		}

		if (tp->numjvars > 1) {
//				cout << "numpass " << numpass << " tp->numjvars " << tp->numjvars << " tp->numpass " << tp->numpass << endl;
			if (jvar->nodenum == tp->sub) {
//				cout << "unfolding1 " << jvar->nodenum << " on " << tp->sub << ":"<<tp->pred << ":" << tp->obj<<endl;
				subject_dim_unfold(jvar, tp, false, numpass);
//				cout << "unfolding1 DONE " << jvar->nodenum << " on " << tp->sub << ":"<<tp->pred << ":" << tp->obj<<endl;
			} else if (jvar->nodenum == tp->obj) {
//				cout << "unfolding2 " << jvar->nodenum << " on " << tp->sub << ":"<<tp->pred << ":" << tp->obj<<endl;
				object_dim_unfold(jvar, tp, false, numpass);
			} else if (jvar->nodenum == tp->pred) {
//				cout << "unfolding3 " << jvar->nodenum << " on " << tp->sub << ":"<<tp->pred << ":" << tp->obj<<endl;
				predicate_dim_unfold(jvar, tp, false, numpass);
			}
		}

		tp->numpass++;
	}
	for (set<string>::iterator it = null_perm.begin(); it != null_perm.end(); it++) {
		string level = *it;
		if (level.find("s") == string::npos)
			return false;
	}

	return true;

//	gettimeofday(&start_time, (struct timezone *)0);

	///////////
	//DEBUG:
	////////////
/*	
	if (jvar->nodenum == -1) {
		unsigned int count = 1;
		for (unsigned int i = 0; i < jvar->joinres_size; i++) {
			if (jvar->joinres[i] == 0x00) {
				count += 8;
				continue;
			}   
			for (unsigned short j = 0; j < 8; j++, count++) {
				if (jvar->joinres[i] & (0x80 >> j)) {
					printf("%u\n", count); 
				}   
			}   
		}
	}
*/	
	/////////////////////

//	cout << "********#bits remaining after a join " << count_bits_in_row(jvar->joinres, jvar->joinres_size) << endl;

//	gettimeofday(&stop_time, (struct timezone *)0);
//	st = start_time.tv_sec + (start_time.tv_usec/MICROSEC);
//	en = stop_time.tv_sec + (stop_time.tv_usec/MICROSEC);
//	curr_time = en-st;
//
//	printf("Time to just prune: %f\n", curr_time);

}
////////////////////////////////////////////////
bool get_limited_subtree(struct node *n) 
{
	bool flag = false;

	LIST *q_neighbor = n->nextadjvar;

	int numAdjNodes = n->numadjvars;

	n->color = BLACK;

	int cnt = 0;

//	cout << "numAdjNodes " << numAdjNodes << endl; 

	for (int i = 0; i < numAdjNodes; i++) {
		if (q_neighbor->gnode->color == WHITE && q_neighbor->gnode->type == JVAR_NODE) {
			cnt++;

			q_neighbor->gnode->color = BLACK;

			if (get_limited_subtree(q_neighbor->gnode)) {
				flag = true;
				if (tree_edges_map.find(n) == tree_edges_map.end()) {
					vector<struct node *> q_n_nodes;
					q_n_nodes.push_back(q_neighbor->gnode);
					tree_edges_map[n] = q_n_nodes;
				} else {
					tree_edges_map[n].push_back(q_neighbor->gnode);
				}

				if (tree_edges_map.find(q_neighbor->gnode) == tree_edges_map.end()) {
					vector<struct node *> q_n_nodes2;
					q_n_nodes2.push_back(n);
					tree_edges_map[q_neighbor->gnode] = q_n_nodes2;
				} else {
					tree_edges_map[q_neighbor->gnode].push_back(n);
				}

			}

		}

		q_neighbor = q_neighbor->next;

	}

	if (cnt == 0 && seljvars.find(((JVAR *)n->data)->nodenum) == seljvars.end())
		return false;
	else if (seljvars.find(((JVAR *)n->data)->nodenum) != seljvars.end())
		return true;

	return flag;

}

//////////////////////////////////////////////////////////////////
bool prune_triples_new(bool bushy, bool opt, vector<vector<struct node *>> &cycles,
						bool best_match_reqd, bool allslaves_one_jvar)
{
//	tree_edges_map.erase(tree_edges_map.begin(), tree_edges_map.end());

	//In any case just make one bottom-up pass
	for (int i = 0; i < graph_jvar_nodes; i++) {
		struct node *gnode = jvarsitr2[i];
		if (!prune_for_jvar(gnode, bushy, 1)) {
			cout << "prune_for_jvar returned1 0 res" << endl;
			return false;
		}
	}

	struct node *root = NULL;

	if (isCycle || seljvars.size() == 0) {
		if (best_match_reqd || cycles.size() > 0 || allslaves_one_jvar) {
			vector<struct node *> for_cyclic;
			for (int i = 0; i < graph_jvar_nodes; i++) {
				for (vector<vector<struct node*> >::iterator itr = cycles.begin(); itr != cycles.end(); ) {
					vector<struct node *> cycle = *itr;
					for (vector<struct node *>::iterator cit = cycle.begin(); cit != cycle.end(); cit++) {
						if (jvarsitr2[i] == *cit) {
							for_cyclic.push_back(jvarsitr2[i]);
						}
					}
					itr++;
				}
			}
			for (int i = 0; i < graph_jvar_nodes; i++) {
				//this jvar node not covered by any cycle
				if (find(for_cyclic.begin(), for_cyclic.end(), jvarsitr2[i]) == for_cyclic.end()) {
					for_cyclic.push_back(jvarsitr2[i]);
				}
			}
//			for (int i = 0; i < graph_jvar_nodes; i++) {
//				if (!prune_for_jvar(jvarsitr2[i], bushy, 2)) {
//					cout << "prune_for_jvar returned2 0 res" << endl;
//					return false;
//				}
//			}
			////////
			//DEBUG
			//TODO: remove later
			///////////
//			cout << "prune_triples_new: for_cyclic -------------" << endl;
//			for (vector<struct node *>::iterator it = for_cyclic.begin(); it != for_cyclic.end(); it++) {
//				cout << ((JVAR *)(*it)->data)->nodenum << endl;
//			}
			////////////////////////
			for (vector<struct node *>::iterator it = for_cyclic.begin(); it != for_cyclic.end(); it++) {
				if (!prune_for_jvar(*it, bushy, 2)) {
					cout << "prune_for_jvar returned2 0 res" << endl;
					return false;
				}
			}
		} else {
			//TODO: needs special processing

//			cout << "prune_triples_new: round 2 special processing" << endl;

			set<struct node *> abs_master_jvars;
			deque<struct node *> rev_order;
			for (int i = 0; i < graph_jvar_nodes; i++) {
				set<string> all_levels = jvarlevels[jvarsitr2[i]];
				for (set<string>::iterator sit = all_levels.begin(); sit != all_levels.end(); sit++) {
					if ((*sit).find("s") == string::npos)
						abs_master_jvars.insert(jvarsitr2[i]);
				}
			}

			for (int i = graph_jvar_nodes - 1; i >= 0; i--) {
				if (abs_master_jvars.find(jvarsitr2[i]) == abs_master_jvars.end())
					continue;
				rev_order.push_back(jvarsitr2[i]);
				abs_master_jvars.erase(jvarsitr2[i]);
			}

			assert(abs_master_jvars.size() == 0);
			//No need of double processing the last jvar
			for (map<string, set<struct node *>>::iterator lit = leveljvars.begin(); lit != leveljvars.end(); lit++) {
				if ((*lit).first.find("s") == string::npos)
					continue;

				set<struct node *> jset = (*lit).second;
				set<struct node *> slave_jvars;
				slave_jvars.insert(jset.begin(), jset.end());

				for (int i = graph_jvar_nodes - 1; i >= 0; i--) {
					if (slave_jvars.find(jvarsitr2[i]) == slave_jvars.end() ||
						find(rev_order.begin(), rev_order.end(), jvarsitr2[i]) != rev_order.end()) {
						continue;
					}

					rev_order.push_back(jvarsitr2[i]);
					slave_jvars.erase(jvarsitr2[i]);
				}
			}
			rev_order.pop_front();

//			for (int i = graph_jvar_nodes - 2; i >= 0; i--) {
//				if (!prune_for_jvar(jvarsitr2[i], bushy, 2)) {
//					cout << "prune_for_jvar returned3 0 res" << endl;
//					return false;
//				}
//			}
			///////////////////
			//DEBUG
//			cout << "prune_triples_new: reverse_order ------------>" << endl;
//			for (deque<struct node *>::iterator rit = rev_order.begin(); rit != rev_order.end(); rit++) {
//				cout << ((JVAR *)(*rit)->data)->nodenum << endl;
//			}
			////////////////////////
			for (deque<struct node *>::iterator rit = rev_order.begin(); rit != rev_order.end(); rit++) {
				if (!prune_for_jvar(*rit, bushy, 2)) {
					cout << "prune_for_jvar returned3 0 res" << endl;
					return false;
				}
			}
		}

	} else if (seljvars.size() > 1) {
		//TODO: may need fix for OPTIONAL/bushy queries
		//RIGHT NOW THIS CODE PATH IS NOT TRACED
//		for (int i = 0; i < graph_var_nodes; i++) {
//			graph[i].color = WHITE;
//		}
//		//Choose root
//		for (int i = 0; i < graph_var_nodes; i++) {
//			if (seljvars.find(((JVAR *)jvarsitr2[i]->data)->nodenum) != seljvars.end()) {
//				root = jvarsitr2[i];
//				break;
//			}
//		}
//		get_limited_subtree(root);
//
//		for (int i = 0; i < graph_var_nodes; i++) {
//			graph[i].color = WHITE;
//		}
//
//		for (int i = 0; i < MAX_JVARS_IN_QUERY; i++) {
//			jvarsitr2[i] = NULL;
//		}
//
//		jvarsitr2[0] = root;
//
//		tree_bfs(root);
//
//		for (int i = 0; i < graph_jvar_nodes; i++) {
//			if (jvarsitr2[i] == NULL)
//				break;
//
//			seljvars[((JVAR *)jvarsitr2[i]->data)->nodenum] = jvarsitr2[i];
//
//			if (!prune_for_jvar(jvarsitr2[i], bushy, 2)) {
//				cout << "prune_for_jvar returned3 0 res" << endl;
//				return false;
//			}
//		}

	}

/*	
	for (int i = 0; i < graph_jvar_nodes; i++) {
		if (!prune_for_jvar(jvarsitr2[i], bushy, 1)) {
			cout << "prune_for_jvar returned 0 res" << endl;
			return false;
		}
	}
	if (!bushy) {
		for (int i = 0; i < graph_jvar_nodes; i++) {
			struct node *gnode = jvarsitr2[graph_jvar_nodes - 1 - i];
			//The node under consideration is not a leaf node
			if (find(leaf_nodes.begin(), leaf_nodes.end(), gnode) == leaf_nodes.end())
				if (!prune_for_jvar(gnode, bushy, 2)) {
					cout << "prune_for_jvar returned2 0 res" << endl;
					return false;
				}
		}
	}

	tree_edges_map.erase(tree_edges_map.begin(), tree_edges_map.end());
*/
	return true;

}
////////////////////////////////////////////////////////////
/*
 * getAndGiveRes determines if the jrestmp and tporig are at such levels that
 * there does not exist any master level of them 
 * which has same join variable as jvarnode
 * so that the current tporig can give and take results to jrestmp
 * It is bit convoluted logic due to current data structures
 * which need a fix in the long run.
 * The logic here first ensures that tporig and tp are at a disjoint
 * level.
 */
bool getAndGiveRes(int jvar_nodenum, struct node *tpg, struct node *tpg2)
{
	TP *tporig = (TP *)tpg->data;
	TP *tp2 = (TP *)tpg2->data;
//	LIST *nextTP = jvarnode->nextTP;
//	JVAR *jvar = (JVAR *)jvarnode->data;

	LIST *nextTP = tpg->nextTP;
	for (; nextTP; nextTP = nextTP->next) {
		TP *tp = (TP*)nextTP->gnode->data;
		if (tp->strlevel.compare(tporig->strlevel) == 0)
			continue;
		if (tp->isSlave(tporig->strlevel))
			continue;
		if ((tp->sub == jvar_nodenum && (tp->sub == tporig->sub || tp->sub == tporig->obj))
			||
			(tp->pred == jvar_nodenum && tp->pred == tporig->pred)
			||
			(tp->obj == jvar_nodenum && (tp->obj == tporig->sub || tp->obj == tporig->obj))
			){
			if (tporig->isSlave(tp->strlevel) && !tp2->isSlave(tp->strlevel))
				return false;
		}

	}
	nextTP = tpg2->nextTP;
	for (; nextTP; nextTP = nextTP->next) {
		TP *tp = (TP*)nextTP->gnode->data;
		if (tp->strlevel.compare(tp2->strlevel) == 0)
			continue;
		if (tp->isSlave(tp2->strlevel))
			continue;
		if ((tp->sub == jvar_nodenum && (tp->sub == tp2->sub || tp->sub == tp2->obj))
			||
			(tp->pred == jvar_nodenum && tp->pred == tp2->pred)
			||
			(tp->obj == jvar_nodenum && (tp->obj == tp2->sub || tp->obj == tp2->obj))
			){
			if (tp2->isSlave(tp->strlevel) && !tporig->isSlave(tp->strlevel))
				return false;
		}

	}
	return true;
}
////////////////////////////////////////////////////////////
void AND_with_all_others(struct node *jnode, BitVecJoinres *jvar_join, struct node *tpnode, int bm_dim, int join_dim, bool firstinlevel)
{
	//NOTE:jvar_join->strlevel == tp->strlevel;
	JVAR *jvar = (JVAR *)jnode->data;
	TP *tp = (TP *)tpnode->data;
	unsigned int limit_bytes = 0;
	int peers = 0;

	LIST *nextTP = jnode->nextTP;

	for (; nextTP; nextTP = nextTP->next) {
		TP *tp2 = (TP *)nextTP->gnode->data;
		assert((tp->sub == jvar->nodenum && (tp->sub == tp2->sub || tp->sub == tp2->obj))
			|| (tp->pred == jvar->nodenum && tp->pred == tp2->pred)
			|| (tp->obj == jvar->nodenum && (tp->obj == tp2->sub || tp->obj == tp2->obj))
		   );
		if (tp != tp2 && tp->strlevel.compare(tp2->strlevel) == 0)
			peers++;
	}

	if (peers > 0) {
//		cout << "PEERS > 0 for " << jvar->nodenum << " level = " << tp->strlevel << " [" << tp->sub << " " << tp->pred << " " << tp->obj << "]" << endl;
		if (jvar_join->joinres_dim != join_dim && jvar_join->joinres_dim != SO_DIMENSION) {
			memset(&jvar_join->joinres[tp->bitmat.common_so_bytes], 0, jvar_join->joinres_size - tp->bitmat.common_so_bytes);
			jvar_join->joinres[tp->bitmat.common_so_bytes-1] &= (0xff << (8-(tp->bitmat.num_comm_so%8)));
			jvar_join->joinres_dim = SO_DIMENSION;
			limit_bytes = tp->bitmat.common_so_bytes;

		} else if (jvar_join->joinres_dim == SO_DIMENSION) {
			limit_bytes = tp->bitmat.common_so_bytes;
		} else if (jvar_join->joinres_dim == join_dim) {
			limit_bytes = jvar_join->joinres_size;
		} else
			assert(0);
		unsigned char *toand = NULL;
		if (bm_dim == ROW) {
			toand = tp->bitmat.subfold;
		} else if (bm_dim == COLUMN) {
			toand = tp->bitmat.objfold;
		} else
			assert(0);

		for (unsigned int i = 0; i < limit_bytes; i++) 
			jvar_join->joinres[i] = jvar_join->joinres[i] & toand[i];
	}

	nextTP = jnode->nextTP;
	BitVecJoinres *joinother = NULL;

	for (; nextTP; nextTP = nextTP->next) {
		TP *tp2 = (TP *)nextTP->gnode->data;
		bool getres = false, giveres = false;

		assert((tp->sub == jvar->nodenum && (tp->sub == tp2->sub || tp->sub == tp2->obj))
			|| (tp->pred == jvar->nodenum && tp->pred == tp2->pred)
			|| (tp->obj == jvar->nodenum && (tp->obj == tp2->sub || tp->obj == tp2->obj))
		   );

		//First find a neighbor which shares same jvar
		//If it's at same level then skip
		//as its results will come when its turn comes
		if (tp->strlevel.compare(tp2->strlevel) == 0) {
			continue;
		}
		if (tp2->isSlave(tp->strlevel)) {
			//if this neighbor is a slave of current, skip
			//he will be taken care of when his turn comes
			continue;
		}
		//If he's not at the same level, find its joinres vector
		for (vector<BitVecJoinres*>::iterator it = jvar->joinres.begin(); it != jvar->joinres.end(); it++) {
			joinother = (*it);

			if (joinother->strlevel.compare(tp2->strlevel) != 0) {
				continue;
			}
			//Found joinres of tp2
			if (tp->isSlave(tp2->strlevel) && firstinlevel) {
				getres = true;
			} else if (tp2->isSlave(tp->strlevel)) {
				//if this neighbor is a slave of current, skip
				//he will be taken care of when his turn comes
				continue;
			} else if (!tp->isSlave(tp2->strlevel) && !tp2->isSlave(tp->strlevel)) {
				//completely disjoint levels
//				getres = giveres = getAndGiveRes(jvar->nodenum, tpnode, nextTP->gnode);
				//Now with "update_graph_for_final" logic, you don't need getAndGiveRes,
				//just check if the two tps are neighbors of each other
				LIST *next = tpnode->nextTP;
				bool found = false;
				for (; next; next = next->next) {
					if (next->gnode == nextTP->gnode) {
						found = true;
					}
				}

				if (found) getres = giveres = true;
			}

			if (getres) {
				if (jvar_join->joinres_dim != joinother->joinres_dim && jvar_join->joinres_dim != SO_DIMENSION) {
					memset(&jvar_join->joinres[tp->bitmat.common_so_bytes], 0, jvar_join->joinres_size - tp->bitmat.common_so_bytes);
					jvar_join->joinres[tp->bitmat.common_so_bytes-1] &= (0xff << (8-(tp->bitmat.num_comm_so%8)));
					jvar_join->joinres_dim = SO_DIMENSION;
					limit_bytes = tp->bitmat.common_so_bytes;
				} else if (jvar_join->joinres_dim == SO_DIMENSION) {
					limit_bytes = tp->bitmat.common_so_bytes;
				} else if (jvar_join->joinres_dim == joinother->joinres_dim) {
					limit_bytes = jvar_join->joinres_size;
				} else
					assert(0);
				for (unsigned int i = 0; i < limit_bytes; i++) 
					jvar_join->joinres[i] = jvar_join->joinres[i] & joinother->joinres[i];
			}

			if (giveres) {
				if (jvar_join->joinres_dim != joinother->joinres_dim && joinother->joinres_dim != SO_DIMENSION) {
					memset(&joinother->joinres[tp->bitmat.common_so_bytes], 0, joinother->joinres_size - tp->bitmat.common_so_bytes);
					joinother->joinres[tp->bitmat.common_so_bytes-1] &= (0xff << (8-(tp->bitmat.num_comm_so%8)));
					joinother->joinres_dim = SO_DIMENSION;
					limit_bytes = tp->bitmat.common_so_bytes;
				} else if (joinother->joinres_dim == SO_DIMENSION) {
					limit_bytes = tp->bitmat.common_so_bytes;
				} else if (jvar_join->joinres_dim == joinother->joinres_dim) {
					limit_bytes = joinother->joinres_size;
				} else
					assert(0);
				for (unsigned int i = 0; i < limit_bytes; i++) 
					joinother->joinres[i] = jvar_join->joinres[i] & joinother->joinres[i];

			}

		}

	}

}

////////////////////////////////////////////////////////////
bool object_dim_join(struct node *gnode, struct node *tpgnode, bool first)
{
	TP *tp = (TP *)tpgnode->data;
	JVAR *jvar = (JVAR*)gnode->data;

	string level = tp->strlevel;
	BitVecJoinres *jvar_join = NULL;

	for (vector<BitVecJoinres*>::iterator it = jvar->joinres.begin(); it != jvar->joinres.end(); it++) {
		if ((*it)->strlevel.compare(level) == 0) {
			jvar_join = (*it); break;
		}
	}

	if (tp->bitmat.dimension == OPS_BITMAT) {

		if (tp->unfolded > 0 && tp->numjvars > 1) {
			simple_fold(&tp->bitmat, ROW, tp->bitmat.subfold, tp->bitmat.subject_bytes);
		}

		if (jvar_join == NULL) {
			//The particular level's bitvec doesn't exist
			//Check if the var appears at some other master level
			jvar_join = new BitVecJoinres;
			jvar_join->joinres = (unsigned char *) malloc (tp->bitmat.subject_bytes);
			jvar_join->empty = false;
			jvar_join->joinres_size = tp->bitmat.subject_bytes;
//			memset(jvar_join->joinres, 0xff, jvar_join->joinres_size);
			memcpy(jvar_join->joinres, tp->bitmat.subfold, jvar_join->joinres_size);
			jvar_join->strlevel = level;
			jvar_join->joinres_dim = OBJ_DIMENSION;
			jvar->joinres.push_back(jvar_join);
//			delete jvar_join;
			//This is needed because push_back makes a copy of the content
			//and hence forth you want pointer to this copy to ensure consistency
//			jvar_join = &(*(jvar->joinres.end()-1));

			
		} else if (!jvar_join->empty) {
			if (jvar_join->joinres_dim != OBJ_DIMENSION) {
				for (unsigned int i = 0; i < tp->bitmat.common_so_bytes; i++) {
					jvar_join->joinres[i] = jvar_join->joinres[i] & tp->bitmat.subfold[i];
				}
				if (jvar_join->joinres_size != tp->bitmat.common_so_bytes) {
					memset(&jvar_join->joinres[tp->bitmat.common_so_bytes], 0x00, jvar_join->joinres_size - tp->bitmat.common_so_bytes);
					jvar_join->joinres[tp->bitmat.common_so_bytes-1] &= (0xff << (8-(tp->bitmat.num_comm_so%8)));
					jvar_join->joinres_size = tp->bitmat.common_so_bytes;
					jvar_join->joinres_dim = SO_DIMENSION;
				}
			} else {
				assert(jvar_join->joinres_size == tp->bitmat.subject_bytes);
				for (unsigned int i = 0; i < jvar_join->joinres_size; i++) {
					jvar_join->joinres[i] = jvar_join->joinres[i] & tp->bitmat.subfold[i];
				}
			}
		}

	} else if (tp->bitmat.dimension == SPO_BITMAT || tp->bitmat.dimension == PSO_BITMAT) {
		if (tp->unfolded > 0 && tp->numjvars > 1) {
			simple_fold(&tp->bitmat, COLUMN, tp->bitmat.objfold, tp->bitmat.object_bytes);
		}
		
		if (jvar_join == NULL) {
			//The particular level's bitvec doesn't exist
			jvar_join = new BitVecJoinres;
			jvar_join->joinres = (unsigned char *) malloc (tp->bitmat.object_bytes);
			jvar_join->joinres_size = tp->bitmat.object_bytes;
//			memset(jvar_join->joinres, 0xff, jvar_join->joinres_size);
			memcpy(jvar_join->joinres, tp->bitmat.objfold, jvar_join->joinres_size);
			jvar_join->empty = false;
			jvar_join->strlevel = level;
			jvar_join->joinres_dim = OBJ_DIMENSION;
			jvar->joinres.push_back(jvar_join);
//			delete jvar_join;
			//This is needed because push_back makes a copy of the content
			//and hence forth you want pointer to this copy to ensure consistency
//			jvar_join = &(*(jvar->joinres.end()-1));

			
		} else if (!jvar_join->empty) {
			if (jvar_join->joinres_dim != OBJ_DIMENSION) {
				for (unsigned int i = 0; i < tp->bitmat.common_so_bytes; i++) {
					jvar_join->joinres[i] = jvar_join->joinres[i] & tp->bitmat.objfold[i];
				}
				if (jvar_join->joinres_size != tp->bitmat.common_so_bytes) {
					memset(&jvar_join->joinres[tp->bitmat.common_so_bytes], 0x00, jvar_join->joinres_size - tp->bitmat.common_so_bytes);
					jvar_join->joinres[tp->bitmat.common_so_bytes-1] &= (0xff << (8-(tp->bitmat.num_comm_so%8)));
					jvar_join->joinres_size = tp->bitmat.common_so_bytes;
					jvar_join->joinres_dim = SO_DIMENSION;
				}
			} else {
				assert(jvar_join->joinres_size == tp->bitmat.object_bytes);

				for (unsigned int i = 0; i < jvar_join->joinres_size; i++) {
					jvar_join->joinres[i] = jvar_join->joinres[i] & tp->bitmat.objfold[i];
				}
			}
		}

	} else {
		assert(0);
	}

	if (!jvar_join->empty && count_bits_in_row(jvar_join->joinres, jvar_join->joinres_size) == 0) {
		jvar_join->empty = true;
		if (level.find("s") == string::npos || tp->rollback) {
			cout << "object_dim_join: 0 results" << endl;
			return false;
		} else {
			free(jvar_join->joinres); jvar_join->joinres = NULL;
			jvar_join->joinres_size = 0;
			null_perm.insert(tp->strlevel);
		}
	} else if (jvar_join->empty) {
		if (level.find("s") == string::npos || tp->rollback) {
			cout << "object_dim_join: 0 results after prune_for_jvar for  " << jvar->nodenum << endl;
			return false;
		} else {
			null_perm.insert(tp->strlevel);
		}
	}
	return true;
}
////////////////////////////////////////////////////////////

bool predicate_dim_join(struct node *gnode, struct node *tpgnode, bool first)
{
	TP *tp = (TP *)tpgnode->data;
	JVAR *jvar = (JVAR *)gnode->data;
	string level = tp->strlevel;

	BitVecJoinres *jvar_join = NULL;

	for (vector<BitVecJoinres*>::iterator it = jvar->joinres.begin(); it != jvar->joinres.end(); it++) {
		if ((*it)->strlevel.compare(level) == 0) {
			jvar_join = (*it); break;
		}
	}

	if (tp->bitmat.dimension == PSO_BITMAT || tp->bitmat.dimension == POS_BITMAT) {
		if (tp->unfolded > 0 && tp->numjvars > 1) {
			simple_fold(&tp->bitmat, ROW, tp->bitmat.subfold, tp->bitmat.subject_bytes);
		}

		if (jvar_join == NULL) {
			jvar_join = new BitVecJoinres;
			jvar_join->joinres = (unsigned char *) malloc (tp->bitmat.subject_bytes);
			jvar_join->joinres_size = tp->bitmat.subject_bytes;
//			memset(jvar_join->joinres, 0xff, jvar_join->joinres_size);
			memcpy(jvar_join->joinres, tp->bitmat.subfold, jvar_join->joinres_size);
			jvar_join->empty = false;
			jvar_join->strlevel = level;
			jvar_join->joinres_dim = PRED_DIMENSION;
			jvar->joinres.push_back(jvar_join);
//			delete jvar_join;
			//This is needed because push_back makes a copy of the content
			//and hence forth you want pointer to this copy to ensure consistency
//			jvar_join = &(*(jvar->joinres.end()-1));
			
		} else if (!jvar_join->empty) {
			if (jvar_join->joinres_dim != PRED_DIMENSION) {
				assert(0);
			} else {
				assert(jvar_join->joinres_size == tp->bitmat.subject_bytes);
				for (unsigned int i = 0; i < jvar_join->joinres_size; i++) {
					jvar_join->joinres[i] = jvar_join->joinres[i] & tp->bitmat.subfold[i];
				}
			}
		}

	} else {
		cout << "predicate_dim_join: **** ERROR.. wrong BitMat dimension" << endl;
		assert(0);
	}
	if (!jvar_join->empty && count_bits_in_row(jvar_join->joinres, jvar_join->joinres_size) == 0) {
		jvar_join->empty = true;
		if (level.find("s") == string::npos || tp->rollback) {
			cout << "pred_dim_join: 0 results after prune_for_jvar for  " << jvar->nodenum << endl;
			return false;
		} else {
			free(jvar_join->joinres); jvar_join->joinres = NULL;
			jvar_join->joinres_size = 0;
			null_perm.insert(tp->strlevel);
		}
	} else if (jvar_join->empty) {
		if (level.find("s") == string::npos || tp->rollback) {
			cout << "pred_dim_join: 0 results after prune_for_jvar for  " << jvar->nodenum << endl;
			return false;
		} else {
			null_perm.insert(tp->strlevel);
		}
	}
	return true;
}

////////////////////////////////////////////////////////////

bool subject_dim_join(struct node *jvarnode, struct node *tpgnode, bool first)
{
	JVAR *jvar = (JVAR *)jvarnode->data;
	TP *tp = (TP*)tpgnode->data;
	string level = tp->strlevel;
	BitVecJoinres *jvar_join = NULL;

//	cout << "Subject_dim_join for " << jvar->nodenum << " [" << tp->sub << " " << tp->pred << " " << tp->obj << "] " << level << endl;

	for (vector<BitVecJoinres*>::iterator it = jvar->joinres.begin(); it != jvar->joinres.end(); it++) {
		if ((*it)->strlevel.compare(level) == 0) {
			jvar_join = (*it); break;
		}
	}

	if (tp->bitmat.dimension == SPO_BITMAT) {
//		cout << "Subject_dim_join SPO_BITMAT" << endl;

		if (tp->unfolded > 0 && tp->numjvars > 1) {
			simple_fold(&tp->bitmat, ROW, tp->bitmat.subfold, tp->bitmat.subject_bytes);
		}
		//DEBUG
		//////
/*		unsigned int count = 1;
		cout << "################################" << endl;
		for (unsigned int i = 0; i < tp->bitmat.subject_bytes; i++) {
			if (tp->bitmat.subfold[i] == 0x00) {
				count += 8;
				continue;
			}   
			for (unsigned short j = 0; j < 8; j++, count++) {
				if (tp->bitmat.subfold[i] & (0x80 >> j)) {
					printf("%u\n", count); 
				}   
			}   
		}
		cout << "################################" << endl;*/
	
		///////////////////
		//First do everything at this level
		//Before doing further joins at this level
		//check master levels
		//////////////////
		if (jvar_join == NULL) {
			//The particular level's bitvec doesn't exist
			//Check if the var appears at some other master level
			jvar_join = new BitVecJoinres;
			jvar_join->joinres = (unsigned char *) malloc (tp->bitmat.subject_bytes);
			jvar_join->joinres_size = tp->bitmat.subject_bytes;
//			memset(jvar_join->joinres, 0xff, jvar_join->joinres_size);
			memcpy(jvar_join->joinres, tp->bitmat.subfold, jvar_join->joinres_size);
			jvar_join->empty = false;
			jvar_join->strlevel = level;
			jvar_join->joinres_dim = SUB_DIMENSION;

			jvar->joinres.push_back(jvar_join);
//			delete jvar_join;
			//This is needed because push_back makes a copy of the content
			//and hence forth you want pointer to this copy to ensure consistency
//			jvar_join = &(*(jvar->joinres.end()-1));
			
		} else if (!jvar_join->empty) {
			if (jvar_join->joinres_dim != SUB_DIMENSION) {
				for (unsigned int i = 0; i < tp->bitmat.common_so_bytes; i++) {
					jvar_join->joinres[i] = jvar_join->joinres[i] & tp->bitmat.subfold[i];
				}
				if (jvar_join->joinres_size != tp->bitmat.common_so_bytes) {
					memset(&jvar_join->joinres[tp->bitmat.common_so_bytes], 0x00, jvar_join->joinres_size - tp->bitmat.common_so_bytes);
					jvar_join->joinres[tp->bitmat.common_so_bytes-1] &= (0xff << (8-(tp->bitmat.num_comm_so%8)));
					jvar_join->joinres_size = tp->bitmat.common_so_bytes;
					jvar_join->joinres_dim = SO_DIMENSION;
				}
			} else {
				assert(jvar_join->joinres_size == tp->bitmat.subject_bytes);
				for (unsigned int i = 0; i < jvar_join->joinres_size; i++) {
					jvar_join->joinres[i] = jvar_join->joinres[i] & tp->bitmat.subfold[i];
				}
			}
		}

	} else if (tp->bitmat.dimension == OPS_BITMAT || tp->bitmat.dimension == POS_BITMAT) {
//		cout << "Subject_dim_join OPS_BITMAT" << endl;

		if (tp->unfolded > 0 && tp->numjvars > 1) {
			simple_fold(&tp->bitmat, COLUMN, tp->bitmat.objfold, tp->bitmat.object_bytes);
		}
		//DEBUG
		//////
/*		unsigned int count = 1;
		cout << "################################" << endl;
		for (unsigned int i = 0; i < tp->bitmat.object_bytes; i++) {
			if (tp->bitmat.objfold[i] == 0x00) {
				count += 8;
				continue;
			}   
			for (unsigned short j = 0; j < 8; j++, count++) {
				if (tp->bitmat.objfold[i] & (0x80 >> j)) {
					printf("%u\n", count); 
				}   
			}   
		}
		cout << "################################" << endl;*/
		if (jvar_join == NULL) {
			//The particular level's bitvec doesn't exist
			//Check if the var appears at some other master level
			jvar_join = new BitVecJoinres;
			jvar_join->joinres = (unsigned char *) malloc (tp->bitmat.object_bytes);
			jvar_join->joinres_size = tp->bitmat.object_bytes;
//			memset(jvar_join->joinres, 0xff, jvar_join->joinres_size);
			memcpy(jvar_join->joinres, tp->bitmat.objfold, jvar_join->joinres_size);
			jvar_join->empty = false;
			jvar_join->strlevel = level;
			jvar_join->joinres_dim = SUB_DIMENSION;

			jvar->joinres.push_back(jvar_join);
//			delete jvar_join;
			//This is needed because push_back makes a copy of the content
			//and hence forth you want pointer to this copy to ensure consistency
//			jvar_join = &(*(jvar->joinres.end()-1));
		} else if (!jvar_join->empty) {
			if (jvar_join->joinres_dim != SUB_DIMENSION) {
				for (unsigned int i = 0; i < tp->bitmat.common_so_bytes; i++) {
					jvar_join->joinres[i] = jvar_join->joinres[i] & tp->bitmat.objfold[i];
				}
				if (jvar_join->joinres_size != tp->bitmat.common_so_bytes) {
					memset(&jvar_join->joinres[tp->bitmat.common_so_bytes], 0x00, jvar_join->joinres_size - tp->bitmat.common_so_bytes);
					jvar_join->joinres[tp->bitmat.common_so_bytes-1] &= (0xff << (8-(tp->bitmat.num_comm_so%8)));
					jvar_join->joinres_size = tp->bitmat.common_so_bytes;
					jvar_join->joinres_dim = SO_DIMENSION;
				}
			} else {
				assert(jvar_join->joinres_size == tp->bitmat.object_bytes);

				for (unsigned int i = 0; i < jvar_join->joinres_size; i++) {
					jvar_join->joinres[i] = jvar_join->joinres[i] & tp->bitmat.objfold[i];
				}
			}
		}

	} else {
		cout << "subject_dim_join: **** ERROR.. wrong BitMat dimension " << tp->bitmat.dimension << endl;
		assert(0);
	}
	//////////
	//DEBUG
	//TODO: remove later
//	if (jvar->nodenum == -1) {
//		cout << "--------------- sub_dim_join: set bits in joinarr for " << jvar->nodenum << endl;
//		tp->print(); cout << endl;
//		print_set_bits_in_row(jvar_join->joinres, jvar_join->joinres_size);
//	}
	/////////

	if (!jvar_join->empty && count_bits_in_row(jvar_join->joinres, jvar_join->joinres_size) == 0) {
		jvar_join->empty = true;
		if (tp->rollback || level.find("s") == string::npos) {
			cout << "sub_dim_join: 0 results after prune_for_jvar for  " << jvar->nodenum << " on tp [" << tp->sub << 
				" " << tp->pred << " " <<tp->obj <<"]" << endl;
			return false;
		} else {
			free(jvar_join->joinres); jvar_join->joinres = NULL;
			jvar_join->joinres_size = 0;
			null_perm.insert(tp->strlevel);
		}
	} else if (jvar_join->empty) {
		if (level.find("s") == string::npos || tp->rollback) {
			cout << "sub_dim_join: 0 results after prune_for_jvar for  " << jvar->nodenum << " on tp [" << tp->sub <<
				" " << tp->pred << " " <<tp->obj <<"]" << endl;
			return false;
		} else {
			null_perm.insert(tp->strlevel);
		}
	}
	return true;
}

bool pass_results_across_levels(struct node *jvarnode)
{
	JVAR *jvar = (JVAR *)jvarnode->data;

	vector<struct node *> tpvec;
	LIST *nextTP = jvarnode->nextTP;

	for (; nextTP; nextTP = nextTP->next) {
		tpvec.push_back(nextTP->gnode);
	}

	stable_sort(tpvec.begin(), tpvec.end(), sort_tp_nodes_prune);

	while (tpvec.size() > 0) {
		set<string> equiv_class;
		struct node *gnode = tpvec.front();
		TP *tp = (TP*) gnode->data;

		equiv_class.insert(tp->strlevel);
		tpvec.erase(tpvec.begin());
		for (LIST *next = gnode->nextTP; next; next = next->next) {
			TP *tpn = (TP*) next->gnode->data;
			vector<struct node *>::iterator it = find(tpvec.begin(), tpvec.end(), next->gnode);
			if (it != tpvec.end() &&
				!tpn->isSlave(tp->strlevel) && !tp->isSlave(tpn->strlevel)) {
				equiv_class.insert(tpn->strlevel);
				tpvec.erase(it);
			}
		}
		/////////////
		//DEBUG
		//TODO: remove later
		////////////////
//		cout << "------------- equiv_class-> ";
//		for (set<string>::iterator itr = equiv_class.begin(); itr != equiv_class.end(); itr++) {
//			cout << *itr << " ";
//		}
//		cout << endl;
		//////////////////
		BitVecJoinres *all_res = NULL;
		//Equivalence class of mutual disjoint levels connected through same jvar
		for (set<string>::iterator it = equiv_class.begin(); it != equiv_class.end() && equiv_class.size() > 1;
				it++) {
			string level = (*it);
			bool isEmpty = false;
			for (vector<BitVecJoinres*>::iterator it = jvar->joinres.begin(); it != jvar->joinres.end(); it++) {
				BitVecJoinres *jvar_join = (*it);
				if (level.compare(jvar_join->strlevel) != 0)
					continue;
				//jvar_join corresponding to the 'level'
				assert(jvar_join != NULL);
				if (jvar_join->empty) {
					if (all_res == NULL) {
						all_res = new BitVecJoinres;
						all_res->joinres = NULL;
					} else {
						if (all_res->joinres != NULL) {
							free(all_res->joinres);
							all_res->joinres = NULL;
						}
					}
					all_res->joinres_size = 0;
					all_res->empty = true;
					isEmpty = true; //all results for this equiv class are empty
					break;
				}
				if (all_res == NULL) {
					all_res = new BitVecJoinres;
					all_res->joinres = (unsigned char *) malloc(jvar_join->joinres_size);
					all_res->joinres_size = jvar_join->joinres_size;
					memcpy(all_res->joinres, jvar_join->joinres, jvar_join->joinres_size);
					all_res->empty = false;
					all_res->joinres_dim = jvar_join->joinres_dim;
				} else {
					if (all_res->joinres_dim != jvar_join->joinres_dim) {
						//S-O join
						for (unsigned int i = 0; i < tp->bitmat.common_so_bytes; i++) {
							all_res->joinres[i] = all_res->joinres[i] & jvar_join->joinres[i];
						}
						if (all_res->joinres_size != tp->bitmat.common_so_bytes) {
							memset(&all_res->joinres[tp->bitmat.common_so_bytes], 0x00, all_res->joinres_size - tp->bitmat.common_so_bytes);
							all_res->joinres[tp->bitmat.common_so_bytes-1] &= (0xff << (8-(tp->bitmat.num_comm_so%8)));
							all_res->joinres_size = tp->bitmat.common_so_bytes;
							all_res->joinres_dim = SO_DIMENSION;
						}
					} else {
						assert(all_res->joinres_size == jvar_join->joinres_size);

						for (unsigned int i = 0; i < all_res->joinres_size; i++) {
							all_res->joinres[i] = all_res->joinres[i] & jvar_join->joinres[i];
						}
					}
				}
				break;
			}
			if (isEmpty) break;
		}
		if (all_res == NULL) {
			//no-op for no equiv classes of disjoint levels
		} else if (all_res->empty || count_bits_in_row(all_res->joinres, all_res->joinres_size) == 0) {
			if (all_res->joinres != NULL) {
				free(all_res->joinres);
				all_res->joinres = NULL;
			}
			all_res->joinres_size = 0;
			all_res->empty = true;
		}

		for (set<string>::iterator it = equiv_class.begin(); it != equiv_class.end() && equiv_class.size() > 1;
				it++) {
			string level = (*it);
			for (vector<BitVecJoinres*>::iterator it = jvar->joinres.begin(); it != jvar->joinres.end(); it++) {
				BitVecJoinres *jvar_join = (*it);
				if (level.compare(jvar_join->strlevel) != 0)
					continue;
				assert(jvar_join != NULL);

				if (all_res->empty) {
					if (!jvar_join->empty) {
						free(jvar_join->joinres); jvar_join->joinres = NULL;
						jvar_join->joinres_size = 0;
						jvar_join->empty = true;
						if (jvar_join->strlevel.find("s") == string::npos) {
							cout << "pass_results_across_levels: 0 results after prune_for_jvar for  " << jvar->nodenum << endl;
							return false;
						} else {
							null_perm.insert(jvar_join->strlevel);
						}
					}

					break;
				}

				if (all_res->joinres_dim != jvar_join->joinres_dim) {
					//S-O join
					for (unsigned int i = 0; i < tp->bitmat.common_so_bytes; i++) {
						jvar_join->joinres[i] = all_res->joinres[i] & jvar_join->joinres[i];
					}
					if (jvar_join->joinres_size != tp->bitmat.common_so_bytes) {
						memset(&jvar_join->joinres[tp->bitmat.common_so_bytes], 0x00, jvar_join->joinres_size - tp->bitmat.common_so_bytes);
						jvar_join->joinres[tp->bitmat.common_so_bytes-1] &= (0xff << (8-(tp->bitmat.num_comm_so%8)));
						jvar_join->joinres_size = tp->bitmat.common_so_bytes;
						jvar_join->joinres_dim = SO_DIMENSION;
					}
				} else {
					assert(all_res->joinres_size == jvar_join->joinres_size);

					for (unsigned int i = 0; i < jvar_join->joinres_size; i++) {
						jvar_join->joinres[i] = all_res->joinres[i] & jvar_join->joinres[i];
					}
				}
				break;
			}
		}

	}

	//COMMENT: Pass results across master-slave levels
	//No need to go "levelwise" because jvar->joinres gets populated from master -> slave order
	//so the order is in general preserved
	/////////
	//DEBUG
	//TODO: remove later
	///////
//	for (vector<BitVecJoinres*>::iterator it = jvar->joinres.begin(); it != jvar->joinres.end(); it++) {
//		BitVecJoinres *jvar_join = *it;
//		cout << "---------- BEFORE last_loop in pass_across: jvar " << jvar->nodenum << " for level " << jvar_join->strlevel << endl;
//		print_set_bits_in_row(jvar_join->joinres, jvar_join->joinres_size);
//	}
	//////////////////////////

	for (vector<BitVecJoinres*>::iterator it = jvar->joinres.begin(); it != jvar->joinres.end(); it++) {
		BitVecJoinres *master_join = (*it);
		string master_level = master_join->strlevel;

		for (vector<BitVecJoinres*>::iterator slave = jvar->joinres.begin(); slave != jvar->joinres.end(); slave++) {
			BitVecJoinres *slave_join = (*slave);
			string slave_level = slave_join->strlevel;

			if (master_level.compare(slave_level) == 0)
				continue;

			/////////////////
			//DEBUG:
			//TODO: remove later
			////////////////
//			cout << "-------pass_across: master=" << master_level << " slave=" << slave_level << endl;
			////////////////////////////

			//COMMENT: Optimization opportunity. If there are multiple levels, start from the topmost
			//so that you don't have to do redundant work of passing results across levels that
			//are not directly in parent-child relationship, e.g. if m1 is master of m1s1m1, passing
			//results of m1 to m1s1m1, will ensure that m1's results get passed to m1s1m1s2m1 too.
			TP *slave_tp = (TP*) (levelwise[slave_level][0])->data;
			TP *master_tp = (TP*) (levelwise[master_level][0])->data;

			//tmp2 is not a direct parent of tmp1
			if (slave_tp->isSlave(master_tp->strlevel)) {
				continue;
			}

			/////////////////
			//DEBUG:
			//TODO: remove later
			////////////////
//			cout << "-------pass_across2: master=" << master_level << " slave=" << slave_level << endl;
//			slave_tp->print(); slave_tp->printPseudomasters(); cout << endl;
//			master_tp->print(); master_tp->printPseudomasters(); cout << endl;
			////////////////////////////

			if (slave_tp->isSlave(master_level)) {
				//level1 is slave of level2, and level2 is a direct parent of level1
				/////////////////
				//DEBUG:
				//TODO: remove later
				////////////////
//				cout << "-------pass_across3: " << master_level << " " << slave_level << endl;
				////////////////////////////
				if (master_join->empty) {
					if (!slave_join->empty) {
						free(slave_join->joinres); slave_join->joinres = NULL;
						slave_join->joinres_size = 0;
						slave_join->empty = true;
						if (slave_join->strlevel.find("s") == string::npos) {
							cout << "pass_results_across_levels: 0 results after prune_for_jvar for  " << jvar->nodenum << endl;
							return false;
						} else {
							null_perm.insert(slave_join->strlevel);
						}
					}
					continue;
				}

				if (slave_join->joinres_dim != master_join->joinres_dim) {
					//S-O join
					for (unsigned int i = 0; i < slave_tp->bitmat.common_so_bytes; i++) {
						slave_join->joinres[i] = slave_join->joinres[i] & master_join->joinres[i];
					}
					if (slave_join->joinres_size != slave_tp->bitmat.common_so_bytes) {
						memset(&slave_join->joinres[slave_tp->bitmat.common_so_bytes], 0x00, slave_join->joinres_size - slave_tp->bitmat.common_so_bytes);
						slave_join->joinres[slave_tp->bitmat.common_so_bytes-1] &= (0xff << (8-(slave_tp->bitmat.num_comm_so%8)));
						slave_join->joinres_size = slave_tp->bitmat.common_so_bytes;
						slave_join->joinres_dim = SO_DIMENSION;
					}
				} else {
					assert(slave_join->joinres_size == master_join->joinres_size);
					for (unsigned int i = 0; i < slave_join->joinres_size; i++) {
						slave_join->joinres[i] = slave_join->joinres[i] & master_join->joinres[i];
					}
				}
			}
		}
	}

	/////////
	//DEBUG
	//TODO: remove later
	///////
//	for (vector<BitVecJoinres*>::iterator it = jvar->joinres.begin(); it != jvar->joinres.end(); it++) {
//		BitVecJoinres *jvar_join = *it;
//		cout << "---------- AFTER last_loop in pass_across: jvar " << jvar->nodenum << " for level " << jvar_join->strlevel << endl;
//		print_set_bits_in_row(jvar_join->joinres, jvar_join->joinres_size);
//	}
	//////////////////////////


	return true;

}

////////////////////////////////////////////////////////////
void subject_dim_unfold(JVAR *jvar, TP *tp, bool force, int numpass)
{
//	cout << "Inside subject_dim_unfold " << tp->sub << " " << tp->pred << " " << tp->obj << " numjvars " << tp->numjvars << " numvars "<< tp->numvars << endl;

	BitVecJoinres *jvar_join = NULL;
	string level = tp->strlevel;

	for (vector<BitVecJoinres*>::iterator it = jvar->joinres.begin(); it != jvar->joinres.end(); it++) {
		if ((*it)->strlevel.compare(level) == 0) {
			jvar_join = (*it); break;
		}
	}
//	cout << "subject_dim_unfold: HERE0" << endl;
	assert(jvar_join != NULL);
	if (jvar_join->empty || count_bits_in_row(jvar_join->joinres, jvar_join->joinres_size) == 0) {
		jvar_join->empty = true;
		assert(!tp->rollback);
		tp->bitmat.freebm();
		tp->triplecnt = 0;
		null_perm.insert(level);
		return;
	}

//	cout << "subject_dim_unfold: HERE1" << endl;

	bool changed = false;

	if (tp->bitmat.dimension == SPO_BITMAT) {

		unsigned int limit_bytes = jvar_join->joinres_size > tp->bitmat.subject_bytes ? tp->bitmat.subject_bytes:jvar_join->joinres_size;

		for (unsigned int i = 0; i < limit_bytes; i++) {
			if (jvar_join->joinres[i] ^ tp->bitmat.subfold[i]) {
				changed = true;
				break;
			}
		}
		if (!changed) {
			//take care of trailing 1s
			unsigned int setbits = count_bits_in_row(&tp->bitmat.subfold[limit_bytes],
													tp->bitmat.subject_bytes - limit_bytes);
			if (setbits > 0)
				changed = true;
		}

		if (changed) {
//			cout << "Subject_dim_unfold: Changed" << endl;
			//We do this now as we don't want to fold again when
			//the TP node has only one jvar
			//In which case it won't get unfolded again until this particular
			//jvar is processed
//				if (tp->bitmat.subfold_prev == NULL)
			memcpy(tp->bitmat.subfold, jvar_join->joinres, limit_bytes);
			if (tp->bitmat.subject_bytes > limit_bytes) {
				memset(&tp->bitmat.subfold[limit_bytes], 0, tp->bitmat.subject_bytes - limit_bytes);
			}

		}
//		cout << "subject_dim_unfold: HERE2" << endl;
		//It's not going to make any difference with the new algo so no need
		//to do additional work.
		if (changed && (tp->numjvars > 1 || force)) {
//			cout << "subject_dim_unfold ROW " << tp->toString() << endl;
			simple_unfold(&tp->bitmat, tp->bitmat.subfold, tp->bitmat.subject_bytes, ROW, numpass);
			tp->bitmat.last_unfold = ROW;
//			cout << "subject_dim_unfold done " << tp->toString() << endl;
			tp->unfolded = numpass;
		} else if (numpass > 1) {
//			cout << "subject_dim_unfold counting triples in bitmat " << tp->toString() << endl;
			tp->triplecnt = count_triples_in_bitmat(&tp->bitmat);
//			cout << "Triples in bitmat " << tp->triplecnt << endl;
		}
//		cout << "subject_dim_unfold: HERE3" << endl;

	} else if (tp->bitmat.dimension == OPS_BITMAT || tp->bitmat.dimension == POS_BITMAT) {

//		cout << "subject_dim_unfold: HERE4" << endl;
		unsigned int limit_bytes = jvar_join->joinres_size < tp->bitmat.object_bytes ? jvar_join->joinres_size : tp->bitmat.object_bytes;
		for (unsigned int i = 0; i < limit_bytes; i++) {
			if (jvar_join->joinres[i] ^ tp->bitmat.objfold[i]) {
				changed = true;
				break;
			}
		}
		if (!changed) {
			//take care of trailing 1s
			unsigned int setbits = count_bits_in_row(&tp->bitmat.objfold[limit_bytes],
													tp->bitmat.object_bytes - limit_bytes);
			if (setbits > 0)
				changed = true;
		}

		if (changed) {
//			cout << "Subject_dim_unfold: Changed2" << endl;
			memcpy(tp->bitmat.objfold, jvar_join->joinres, limit_bytes);
			if (tp->bitmat.object_bytes > limit_bytes)
				memset(&tp->bitmat.objfold[limit_bytes], 0, tp->bitmat.object_bytes - limit_bytes);

			//objfold is already consistent with the latest joinres
//				if (tp->numjvars > 1 || (tp->numjvars == 1 && tp->numvars == 1))
		}
		//////////
		//DEBUG
		//TODO: remove later
//		if (jvar->nodenum == -1) {
//			cout << "--------------- set bits in joinarr for " << jvar->nodenum << endl;
//			tp->print(); cout << endl;
//			print_set_bits_in_row(jvar_join->joinres, jvar_join->joinres_size);
//			cout << "--------------- set bits in bitmat_objfold for " << jvar->nodenum << endl;
//			print_set_bits_in_row(tp->bitmat.objfold, tp->bitmat.object_bytes);
//		}
		/////////

//		cout << "subject_dim_unfold: HERE5" << endl;
		if (changed && (tp->numjvars > 1 || force)) {
//			cout << "subject_dim_unfold COLUMN " << tp->toString() << endl;
			simple_unfold(&tp->bitmat, tp->bitmat.objfold, tp->bitmat.object_bytes, COLUMN, numpass);
			tp->bitmat.last_unfold = COLUMN;
//			cout << "subject_dim_unfold done "<< tp->toString()<< endl;
			tp->unfolded = numpass;
		}

	}

}
//		if (changed && (tp->numjvars > 1 || force)) {
////			unfold(&tp->bitmat, comp_obj, comp_obj_size, OBJ_DIMENSION);
////			if (force || tp->numjvars > 1 || (tp->numjvars == 1 && tp->numvars == 1))
//			//TODO: disable this for other queries
//			//you need to make a decision when you want to do this
//			//not needed everytime
//
//			unsigned int columns = count_bits_in_row(tp->bitmat.objfold, tp->bitmat.object_bytes);
//
//			if (tp->bitmat.dimension == OPS_BITMAT && tp->bitmat.bm.size() > columns) {
////			if (tp->bitmat.dimension == OPS_BITMAT) {
//				cout << "Rows more than columns1 " << tp->bitmat.bm.size() << " columns=" << columns << endl;
//				assert(tp->pred > 0);
//
//				vector<struct triple> triplelist;
//
//				list_enctrips_bitmat_new(&tp->bitmat, tp->pred, triplelist);
//
//				clear_rows(&tp->bitmat, true, false, true);
////					tp->bitmat.bm.clear();
//
//				sort(triplelist.begin(), triplelist.end(), sort_triples_inverted);
//
////					cout << "Loading SPO_BITMAT" << endl;
//				shallow_init_bitmat(&tp->bitmat2, tp->bitmat.num_objs, tp->bitmat.num_preds, tp->bitmat.num_subs, tp->bitmat.num_comm_so, SPO_BITMAT);
//
//				load_data_vertically(NULL, triplelist, &tp->bitmat2, NULL, false, true, true);
//				triplelist.clear();
//
//				simple_unfold(&tp->bitmat2, tp->bitmat.objfold, tp->bitmat.object_bytes, ROW);
//				tp->unfolded = true;
//
//				list_enctrips_bitmat_new(&tp->bitmat2, tp->pred, triplelist);
//				clear_rows(&tp->bitmat2, true, false, true);
////					tp->bitmat2.bm.clear();
//
//				sort(triplelist.begin(), triplelist.end(), sort_triples_inverted);
//
////					cout << "Reloading OPS_BITMAT #triples " << triplelist.size() << endl;
//				load_data_vertically(NULL, triplelist, &tp->bitmat, NULL, false, true, true);
//				triplelist.clear();
//
//			} else {
//				cout << "Rows lesser than columns1 " << tp->bitmat.bm.size() << " columns=" << columns << endl;
////				unsigned char *comp_obj = (unsigned char *) malloc (GAP_SIZE_BYTES * tp->bitmat.num_objs);
////				unsigned int comp_obj_size = 0;
////				comp_obj_size = dgap_compress_new(tp->bitmat.objfold, tp->bitmat.object_bytes, comp_obj);
////				simple_unfold(&tp->bitmat, comp_obj, comp_obj_size, COLUMN);
//				simple_unfold(&tp->bitmat, tp->bitmat.objfold, tp->bitmat.object_bytes, COLUMN);
//				tp->unfolded = true;
////				free(comp_obj);
//			}
//		}


////////////////////////////////////////////////////////////
void object_dim_unfold(JVAR *jvar, TP *tp, bool force, int numpass)
{
//	if (DEBUG)
//	cout << "Inside object_dim_unfold " << tp->sub << " " << tp->pred << " " << tp->obj << " numjvars " << tp->numjvars << " numvars "<< tp->numvars << endl;
	BitVecJoinres *jvar_join = NULL;
	string level = tp->strlevel;

	for (vector<BitVecJoinres*>::iterator it = jvar->joinres.begin(); it != jvar->joinres.end(); it++) {
		if ((*it)->strlevel.compare(level) == 0) {
			jvar_join = (*it); break;
		}
	}
	assert(jvar_join != NULL);
	if (jvar_join->empty || count_bits_in_row(jvar_join->joinres, jvar_join->joinres_size) == 0) {
		jvar_join->empty = true;
		assert(!tp->rollback);
		tp->bitmat.freebm();
		tp->triplecnt = 0;
		null_perm.insert(level);
		return;
	}

	bool changed = false;

	if (tp->bitmat.dimension == OPS_BITMAT) {
		unsigned int limit_bytes = jvar_join->joinres_size < tp->bitmat.subject_bytes ? jvar_join->joinres_size : tp->bitmat.subject_bytes;

		for (unsigned int i = 0; i < limit_bytes; i++) {
			if (jvar_join->joinres[i] ^ tp->bitmat.subfold[i]) {
				changed = true;
				break;
			}
		}
		if (!changed) {
			//take care of trailing 1s
			unsigned int setbits = count_bits_in_row(&tp->bitmat.subfold[limit_bytes], tp->bitmat.subject_bytes - limit_bytes);
			if (setbits > 0)
				changed = true;
		}
		if (changed) {
//			cout << "Object_dim_unfold: Changed" << endl;
			memcpy(tp->bitmat.subfold, jvar_join->joinres, limit_bytes);
			if (tp->bitmat.subject_bytes > limit_bytes)
				memset(&tp->bitmat.subfold[limit_bytes], 0, tp->bitmat.subject_bytes - limit_bytes);
//				if (force || tp->numjvars > 1 || (tp->numjvars == 1 && tp->numvars == 1))
		}
		if (changed && (tp->numjvars > 1 || force)) {
//			cout << "object_dim_unfold ROW " << tp->toString() << endl;
			simple_unfold(&tp->bitmat, tp->bitmat.subfold, tp->bitmat.subject_bytes, ROW, numpass);
			tp->bitmat.last_unfold = ROW;
//			cout << "object_dim_unfold done " << tp->toString() << endl;
			tp->unfolded = numpass;
		}  else if (numpass > 1) {
			tp->triplecnt = count_triples_in_bitmat(&tp->bitmat);
		}


	} else if (tp->bitmat.dimension == SPO_BITMAT || tp->bitmat.dimension == PSO_BITMAT) {

//		unsigned char comp_obj[tp->bitmat.gap_size_bytes * tp->bitmat.num_objs];

		unsigned int limit_bytes = jvar_join->joinres_size < tp->bitmat.object_bytes ? jvar_join->joinres_size : tp->bitmat.object_bytes;
		for (unsigned int i = 0; i < limit_bytes; i++) {
			if (jvar_join->joinres[i] ^ tp->bitmat.objfold[i]) {
				changed = true;
				break;
			}
		}
		if (!changed) {
			//take care of trailing 1s
			unsigned int setbits = count_bits_in_row(&tp->bitmat.objfold[limit_bytes], tp->bitmat.object_bytes - limit_bytes);
			if (setbits > 0)
				changed = true;
		}
		if (changed) {
//			cout << "Object_dim_unfold: Changed2" << endl;
			memcpy(tp->bitmat.objfold, jvar_join->joinres, limit_bytes);
			if (tp->bitmat.object_bytes > limit_bytes)
				memset(&tp->bitmat.objfold[limit_bytes], 0, tp->bitmat.object_bytes - limit_bytes);
			//Now objfold is consistent with latest joinres
//				if (tp->numjvars > 1 || (tp->numjvars == 1 && tp->numvars == 1))
		}
		if (changed && (tp->numjvars > 1 || force)) {
//			cout << "object_dim_unfold COLUMN " << tp->toString() << endl;
			simple_unfold(&tp->bitmat, tp->bitmat.objfold, tp->bitmat.object_bytes, COLUMN, numpass);
			tp->bitmat.last_unfold = COLUMN;
//			cout << "object_dim_unfold done " << tp->toString() << endl;
			tp->unfolded = numpass;
		}

//		if (changed && (tp->numjvars > 1 || force)) {
////			if (force || tp->numjvars > 1 || (tp->numjvars == 1 && tp->numvars == 1))
//			unsigned int columns = count_bits_in_row(tp->bitmat.objfold, tp->bitmat.object_bytes);
//			if (tp->bitmat.dimension == SPO_BITMAT && tp->bitmat.bm.size() > columns) {
////				cout << "Rows more than columns rows=" << tp->bitmat.bm.size() << " columns=" << columns << endl;
////			if (tp->bitmat.dimension == SPO_BITMAT)
//	//			assert(tp->pred > 0);
//
//				vector<struct triple> triplelist;
//
//				list_enctrips_bitmat_new(&tp->bitmat, tp->pred, triplelist);
//				clear_rows(&tp->bitmat, true, false, true);
//
//				sort(triplelist.begin(), triplelist.end(), sort_triples_inverted);
//
////					cout << "Loading OPS_BITMAT" << endl;
//				shallow_init_bitmat(&tp->bitmat2, tp->bitmat.num_objs, tp->bitmat.num_preds, tp->bitmat.num_subs, tp->bitmat.num_comm_so, SPO_BITMAT);
//
//				load_data_vertically(NULL, triplelist, &tp->bitmat2, NULL, false, true, true);
//				triplelist.clear();
//
//				simple_unfold(&tp->bitmat2, tp->bitmat.objfold, tp->bitmat.object_bytes, ROW);
//				tp->unfolded = true;
//
////					cout << "# triples in inverted BM after unfold " << count_triples_in_bitmat(&tp->bitmat2, tp->bitmat2.dimension) << endl;
//
//				list_enctrips_bitmat_new(&tp->bitmat2, tp->pred, triplelist);
//				clear_rows(&tp->bitmat2, true, false, true);
//
////					tp->bitmat2.bm.clear();
//
//				sort(triplelist.begin(), triplelist.end(), sort_triples_inverted);
////					cout << "Reloading OPS_BITMAT #triples " << triplelist.size() << endl;
//				load_data_vertically(NULL, triplelist, &tp->bitmat, NULL, false, true, true);
//				triplelist.clear();
//			} else {
//				cout << "Rows smaller than columns rows=" << tp->bitmat.bm.size() << " columns=" << columns << endl;
////				unsigned char *comp_obj = (unsigned char *) malloc (GAP_SIZE_BYTES * tp->bitmat.num_objs);
////				unsigned int comp_obj_size = 0;
////				comp_obj_size = dgap_compress_new(tp->bitmat.objfold, tp->bitmat.object_bytes, comp_obj);
////				simple_unfold(&tp->bitmat, comp_obj, comp_obj_size, COLUMN);
//				simple_unfold(&tp->bitmat, tp->bitmat.objfold, tp->bitmat.object_bytes, COLUMN);
//				tp->unfolded = true;
////				free(comp_obj);
//			}
//		}

	}

}
////////////////////////////////////////////////////////////
void predicate_dim_unfold(JVAR *jvar, TP *tp, bool force, int numpass)
{
	BitVecJoinres *jvar_join = NULL;
	string level = tp->strlevel;

	for (vector<BitVecJoinres*>::iterator it = jvar->joinres.begin(); it != jvar->joinres.end(); it++) {
		if ((*it)->strlevel.compare(level) == 0) {
			jvar_join = (*it); break;
		}
	}
	if (jvar_join->empty || count_bits_in_row(jvar_join->joinres, jvar_join->joinres_size) == 0) {
		jvar_join->empty = true;
		assert(!tp->rollback);
		tp->bitmat.freebm();
		tp->triplecnt = 0;
		null_perm.insert(level);
		return;
	}

	bool changed = false;

	if (tp->bitmat.dimension == PSO_BITMAT || tp->bitmat.dimension == POS_BITMAT) {
		if (jvar_join->joinres_dim != PRED_DIMENSION) {
			cout << "predicate_dim_unfold: **** ERROR Unhandled SP or PO join" << endl;
			assert(0);
		}
		assert(jvar_join->joinres_size == tp->bitmat.subject_bytes);

		for (unsigned int i = 0; i < jvar_join->joinres_size; i++) {
			if (jvar_join->joinres[i] ^ tp->bitmat.subfold[i]) {
				changed = true;
				break;
			}
		}

		if (changed) {
			memcpy(tp->bitmat.subfold, jvar_join->joinres, jvar_join->joinres_size);
		}
		if (changed && (tp->numjvars > 1 || force)) {
			simple_unfold(&tp->bitmat, tp->bitmat.subfold, tp->bitmat.subject_bytes, ROW, numpass);
			tp->bitmat.last_unfold = ROW;
			tp->unfolded = numpass;
		} else if (numpass > 1) {
			tp->triplecnt = count_triples_in_bitmat(&tp->bitmat);
		}
	} else {
		cout << "predicate_dim_unfold: **** ERROR wrong bitmat dim" << endl;
		assert(0);
	}

}
///////////////////////////////////////////////////////
/*
void cleanup_graph(void)
{
	TP *tp;
	JVAR *jvar;
	LIST *next, *prev;

	cout << "**** Cleaning up ConstraintGraph ****" << endl;

	for (int i = 0; i < graph_tp_nodes; i++) {
		prev = graph[MAX_JVARS_IN_QUERY + i].nextadjvar;
		next = prev->next;

		for (int j = 0; j < graph[MAX_JVARS_IN_QUERY + i].numadjvars; j++) {
			free(prev);
			if (next == NULL)
				break;
			prev = next;
			next = prev->next;
		}

		if (graph[MAX_JVARS_IN_QUERY + i].nextTP == NULL)
			continue;

		prev = graph[MAX_JVARS_IN_QUERY + i].nextTP;
		next = prev->next;

		for (int j = 0; j < graph[MAX_JVARS_IN_QUERY + i].numTPs; j++) {
			free(prev);
			if (next == NULL)
				break;
			prev = next;
			next = prev->next;
		}

	}

	for (int i = 0; i < graph_jvar_nodes; i++) {
		
		prev = graph[i].nextTP;
		next = prev->next;

		for (int j = 0; j < graph[i].numTPs; j++) {
			free(prev);
			if (next == NULL)
				break;
			prev = next;
			next = prev->next;
		}

		if (graph[i].nextadjvar == NULL)
			continue;
		prev = graph[i].nextadjvar;
		next = prev->next;

		for (int j = 0; j < graph[i].numadjvars; j++) {
			free(prev);
			if (next == NULL)
				break;
			prev = next;
			next = prev->next;
		}

	}

	for (int i = 0; i < graph_tp_nodes; i++) {
		tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
//		cout << "Clearing BITMATS" << endl;
		if (tp->bitmat.bm != NULL) {
			if (tp->bitmat.single_row) {
				if (tp->bitmat.bm[0] != NULL)
					free(tp->bitmat.bm[0]);
			} else {
				for (unsigned int i = 0; i < tp->bitmat.num_subs; i++) {

					if (tp->bitmat.bm[i] != NULL)
						free(tp->bitmat.bm[i]);
					tp->bitmat.bm[i] = NULL;
				}
			}
			if (tp->bitmat.subfold != NULL)
				free(tp->bitmat.subfold);
			tp->bitmat.subfold = NULL;

			if (tp->bitmat.subfold_prev != NULL)
				free(tp->bitmat.subfold_prev);
			tp->bitmat.subfold_prev = NULL;

			if (tp->bitmat.objfold != NULL)
				free(tp->bitmat.objfold);
			tp->bitmat.objfold = NULL;

			if (tp->bitmat.objfold_prev != NULL)
				free(tp->bitmat.objfold_prev);
			tp->bitmat.objfold_prev = NULL;
		}
		
		free(tp);
	}

	for (int i = 0; i < graph_jvar_nodes; i++) {
//		cout << "Clearing jvar nodes" << endl;
		jvar = (JVAR *)graph[i].data;

//		if (jvar->joinres != NULL)
//			free(jvar->joinres);
//		jvar->joinres = NULL;
//
//		if (jvar->joinres_so != NULL)
//			free(jvar->joinres_so);
//		jvar->joinres_so = NULL;

		free(jvar);

	}

	graph_tp_nodes = 0;
	graph_jvar_nodes = 0;

//	cout << "Clearing rest of the maps" << endl;

	q_to_gr_node_map.erase(q_to_gr_node_map.begin(), q_to_gr_node_map.end());
	tree_edges_map.erase(tree_edges_map.begin(), tree_edges_map.end());
	unvisited.erase(unvisited.begin(), unvisited.end());

}
*/
/////////////////////////////////////////////////
void fix_all_tp_bitmats()
{
	for (int i = 0; i < graph_tp_nodes; i++) {
		TP *tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;

		if (tp->numjvars > 1 || !graph[MAX_JVARS_IN_QUERY + i].isNeeded)
			continue;

		LIST *nextvar = graph[MAX_JVARS_IN_QUERY + i].nextadjvar;

		for (; nextvar; nextvar = nextvar->next) {
			if (null_perm.find(tp->strlevel) == null_perm.end()) {
				if (nextvar->gnode->type == NON_JVAR_NODE)
					continue;

				JVAR *jvar = (JVAR*)nextvar->gnode->data;

				if (tp->sub == jvar->nodenum) {
					subject_dim_unfold((JVAR*)nextvar->gnode->data, tp, true, 3);
				}

				if (tp->pred == jvar->nodenum) {
					predicate_dim_unfold((JVAR*)nextvar->gnode->data, tp, true, 3);
				}

				if (tp->obj == jvar->nodenum) {
					object_dim_unfold((JVAR*)nextvar->gnode->data, tp, true, 3);
				}
			}
		}
//		cout << "fix_all_tp_bitmats: Null perm for this tp " << (null_perm.find(tp->strlevel) == null_perm.end() ? "NOT null":"NULL") << endl;
	}

//	for (int i = 0; i < graph_tp_nodes; i++) {
//
//		TP *tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
//		string tplevel = tp->strlevel;
//
//		if (tp->isSlaveOfAnyone(null_perm)) {
//			null_perm.insert(tp->strlevel);
//			tp->bitmat.freebm();
//			tp->triplecnt = 0;
//			graph[MAX_JVARS_IN_QUERY + i].isNeeded = false;
//			continue;
//		}
//
//		if (!graph[MAX_JVARS_IN_QUERY + i].isNeeded || (null_perm.find(tplevel) != null_perm.end()))
//			continue;
//
////		tp->triplecnt = count_triples_in_bitmat(&tp->bitmat);
//
//		assert(tp->bitmat.num_triples > 0);
//
//		if (tp->numjvars == 1) {
//			if (tp->bitmat.bm.size() > 0) {
//			tp->bitmat.vbm.clear();
//				for (std::list<struct row>::iterator it = tp->bitmat.bm.begin(); it != tp->bitmat.bm.end(); ) {
//					tp->bitmat.vbm.push_back((*it));
//					it = tp->bitmat.bm.erase(it);
//				}
//			}
//			continue;
//		}
//
//		vector<struct twople> twoplelist;
//
//		//Form a 2nd BM for the other dimension
//		if (tp->sub < 0 && tp->sub > MAX_JVARS && tp->pred < 0 && tp->pred > MAX_JVARS) {
//			assert(tp->bitmat.dimension == POS_BITMAT);
//			assert(tp->obj > 0);
////			list_enctrips_bitmat_new(&tp->bitmat, tp->obj, triplelist);
//
//		} else if (tp->sub < 0 && tp->sub > MAX_JVARS && tp->obj < 0 && tp->obj > MAX_JVARS) {
//			assert(tp->bitmat.dimension == SPO_BITMAT || tp->bitmat.dimension == OPS_BITMAT);
//			assert(tp->pred > 0);
//			//Use load_data_vertically with no ondisk option
//			list_enctrips_bitmat_new(&tp->bitmat, tp->pred, twoplelist, NULL);
//		} else if (tp->pred < 0 && tp->pred > MAX_JVARS && tp->obj < 0 && tp->obj > MAX_JVARS) {
//			assert(tp->bitmat.dimension == PSO_BITMAT);
//			assert(tp->sub > 0);
////			list_enctrips_bitmat_new(&tp->bitmat, tp->sub, triplelist);
//		}
//
////		assert(triplelist.size() != 0);
//		cout << "fix_all_tp_bitmats: triplelist.size " << twoplelist.size() << " for tp ";
//		cout << tp->toString() << " tp->bitmat.dim " << tp->bitmat.dimension << endl;// " count_triples " << count_triples_in_bitmat(&tp->bitmat) << endl;
//
//		if (twoplelist.size() > 0) {
//			if (twoplelist.size() != tp->bitmat.num_triples) {
//				cout << "Twoplelist size " << twoplelist.size() << " bitmat-triples " << tp->bitmat.num_triples << endl;
//				assert(0);
//			}
//			sort(twoplelist.begin(), twoplelist.end(), sort_triples_inverted);
//		}
//
//		switch (tp->bitmat.dimension) {
//			case (POS_BITMAT):
//				//List triples in POS bitmat and sort them as SOP
//				//Use load_data_vertically with no ondisk option
////				shallow_init_bitmat(&tp->bitmat2, tp->bitmat.num_objs, tp->bitmat.num_preds, tp->bitmat.num_subs, tp->bitmat.num_comm_so, SOP_BITMAT);
//				cout << "rank_nodes: no-op for dimension " << tp->bitmat.dimension << endl;
//				break;
//			case (PSO_BITMAT):
////				shallow_init_bitmat(&tp->bitmat2, tp->bitmat.num_objs, tp->bitmat.num_preds, tp->bitmat.num_subs, tp->bitmat.num_comm_so, OSP_BITMAT);
//				cout << "rank_nodes: no-op for dimension " << tp->bitmat.dimension << endl;
//				break;
//			case (SPO_BITMAT):
//				assert(twoplelist.size() > 0);
//				shallow_init_bitmat(&tp->bitmat2, tp->bitmat.num_objs, tp->bitmat.num_preds, tp->bitmat.num_subs, tp->bitmat.num_comm_so, OPS_BITMAT);
//				load_data_vertically(NULL, twoplelist, &tp->bitmat2, NULL, false, true, false, NULL);
//				twoplelist.clear();
//				break;
//			case (OPS_BITMAT):
//				assert(twoplelist.size() > 0);
//				shallow_init_bitmat(&tp->bitmat2, tp->bitmat.num_objs, tp->bitmat.num_preds, tp->bitmat.num_subs, tp->bitmat.num_comm_so, SPO_BITMAT);
//				load_data_vertically(NULL, twoplelist, &tp->bitmat2, NULL, false, true, false, NULL);
//				twoplelist.clear();
//				break;
//		}
//
//		//load_data_vertically(filetoread, triplelist, &tp->bitmat2, filetodump, ondisk, invert);
//		cout << "Came here in fix_all_tp_bitmats [" << tp->sub << " " << tp->pred << " " << tp->obj << "]" << endl;
//		if (tp->bitmat.bm.size() > 0) {
////			tp->bitmat.vbm.clear();
//			for (std::list<struct row>::iterator it = tp->bitmat.bm.begin(); it != tp->bitmat.bm.end(); ) {
//				tp->bitmat.vbm.push_back((*it));
//				it = tp->bitmat.bm.erase(it);
//			}
//		}
//		if (tp->bitmat2.bm.size() > 0) {
////			tp->bitmat2.vbm.clear();
//			for (std::list<struct row>::iterator it = tp->bitmat2.bm.begin(); it != tp->bitmat2.bm.end(); ) {
//				tp->bitmat2.vbm.push_back((*it));
//				it = tp->bitmat2.bm.erase(it);
//			}
//		}
//	}
}

void fix_all_tp_bitmats2(vector<struct node *>&tplist)
{
//	cout << "Inside fix_all_tp_bitmats2" << endl;
	vector<struct twople> twoplelist;

	for (int i = 0; i < tplist.size(); i++) {
		TP *tp = (TP*)tplist[i]->data;
		string tplevel = tp->strlevel;

		if (tp->isSlaveOfAnyone(null_perm)) {
			null_perm.insert(tp->strlevel);
			tp->bitmat.freebm();
			tp->triplecnt = 0;
			tplist[i]->isNeeded = false;
			continue;
		}

		if (!tplist[i]->isNeeded || (null_perm.find(tplevel) != null_perm.end()))
			continue;

		assert(tp->bitmat.num_triples > 0);

		if (tp->numjvars == 1) {
			if (tp->bitmat.bm.size() > 0) {
				for (std::list<struct row>::iterator it = tp->bitmat.bm.begin(); it != tp->bitmat.bm.end(); ) {
					tp->bitmat.vbm.push_back((*it));
					it = tp->bitmat.bm.erase(it);
				}
			}
			continue;
		}

		int jvar1 = 0, jvar2 = 0, jvar_cnt = 0;
		int jvar1_dim = 0, jvar2_dim = 0;

		if (tp->sub < 0 && tp->sub > MAX_JVARS) {
			jvar1 = tp->sub; jvar1_dim = SUB_DIMENSION;
			jvar_cnt++;
		}

		if (jvar_cnt != 0 && tp->pred < 0 && tp->pred > MAX_JVARS) {
			jvar2 = tp->pred; jvar2_dim = PRED_DIMENSION;
			jvar_cnt++;
		} else if (tp->pred < 0 && tp->pred > MAX_JVARS) {
			jvar1 = tp->pred; jvar1_dim = PRED_DIMENSION;
			jvar_cnt++;
		}

		if (jvar_cnt < 2 && tp->obj < 0 && tp->obj > MAX_JVARS) {
			if (jvar_cnt == 0) {
				cout << tp->toString() << endl;
				assert(0);
			} else {
				jvar2 = tp->obj; jvar2_dim = OBJ_DIMENSION;
			}
		} else if (jvar_cnt >= 2 && tp->obj < 0 && tp->obj > MAX_JVARS) {
			cout << tp->toString() << endl;
			assert(0);
		}

		bool jvar1_appears = false, jvar2_appears = false;

		for (int j = 0; j < i; j++) {
			TP *prev = (TP*)tplist[j]->data;
			if (prev->sub < 0 && prev->sub > MAX_JVARS && prev->sub == jvar1) {
				jvar1_appears = true;
			} else if (prev->sub < 0 && prev->sub > MAX_JVARS && prev->sub == jvar2) {
				jvar2_appears = true;
			}

			if (prev->pred < 0 && prev->pred > MAX_JVARS && prev->pred == jvar1) {
				jvar1_appears = true;
			} else if (prev->pred < 0 && prev->pred > MAX_JVARS && prev->pred == jvar2) {
				jvar2_appears = true;
			}

			if (prev->obj < 0 && prev->obj > MAX_JVARS && prev->obj == jvar1) {
				jvar1_appears = true;
			} else if (prev->obj < 0 && prev->obj > MAX_JVARS && prev->obj == jvar2) {
				jvar2_appears = true;
			}
		}

		if ((jvar1_appears && jvar2_appears) || (!jvar1_appears && !jvar2_appears) ) {
			if (tp->bitmat.bm.size() > 0) {
				for (std::list<struct row>::iterator it = tp->bitmat.bm.begin(); it != tp->bitmat.bm.end(); ) {
					tp->bitmat.vbm.push_back((*it));
					it = tp->bitmat.bm.erase(it);
				}
			}
		} else {
			int jvar_dim = 0;
			if (jvar1_appears) {
				jvar_dim = jvar1_dim;
			} else if (jvar2_appears) {
				jvar_dim = jvar2_dim;
			} else {
				assert(0);
			}

			twoplelist.clear();

			switch(jvar_dim) {
				case(SUB_DIMENSION):
					if (tp->bitmat.dimension == OPS_BITMAT) {
//						cout << "Changing dimension1 " << tp->toString() << endl;
						//You don't need to list out entire set of triples
						//Instead load opposite dimension bitmat by using 
						if (tp->bitmat.last_unfold == ROW) {
							simple_fold(&tp->bitmat, COLUMN, tp->bitmat.objfold, tp->bitmat.object_bytes);
						} else if (tp->bitmat.last_unfold == COLUMN) {
							simple_fold(&tp->bitmat, ROW, tp->bitmat.subfold, tp->bitmat.subject_bytes);
						}

						/////////////////////
						//DEBUG
//						cout << "--------- fix_all_tp_bitmat2 " << tp->toString() << " subfold" << endl;
//						print_set_bits_in_row(tp->bitmat.subfold, tp->bitmat.subject_bytes);
//						cout << "--------- fix_all_tp_bitmat2 " << tp->toString() << " objfold" << endl;
//						print_set_bits_in_row(tp->bitmat.objfold, tp->bitmat.object_bytes);
//						cout << "--------- fix_all_tp_bitmat2 " << tp->toString() << " triples" << endl;
//						list_enctrips_bitmat_new(&tp->bitmat, tp->pred, twoplelist, NULL);
//						for (vector<struct twople>::iterator it = twoplelist.begin(); it < twoplelist.end(); it++) {
//							cout << (*it).sub << " " << (*it).obj << endl;
//						}
//						twoplelist.clear();

						////////////////////

						unsigned char *row_mask = (unsigned char *) malloc (tp->bitmat.object_bytes);
						memcpy(row_mask, tp->bitmat.objfold, tp->bitmat.object_bytes);
						unsigned int rowmask_size = tp->bitmat.object_bytes;

						unsigned char *column_mask = (unsigned char *) malloc (tp->bitmat.subject_bytes);
						memcpy(column_mask, tp->bitmat.subfold, tp->bitmat.subject_bytes);
						unsigned int columnmask_size = tp->bitmat.subject_bytes;

						tp->bitmat.freebm();
						shallow_init_bitmat(&tp->bitmat, tp->bitmat.num_objs, tp->bitmat.num_preds, tp->bitmat.num_subs, tp->bitmat.num_comm_so, SPO_BITMAT);

						char dumpfile[1024];
						sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_SPO")].c_str());
						load_from_dump_file(dumpfile, tp->pred, &tp->bitmat, true, true, row_mask, rowmask_size, ROW, NULL, 0, false);
						////////////////
						//DEBUG
//						cout << "--------- fix_all_tp_bitmat2 " << tp->toString() << " objfold2" << endl;
//						print_set_bits_in_row(tp->bitmat.objfold, tp->bitmat.object_bytes);
						////////////////////

						simple_unfold(&tp->bitmat, column_mask, columnmask_size, COLUMN, 3);

						free(row_mask);
						free(column_mask);

						////////////////
						//DEBUG
//						simple_fold(&tp->bitmat, COLUMN, tp->bitmat.objfold, tp->bitmat.object_bytes, true);
//						cout << "--------- fix_all_tp_bitmat2 " << tp->toString() << " objfold3" << endl;
//						print_set_bits_in_row(tp->bitmat.objfold, tp->bitmat.object_bytes);
//
//						cout << "--------- fix_all_tp_bitmat2 " << tp->toString() << " triples2" << endl;
//						list_enctrips_bitmat2(&tp->bitmat, tp->pred, twoplelist, NULL);
//						for (vector<struct twople>::iterator it = twoplelist.begin(); it < twoplelist.end(); it++) {
//							cout << (*it).sub << " " << (*it).obj << endl;
//						}
//						twoplelist.clear();
						///////////////////////////////////

//						list_enctrips_bitmat_new(&tp->bitmat, tp->pred, twoplelist, NULL);
					}
					break;
				case(PRED_DIMENSION): //no-op
					break;
				case(OBJ_DIMENSION):
					if (tp->bitmat.dimension == SPO_BITMAT) {
						//You don't need to list out entire set of triples
						//Instead load opposite dimension bitmat by using 
//						cout << "Changing dimension2 " << tp->toString() << endl;
						
						if (tp->bitmat.last_unfold == ROW) {
							simple_fold(&tp->bitmat, COLUMN, tp->bitmat.objfold, tp->bitmat.object_bytes);
						} else if (tp->bitmat.last_unfold == COLUMN) {
							simple_fold(&tp->bitmat, ROW, tp->bitmat.subfold, tp->bitmat.subject_bytes);
						}
						/////////////////////
						//DEBUG
//						cout << "--------- fix_all_tp_bitmat2 " << tp->toString() << " subfold" << endl;
//						print_set_bits_in_row(tp->bitmat.subfold, tp->bitmat.subject_bytes);
//						cout << "--------- fix_all_tp_bitmat2 " << tp->toString() << " objfold"<< endl;
//						print_set_bits_in_row(tp->bitmat.objfold, tp->bitmat.object_bytes);
//						cout << "--------- fix_all_tp_bitmat2 " << tp->toString() << " triples" << endl;
//						list_enctrips_bitmat_new(&tp->bitmat, tp->pred, twoplelist, NULL);
//						for (vector<struct twople>::iterator it = twoplelist.begin(); it < twoplelist.end(); it++) {
//							cout << (*it).sub << " " << (*it).obj << endl;
//						}
//						twoplelist.clear();
//
						////////////////////

						unsigned char *row_mask = (unsigned char *) malloc (tp->bitmat.object_bytes);
						memcpy(row_mask, tp->bitmat.objfold, tp->bitmat.object_bytes);
						unsigned int rowmask_size = tp->bitmat.object_bytes;
						
						unsigned char *column_mask = (unsigned char *) malloc (tp->bitmat.subject_bytes);
						memcpy(column_mask, tp->bitmat.subfold, tp->bitmat.subject_bytes);
						unsigned int columnmask_size = tp->bitmat.subject_bytes;
						
						tp->bitmat.freebm();
						shallow_init_bitmat(&tp->bitmat, tp->bitmat.num_objs, tp->bitmat.num_preds, tp->bitmat.num_subs, tp->bitmat.num_comm_so, OPS_BITMAT);

						char dumpfile[1024];
						sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_OPS")].c_str());
						load_from_dump_file(dumpfile, tp->pred, &tp->bitmat, true, true, row_mask,
								rowmask_size, ROW, NULL, 0, false);

						////////////////
						//DEBUG
//						cout << "--------- fix_all_tp_bitmat2.1 " << tp->toString() << " objfold2" << endl;
//						print_set_bits_in_row(tp->bitmat.objfold, tp->bitmat.object_bytes);
						////////////////////

						simple_unfold(&tp->bitmat, column_mask, columnmask_size, COLUMN, 3);

						free(row_mask);
						free(column_mask);

						////////////////
						//DEBUG
//						simple_fold(&tp->bitmat, COLUMN, tp->bitmat.objfold, tp->bitmat.object_bytes, true);
//						cout << "--------- fix_all_tp_bitmat2.1 " << tp->toString() << " objfold3" << endl;
//						print_set_bits_in_row(tp->bitmat.objfold, tp->bitmat.object_bytes);
//
//						cout << "--------- fix_all_tp_bitmat2.1 " << tp->toString() << " triples2" << endl;
//						FILE *ftmp = fopen("/tmp/triples2", "w");
//						list_enctrips_bitmat2(&tp->bitmat, tp->pred, twoplelist, ftmp);
//						for (vector<struct twople>::iterator it = twoplelist.begin(); it < twoplelist.end(); it++) {
//							cout << (*it).sub << " " << (*it).obj << endl;
//						}
//						twoplelist.clear();
//						fclose(ftmp);
						///////////////////////////////////

//						list_enctrips_bitmat_new(&tp->bitmat, tp->pred, twoplelist, NULL);
					}
					break;
				default:
					break;
			}

			if (twoplelist.size() > 0) {
				if (twoplelist.size() != tp->bitmat.num_triples) {
					cout << "Twoplelist size " << twoplelist.size() << " bitmat->triples " << tp->bitmat.num_triples << endl;
					assert(0);
				}
				sort(twoplelist.begin(), twoplelist.end(), sort_triples_inverted);
				tp->bitmat.freebm();
				tp->bitmat.num_triples = twoplelist.size();
				shallow_init_bitmat(&tp->bitmat, tp->bitmat.num_objs, tp->bitmat.num_preds, tp->bitmat.num_subs, tp->bitmat.num_comm_so, OPS_BITMAT);
				load_data_vertically(NULL, twoplelist, &tp->bitmat, NULL, false, true, false, NULL);
				twoplelist.clear();
			}
			if (tp->bitmat.bm.size() > 0) {
				for (std::list<struct row>::iterator it = tp->bitmat.bm.begin(); it != tp->bitmat.bm.end(); ) {
					tp->bitmat.vbm.push_back((*it));
					it = tp->bitmat.bm.erase(it);
				}
			}
		}
	}

}
////////////////////////////////////////////////////
bool slaves_with_more_than_one_jvar()
{
	for (map<string, vector<struct node *>>::iterator it = levelwise.begin(); it != levelwise.end(); it++) {
		string level = (*it).first;
		if (level.find("s") == string::npos)
			continue;
		vector<struct node *> tplist = (*it).second;
		set<int> jvars;
		for (vector<struct node *>::iterator vit = tplist.begin(); vit != tplist.end(); vit++) {
			TP *tp = (TP *)(*vit)->data;
			if (tp->sub < 0 && tp->sub > MAX_JVARS) {
				jvars.insert(tp->sub);
			}
			if (tp->pred < 0 && tp->pred > MAX_JVARS) {
				jvars.insert(tp->pred);
			}
			if (tp->obj < 0 && tp->obj > MAX_JVARS) {
				jvars.insert(tp->obj);
			}
		}
		if (jvars.size() > 1)
			return true;

	}
	return false;
}

////////////////////////////////////////////////////
void populate_objfold(BitMat *bitmat)
{
	unsigned int rowsize = 0;

	for (std::list<struct row>::iterator it = bitmat->bm.begin(); it != bitmat->bm.end(); it++) {
		unsigned char *data = (*it).data;
		memcpy(&rowsize, data, ROW_SIZE_BYTES);
		dgap_uncompress(data + ROW_SIZE_BYTES, rowsize, bitmat->objfold, bitmat->object_bytes);
	}

}

void enumerate_cycles(struct node *jvar, struct node *parent,
			vector<struct node *> &path, set<struct node *> &visited, vector<vector<struct node*>> &cycles)
{
	visited.insert(jvar);

	for (LIST *next = jvar->nextadjvar; next; next = next->next) {
		struct node *gnbr = next->gnode;
		set<struct node *>::iterator itr = visited.find(gnbr);
		if (gnbr != parent && itr == visited.end()) {
			path.push_back(gnbr);
			enumerate_cycles(gnbr, jvar, path, visited, cycles);
			path.pop_back();
		} else if (gnbr != parent && itr != visited.end()) {
			cycles.push_back(path);
		}
	}
	visited.erase(jvar);
}

//void print_results(FILE *outfile)
//{
//
//	if (selvars.size() == 0)
//		return;
//
//	for (map<int, struct node *>::iterator it = selvars.begin(); it != selvars.end(); it++) {
//		fprintf(outfile, "%d:", it->first);
//	}
//	fprintf(outfile, "\n===========================\n");
//
//	if (resbitvec) {
//		unsigned int count = 0;
//
//		deque<JVAR *> fwdq;
//
//		for (map<int, struct node *>::iterator it = selvars.begin(); it != selvars.end(); it++) {
//			fwdq.push_back((JVAR *)it->second->data);
//		}
//
//		for (unsigned int i = 0; i < distresult_size; i++) {
//			if (distresult[i] == 0x00) {
//				count += 8;
//				continue;
//			}
//			for (unsigned short j = 0; j < 8; j++, count++) {
//				if (distresult[i] & (0x80 >> j)){
//
//					stack<unsigned int> st;
//					unsigned int bitid = count;
//
//					for (deque<JVAR *>::reverse_iterator it = fwdq.rbegin(); it != fwdq.rend(); it++) {
//						st.push((bitid % ((*it)->range)) + (*it)->lowerlim);
//						bitid /= (*it)->range;
//					}
//
//					while (!st.empty()) {
//						fprintf(outfile, "%u:", st.top());
//						st.pop();
//					}
//					fprintf(outfile, "\n");
//
//	//				fprintf(ofstr, "%u\n", count);
//
//				}
//			}
//		}
//	} else {
//		for (ResultSet::const_iterator rit = resset.begin(); rit != resset.end(); rit++) {
//			fprintf(outfile, "%s\n", (*rit).c_str());
//		}
//	}
//
//}

//bool getAndGiveRes(struct node *jvarnode, TP *tporig, BitVecJoinres *jrestmp)
//{
////	TP *tporig = (TP *)tpg->data;
//	LIST *nextTP = jvarnode->nextTP;
//	JVAR *jvar = (JVAR *)jvarnode->data;
//
//	for (; nextTP; nextTP = nextTP->next) {
//		TP *tp = (TP*)nextTP->gnode->data;
//		if (tp == tporig) {
//			LIST *next = nextTP->gnode->nextTP;
//			for (; next; next = next->next) {
//				TP *adjtp = (TP*)next->gnode->data;
//				if ((tp->sub == jvar->nodenum && (tp->sub == adjtp->sub || tp->sub == adjtp->obj))
//					||
//					(tp->pred == jvar->nodenum && tp->pred == adjtp->pred)
//					||
//					(tp->obj == jvar->nodenum && (tp->obj == adjtp->sub || tp->obj == adjtp->obj))
//					){
//					if (tp->strlevel.compare(adjtp->strlevel) != 0 && tp->isSlave(adjtp->strlevel))
//						return false; 
//				}
//
//			}
//
//		}
//	}
//	nextTP = jvarnode->nextTP;
//	for (; nextTP; nextTP = nextTP->next) {
//		TP *tp = (TP*)nextTP->gnode->data;
//		if (tp->strlevel.compare(jrestmp->strlevel) == 0 && !tp->isSlave(tporig->strlevel)) {
//			//Get the nextTPs of the tp
//			LIST *next = nextTP->gnode->nextTP;
//			for (; next; next = next->next) {
//				TP *adjtp = (TP*)next->gnode->data;
//				if ((tp->sub == jvar->nodenum && (tp->sub == adjtp->sub || tp->sub == adjtp->obj))
//					||
//					(tp->pred == jvar->nodenum && tp->pred == adjtp->pred)
//					||
//					(tp->obj == jvar->nodenum && (tp->obj == adjtp->sub || tp->obj == adjtp->obj))
//					){
//					if (tp->strlevel.compare(adjtp->strlevel) != 0 && tp->isSlave(adjtp->strlevel))
//						return false;
//				}
//			}
//		}
//	}
//	return true;
//}
//
///////////////////////////////////////////////////////////////////////
//void AND_with_all_others(struct node *gnode, BitVecJoinres *jvar_join, TP *tp, int bm_dim, int join_dim, bool firstinlevel)
//{
//	//NOTE:jvar_join->strlevel == tp->strlevel;
//	JVAR *jvar = (JVAR *)gnode->data;
//	
//	//Take on all the pruning you can from the master levels here
//	//so that for the other TP nodes at the same level later
//	//you don't have to bother
//	for (vector<BitVecJoinres>::iterator it = jvar->joinres.begin(); it != jvar->joinres.end(); it++) {
//		BitVecJoinres *jrestmp = &(*it);
//		bool getres = false, giveres = false;
//		if (firstinlevel) {
//			if (jrestmp->strlevel.find("s") == string::npos) {
//				//Get this one's results as it's an absolute master
//				getres = true;
//			} else {
//				if (tp->isSlave(jrestmp->strlevel)) {
//					//Get this one's results as it's a slave-master of jvar_join
//					getres = true;
//				} else {
//					getres = getAndGiveRes(gnode, tp, jrestmp);
//					giveres = getres;
//				}
//			}
//		} else if (jrestmp == jvar_join) {
//			getres = true;
//		}
//
////		if (tp->rollback)
////			giveres = true;
//
//		if (!getres && !giveres)
//			continue;
//
//		unsigned int limit_bytes = 0;
//
//		if (jvar_join->joinres_dim != join_dim && jvar_join->joinres_dim != SO_DIMENSION) {
//			memset(&jvar_join->joinres[tp->bitmat.common_so_bytes], 0, jvar_join->joinres_size - tp->bitmat.common_so_bytes);
//			jvar_join->joinres[tp->bitmat.common_so_bytes-1] &= (0xff << (8-(tp->bitmat.num_comm_so%8)));
//			jvar_join->joinres_dim = SO_DIMENSION;
//			limit_bytes = tp->bitmat.common_so_bytes;
//
//		} else if (jvar_join->joinres_dim == SO_DIMENSION) {
//			limit_bytes = tp->bitmat.common_so_bytes;
//		} else if (jvar_join->joinres_dim == join_dim) {
//			limit_bytes = jvar_join->joinres_size;
//		} else
//			assert(0);
// 
//		if (jvar_join->joinres_dim != jrestmp->joinres_dim) {
//			if (getres && jvar_join->joinres_dim != SO_DIMENSION) {
//				memset(&jvar_join->joinres[tp->bitmat.common_so_bytes], 0, jvar_join->joinres_size - tp->bitmat.common_so_bytes);
//				jvar_join->joinres[tp->bitmat.common_so_bytes-1] &= (0xff << (8-(tp->bitmat.num_comm_so%8)));
//				jvar_join->joinres_dim = SO_DIMENSION;
//			}
//			if (giveres && jrestmp->joinres_dim != SO_DIMENSION) {
//				memset(&jrestmp->joinres[tp->bitmat.common_so_bytes], 0, jrestmp->joinres_size - tp->bitmat.common_so_bytes);
//				jrestmp->joinres[tp->bitmat.common_so_bytes-1] &= (0xff << (8-(tp->bitmat.num_comm_so%8)));
//				jrestmp->joinres_dim = SO_DIMENSION;
//			}
//			limit_bytes = tp->bitmat.common_so_bytes;
//
//		}
//		unsigned char *toand = NULL;
//		if (bm_dim == ROW) {
//			toand = tp->bitmat.subfold;
//		} else if (bm_dim == COLUMN) {
//			toand = tp->bitmat.objfold;
//		} else
//			assert(0);
//
//		if (jvar_join == jrestmp) {
//			for (unsigned int i = 0; i < limit_bytes; i++) 
//				jvar_join->joinres[i] = jvar_join->joinres[i] & toand[i];
//
//		} else {
//			if (getres)
//				for (unsigned int i = 0; i < limit_bytes; i++) 
//					jvar_join->joinres[i] = jvar_join->joinres[i] & jrestmp->joinres[i] & toand[i];
//
//			if (giveres)
//				for (unsigned int i = 0; i < limit_bytes; i++) 
//					jrestmp->joinres[i] = jvar_join->joinres[i] & jrestmp->joinres[i]  & toand[i];
//		}
//	}
//}
