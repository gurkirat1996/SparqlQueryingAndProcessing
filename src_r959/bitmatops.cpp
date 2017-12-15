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

#define _LFS_LARGEFILE          1
#define _LFS64_LARGEFILE        1
#define _LFS64_STDIO			1
#define _LARGEFILE64_SOURCE    	1

#include "bitmat.hpp"
#include <thread>
unsigned char *grow;
unsigned int grow_size;
unsigned char *comp_subfold;
unsigned char *comp_objfold;
unsigned int comp_subfold_size;
map<struct node*, struct triple> q_to_gr_node_map;  // Maps query node to the graph triple.
map<struct node*, struct triple> copy_of_resmap;
unsigned int gmaxrowsize;
unsigned long gtotal_size;
unsigned int gtotal_triples;
unsigned int prev_bitset, prev_rowbit;
unsigned int table_col_bytes;
unsigned char *distresult;
unsigned long distresult_size;
ResultSet resset;
map<string, char *> mmap_table;
vector< pair<int, off64_t> > vectfd(8);
char buf_tmp[0xf000000];
char buf_dump[0xf000000];
char buf_table[0xf000000];

//For file buffering
//char buffer[0x8000000];

//BitMat bmorig;
//set of unvisited nodes in the query graph,
//it is useful when you are at a leaf node
//and want to jump to a node which is not a neighbor
//of your parent node in case of following type of Q graph traversal in the
//order of nodes 1, 2, 3, 4, 7,... then you want to jump
//from 7 to 6 or 5.
//
//   1   2   3   4
//   o---o---o---o
//  /   /   /
// o   o   o
// 5   6   7 

//deque<struct node *> unvisited;

/**
 * Comparator (less than) for the map.
 */
struct lt_triple {
	bool operator()(const TP& a, const TP& b) const {
		if((a.sub < b.sub) || (a.sub == b.sub && a.pred < b.pred) || (a.sub == b.sub && a.pred == b.pred && a.obj < b.obj))
			return true;

		return false;
	}
};

template <class ForwardIterator, class T>
ForwardIterator mybinary_search(ForwardIterator first, ForwardIterator last, const T& value)
{
	first = lower_bound(first,last,value);
	if (first != last && !(value < *first))
		return (first);
	else
		return (last);
}

//////////////////////////////////////////////////////////
void init_bitmat(BitMat *bitmat, unsigned int snum, unsigned int pnum, unsigned int onum, unsigned int commsonum, int dimension)
{
//	bitmat->bm = (unsigned char **) malloc (snum * sizeof (unsigned char *));
//	memset(bitmat->bm, 0, snum * sizeof (unsigned char *));
	bitmat->bm.clear();
	bitmat->num_subs = snum;
	bitmat->num_preds = pnum;
	bitmat->num_objs = onum;
	bitmat->num_comm_so = commsonum;

//	row_size_bytes = bitmat->row_size_bytes;
//	gap_size_bytes = bitmat->gap_size_bytes;
	bitmat->dimension = dimension;

	bitmat->subject_bytes = (snum%8>0 ? snum/8+1 : snum/8);
	bitmat->predicate_bytes = (pnum%8>0 ? pnum/8+1 : pnum/8);
	bitmat->object_bytes = (onum%8>0 ? onum/8+1 : onum/8);
	bitmat->common_so_bytes = (commsonum%8>0 ? commsonum/8+1 : commsonum/8);

	bitmat->subfold = (unsigned char *) malloc (bitmat->subject_bytes * sizeof(unsigned char));
	memset(bitmat->subfold, 0, bitmat->subject_bytes * sizeof(unsigned char));
	bitmat->objfold = (unsigned char *) malloc (bitmat->object_bytes * sizeof(unsigned char));
	memset(bitmat->objfold, 0, bitmat->object_bytes * sizeof(unsigned char));
//	bitmat->subfold_prev = NULL;
//	bitmat->objfold_prev = NULL;
	bitmat->single_row = false;
	bitmat->num_triples = 0;

}
/////////////////////////////////////////////////
void init_bitmat_rows(BitMat *bitmat, bool initbm, bool initfoldarr)
{
	if (initfoldarr) {
		bitmat->subfold = (unsigned char *) malloc (bitmat->subject_bytes * sizeof(unsigned char));
		memset(bitmat->subfold, 0, bitmat->subject_bytes * sizeof(unsigned char));
		bitmat->objfold = (unsigned char *) malloc (bitmat->object_bytes * sizeof(unsigned char));
		memset(bitmat->objfold, 0, bitmat->object_bytes * sizeof(unsigned char));
	}
}
///////////////////////////////////////////////////////////
/*
void shallo_copy_bitmat(BitMat *bitmat1, BitMat *bitmat2)
{
	bitmat2->bm = NULL;
	bitmat2->num_subs 		= bitmat1->num_subs;
	bitmat2->num_preds 		= bitmat1->num_preds;
	bitmat2->num_objs 		= bitmat1->num_objs;

	bitmat2->num_comm_so 	= bitmat1->num_comm_so;

	bitmat2->dimension 		= bitmat1->dimension;

	bitmat2->subject_bytes 		= subject_bytes;
	bitmat2->predicate_bytes 	= predicate_bytes;
	bitmat2->object_bytes 		= object_bytes;
	bitmat2->common_so_bytes 	= common_so_bytes;

	bitmat2->subfold 		= NULL;
	bitmat2->objfold 		= NULL;
	bitmat2->subfold_prev 	= NULL;
	bitmat2->objfold_prev 	= NULL;
	bitmat2->single_row 	= false;
}
*/
////////////////////////////////////////////////////////////
/*
unsigned int dgap_compress(unsigned char *in, unsigned int size, unsigned char *out)
{
	unsigned long long i = 0;
	unsigned long count = 0;
	unsigned int idx = 0;

	bool flag = in[0] & 0x80;
//	printf("[%u] ", flag);
	out[0] = flag;
	idx += 1;

	for (i = 0; i < size*8; i++) {
		unsigned char c = 0x80;
		c >>= (i%8);
		if (flag) {
			if (in[i/8] & c) {
				count++;
				if (count == pow(2, (sizeof(unsigned long) << 3)) ) {
					printf("***** ERROR Count reached limit\n");
					fflush(stdout);
					exit (1);
				}
			} else {
				flag = 0;
//				printf("%u ", count);
				memcpy(&out[idx], &count, GAP_SIZE_BYTES);
				idx += GAP_SIZE_BYTES;
				count = 1;
			}
		} else {
			if (!(in[i/8] & c)) {
				count++;
				if (count == pow(2, 8*sizeof(unsigned long))) {
					printf("***** ERROR Count reached limit\n");
					fflush(stdout);
					exit (1);
				}
			} else {
				flag = 1;
//				printf("%u ", count);
				memcpy(&out[idx], &count, GAP_SIZE_BYTES);
				idx += GAP_SIZE_BYTES;
				count = 1;
			}
		}
	}

//	printf("%u ", count);
	memcpy(&out[idx], &count, GAP_SIZE_BYTES);
	idx += GAP_SIZE_BYTES;
//	printf("\n");
	if (idx >= pow(2, 8*ROW_SIZE_BYTES)) {
		printf("**** dgap_compress: ERROR size is greater than 2^%u %u\n", 8*ROW_SIZE_BYTES, idx);
		fflush(stdout);
//		exit(1);
	}
	return idx;
}
*/
////////////////////////////////////////////////////////
unsigned int dgap_compress_new(unsigned char *in, unsigned int size, unsigned char *out)
{
	unsigned int i = 0;
	unsigned int count = 0;
	unsigned int idx = 0;

	bool flag = (in[0] & 0x80 ? 0x01 : 0x00);
	out[0] = (in[0] & 0x80 ? 0x01 : 0x00);
	idx += 1;

	for (i = 0; i < size; i++) {
		if (in[i] == 0x00) {
			//All 0 bits
			if (!flag) {
				count += 8;
			} else {
	//			printf("%u ", count);
				memcpy(&out[idx], &count, GAP_SIZE_BYTES);
				flag = 0;
				idx += GAP_SIZE_BYTES;
				count = 8;
			}
		} else if (in[i] == 0xff) {
			if (flag) {
				count += 8;
			} else {
				memcpy(&out[idx], &count, GAP_SIZE_BYTES);
				flag = 1;
				idx += GAP_SIZE_BYTES;
				count = 8;
			}
		} else {
			//mix of 0s and 1s byte
			for (unsigned short j = 0; j < 8; j++) {
				if (!(in[i] & (0x80 >> j))) {
					//0 bit
					if (!flag) {
						count++;
					} else {
						memcpy(&out[idx], &count, GAP_SIZE_BYTES);
						flag = 0;
						idx += GAP_SIZE_BYTES;
						count = 1;
					}
				} else {
					//1 bit
					if (flag) {
						count++;
					} else {
						memcpy(&out[idx], &count, GAP_SIZE_BYTES);
						flag = 1;
						idx += GAP_SIZE_BYTES;
						count = 1;
					}

				}
				
			}
		}
	}

	//TODO: can omit pushing last 0 bits count
//	if (flag) {
		memcpy(&out[idx], &count, GAP_SIZE_BYTES);
		idx += GAP_SIZE_BYTES;
//	}
//	if (idx >= pow(2, 8*ROW_SIZE_BYTES)) {
//		printf("**** dgap_compress: ERROR size is greater than 2^%u %u\n", 8*ROW_SIZE_BYTES, idx);
//		fflush(stdout);
//		exit(1);
//	}
	return idx;
}

////////////////////////////////////////////////////////////

unsigned long count_bits_in_row(unsigned char *in, unsigned int size)
{
	if (size == 0)
		return 0;

#if USE_MORE_BYTES
	unsigned long count;
#else	
	unsigned int count;
#endif	
	unsigned int i;
//	unsigned char tmp[size];

//	memcpy(tmp, in, size);

	count = 0;
/*	
	for (i = 0; i < size*8; i++) {
		if (tmp[i/8] & 0x01u) {
			count++;
		}
		tmp[i/8] >>= 1;
	}
*/
	for (i = 0; i < size; i++) {
		if (in[i] == 0xff) {
			count += 8;
		} else if (in[i] > 0x00) {
			for (unsigned short j = 0; j < 8; j++) {
				if (in[i] & (0x80 >> j)) {
					count++;
				}
			}
		}
	}

	return count;
}
unsigned int print_set_bits_in_row(unsigned char *in, unsigned int size)
{
	unsigned int count;
	unsigned int i;

	if (in == NULL || size == 0)
		return 0;

	count = 0;
/*	
	for (i = 0; i < size*8; i++) {
		if (tmp[i/8] & 0x01u) {
			count++;
		}
		tmp[i/8] >>= 1;
	}
*/
	for (i = 0; i < size; i++) {
		if (in[i] == 0xff) {
			count += 8;
			for (unsigned short j=1; j<= 8; j++) {
				cout << i*8+j << endl;
			}
		} else if (in[i] > 0x00) {
			for (unsigned short j = 0; j < 8; j++) {
				if (in[i] & (0x80 >> j)) {
					cout << i*8+(j+1) << endl;
					count++;
				}
			}
		}
	}

	return count;
}


////////////////////////////////////////////////////////////
/*
 * The caller function is responsible for appropriate
 * initialization of the out array. It's not memset everytime as
 * in the fold function this property is exploited
 */
void dgap_uncompress(unsigned char *in, unsigned int insize, unsigned char *out, unsigned int outsize)
{
#if USE_MORE_BYTES
	unsigned long tmpcnt = 0, bitcnt = 0;
#else
	unsigned int tmpcnt = 0, bitcnt = 0;
#endif
	unsigned int cnt = 0, total_cnt = 0, bitpos = 0;
	bool flag;
//	unsigned char b;

	total_cnt = (insize-1)/GAP_SIZE_BYTES;
	flag = in[0] & 0x01;
	unsigned char format = in[0] & 0x02;

	while (cnt < total_cnt) {
		memcpy(&tmpcnt, &in[cnt*GAP_SIZE_BYTES+1], GAP_SIZE_BYTES);
//		printf("Inside while loop %u\n", cnt);
		if (format == 0x02) {
			out[(tmpcnt-1)/8] |= (0x80 >> ((tmpcnt-1) % 8));
		} else {
			if (flag) {
				for (bitpos = bitcnt; bitpos < bitcnt+tmpcnt; bitpos++) {
					out[bitpos/8] |= (0x80 >> (bitpos % 8));
				}
			}
		}
		cnt++;
		flag = !flag;
		bitcnt += tmpcnt;
	}
	if (format == 0x02) {
		assert((tmpcnt-1)/8 < outsize);
	} else {
		assert((bitcnt-1)/8 < outsize);
	}
}

////////////////////////////////////////////////////////////

void cumulative_dgap(unsigned char *in, unsigned int size, unsigned char *out)
{
	unsigned int cnt = 0;
	unsigned int total_cnt = (size-1)/GAP_SIZE_BYTES;
#if USE_MORE_BYTES
	unsigned long bitcnt = 0;
	unsigned long tmpcnt = 0;
#else
	unsigned int bitcnt = 0;
	unsigned int tmpcnt = 0;
#endif	

	out[0] = in[0];

	while (cnt < total_cnt) {
		memcpy(&tmpcnt, &in[(cnt)*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);
		bitcnt += tmpcnt;

		memcpy(&out[(cnt)*GAP_SIZE_BYTES + 1], &bitcnt, GAP_SIZE_BYTES);

		cnt++;
	}

}
////////////////////////////////////////////////////////////

void de_cumulative_dgap(unsigned char *in, unsigned int size, unsigned char *out)
{
	unsigned int cnt = 1;
	unsigned int total_cnt = (size-1)/GAP_SIZE_BYTES;
#if USE_MORE_BYTES
	unsigned long bitcnt = 0, tmpcnt = 0;
#else	
	unsigned int bitcnt = 0, tmpcnt = 0;
#endif	

	out[0] = in[0];

	memcpy(&tmpcnt, &in[(cnt-1)*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);
	cnt = 2;
	while (cnt <= total_cnt) {
		memcpy(&bitcnt, &in[(cnt-1)*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);
		tmpcnt = bitcnt - tmpcnt;

		memcpy(&out[(cnt-1)*GAP_SIZE_BYTES + 1], &tmpcnt, GAP_SIZE_BYTES);
		tmpcnt = bitcnt;

		cnt++;
	}

}

////////////////////////////////////////////////////////////
void bitmat_cumulative_dgap(unsigned char **bitmat)
{
	unsigned char *rowptr;
	unsigned int rowsize = 0;
	unsigned int i;

	for (i = 0; i < gnum_subs; i++) {
		if (bitmat[i] == NULL)
			continue;

		rowptr = bitmat[i] + ROW_SIZE_BYTES;
		memcpy(&rowsize, bitmat[i], ROW_SIZE_BYTES);

		cumulative_dgap(rowptr, rowsize, rowptr);
	}
}

////////////////////////////////////////////////////////////
void bitmat_de_cumulative_dgap(unsigned char **bitmat)
{
	unsigned char *rowptr;
	unsigned int rowsize = 0;
	unsigned int i;

	for (i = 0; i < gnum_subs; i++) {
		if (bitmat[i] == NULL)
			continue;

		rowptr = bitmat[i] + ROW_SIZE_BYTES;
		memcpy(&rowsize, bitmat[i], ROW_SIZE_BYTES);

		de_cumulative_dgap(rowptr, rowsize, rowptr);
	}
}

////////////////////////////////////////////////////////////
unsigned int dgap_AND_new(unsigned char *in1, unsigned int size1,
					unsigned char *in2, unsigned int size2, 
					unsigned char *res)
{
	//get the initial flags whether the seq starts
	//with a 0 or 1
	bool start1 = in1[0] & 0x01;
	bool start2 = in2[0] & 0x01;
	unsigned int digit_cnt1 = (size1 - 1)/GAP_SIZE_BYTES;
	unsigned int digit_cnt2 = (size2 - 1)/GAP_SIZE_BYTES;

	if (start1 && start2)
		res[0] = 0x01;
	else 
		res[0] = 0x00;

	unsigned int cd1_pos = 0, cd1_pos_prev;
	unsigned int cd2_pos = 0, cd2_pos_prev;
	unsigned int r_pos = 0;

#if USE_MORE_BYTES
	unsigned long digit1 = 0, digit2 = 0;
#else
	unsigned int digit1 = 0, digit2 = 0;
	unsigned int tdigit1 = 0, tdigit2 = 0;
#endif

	bool flag1, flag2, flag_r, start_r;
	bool start = true;
	
	cd1_pos_prev = cd1_pos;
	cd2_pos_prev = cd2_pos;
	memcpy(&digit1, &in1[cd1_pos*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);
	memcpy(&digit2, &in2[cd2_pos*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);
	flag1 = start1;
	flag2 = start2;
	tdigit1 = digit1;
	tdigit2 = digit2;
	start_r = res[0];

	while (cd1_pos < digit_cnt1 && cd2_pos < digit_cnt2) {
		// no need to memcpy if cd1/2_pos is same
		if (cd1_pos_prev != cd1_pos) {
			memcpy(&tdigit1, &in1[cd1_pos*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);	
			digit1 += tdigit1;
			cd1_pos_prev = cd1_pos;
		}
		if (cd2_pos_prev != cd2_pos) {
			memcpy(&tdigit2, &in2[cd2_pos*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);	
			digit2 += tdigit2;
			cd2_pos_prev = cd2_pos;
		}

		flag_r = ((r_pos % 2) == 0) ? start_r : !start_r;

		// case '00'
		if (!flag1 && !flag2) {
//			cout << "case: 00 digit1 " << digit1 << " digit2 " << digit2 << endl;
			if (digit1 >= digit2) {

				if (start || !flag_r) {
					memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit1, GAP_SIZE_BYTES);
				} else {
					r_pos++;
					memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit1, GAP_SIZE_BYTES);
				}
				cd1_pos++;
				flag1 = !flag1;

				if (digit1 == digit2) {
					cd2_pos++;
					flag2 = !flag2;					
				} else {
					cd2_pos++;
					flag2 = !flag2;					
					while (cd2_pos < digit_cnt2) {
						memcpy(&tdigit2, &in2[cd2_pos*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);
						digit2 += tdigit2;
						if (digit2 > digit1) {
							cd2_pos_prev = cd2_pos;
							break;
						}
						cd2_pos++;
						flag2 = !flag2;					
					}
				}

			} else {
				if (start || !flag_r) {
					memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit2, GAP_SIZE_BYTES);
				} else {
					r_pos++;
					memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit2, GAP_SIZE_BYTES);
				}
				cd2_pos++;
				flag2 = !flag2;

				cd1_pos++;
				flag1 = !flag1;
				while (cd1_pos < digit_cnt1) {
					memcpy(&tdigit1, &in1[cd1_pos*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);
					digit1 += tdigit1;
					if (digit1 > digit2) {
						cd1_pos_prev = cd1_pos;
						break;
					}
					cd1_pos++;
					flag1 = !flag1;
				}

			}
		}
		// case '11'
		else if (flag1 && flag2) {
//			cout << "case: 11 digit1 " << digit1 << " digit2 " << digit2 << endl;
			if (digit1 <= digit2) {
				if (start) {
					memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit1, GAP_SIZE_BYTES);
				} else {
					r_pos++;
					memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit1, GAP_SIZE_BYTES);
				}
				cd1_pos++;
				flag1 = !flag1;

				if (digit1 == digit2) {
					cd2_pos++;
					flag2 = !flag2;
				}

			} else {
				if (start) {
					memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit2, GAP_SIZE_BYTES);
				} else {
					r_pos++;
					memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit2, GAP_SIZE_BYTES);
				}

				cd2_pos++;
				flag2 = !flag2;

			}
		}
		// case '01'
		else if (!flag1 && flag2) {
//			cout << "case: 01 digit1 " << digit1 << " digit2 " << digit2 << endl;
			if (start || !flag_r) {
				memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit1, GAP_SIZE_BYTES);
			} else {
				r_pos++;
				memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit1, GAP_SIZE_BYTES);
			}

			cd1_pos++;
			flag1 = !flag1;

			if (digit1 == digit2) {
				cd2_pos++;
				flag2 = !flag2;
			} else if (digit2 < digit1) {
				cd2_pos++;
				flag2 = !flag2;
				while (cd2_pos < digit_cnt2) {
					memcpy(&tdigit2, &in2[cd2_pos*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);
					digit2 += tdigit2;
					if (digit2 > digit1) {
						cd2_pos_prev = cd2_pos;
						break;
					}
					cd2_pos++;
					flag2 = !flag2;
				}
			}

		}
		// case '10'
		else if (flag1 && !flag2) {
//			cout << "case: 10 digit1 " << digit1 << " digit2 " << digit2 << endl;
			if (start || !flag_r) {
				memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit2, GAP_SIZE_BYTES);
			} else {
				r_pos++;
				memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit2, GAP_SIZE_BYTES);
			}

			cd2_pos++;
			flag2 = !flag2;

			if (digit1 == digit2) {
				cd1_pos++;
				flag1 = !flag1;
			} else if (digit1 < digit2) {
				cd1_pos++;
				flag1 = !flag1;
				while (cd1_pos < digit_cnt1) {
					memcpy(&tdigit1, &in1[cd1_pos*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);
					digit1 += tdigit1;
					if (digit1 > digit2) {
						cd1_pos_prev = cd1_pos;
						break;
					}
					cd1_pos++;
					flag1 = !flag1;
				}

			}

		}

		if (start)
			start = false;

	}
	unsigned int ressize = (r_pos+1) * GAP_SIZE_BYTES + 1;
	de_cumulative_dgap(res, ressize, res);
//	return ((r_pos+1) * GAP_SIZE_BYTES + 1);	
	return (ressize);	

}

unsigned int dgap_AND(unsigned char *in1, unsigned int size1,
					unsigned char *in2, unsigned int size2, 
					unsigned char *res)
{
	//get the initial flags whether the seq starts
	//with a 0 or 1
	bool start1 = in1[0] & 0x01;
	bool start2 = in2[0] & 0x01;
	unsigned int digit_cnt1 = (size1 - 1)/GAP_SIZE_BYTES;
	unsigned int digit_cnt2 = (size2 - 1)/GAP_SIZE_BYTES;

	if (start1 && start2)
		res[0] = 0x01;
	else 
		res[0] = 0x00;

	unsigned int cd1_pos = 0, cd1_pos_prev;
	unsigned int cd2_pos = 0, cd2_pos_prev;
	unsigned int r_pos = 0;

#if USE_MORE_BYTES
	unsigned long digit1 = 0, digit2 = 0;
#else
	unsigned int digit1 = 0, digit2 = 0;
#endif

	bool flag1, flag2, flag_r, start_r;
	bool start = true;
	
	cd1_pos_prev = cd1_pos;
	cd2_pos_prev = cd2_pos;
	memcpy(&digit1, &in1[cd1_pos*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);
	memcpy(&digit2, &in2[cd2_pos*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);
	flag1 = start1;
	flag2 = start2;
	start_r = res[0];

	while (cd1_pos < digit_cnt1 && cd2_pos < digit_cnt2) {
		// no need to memcpy if cd1/2_pos is same
		if (cd1_pos_prev != cd1_pos) {
			memcpy(&digit1, &in1[cd1_pos*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);	
			cd1_pos_prev = cd1_pos;
		}
		if (cd2_pos_prev != cd2_pos) {
			memcpy(&digit2, &in2[cd2_pos*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);	
			cd2_pos_prev = cd2_pos;
		}

		flag_r = ((r_pos % 2) == 0) ? start_r : !start_r;

		// case '00'
		if (!flag1 && !flag2) {
//			cout << "case: 00 digit1 " << digit1 << " digit2 " << digit2 << endl;
			if (digit1 >= digit2) {

				if (start || !flag_r) {
					memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit1, GAP_SIZE_BYTES);
				} else {
					r_pos++;
					memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit1, GAP_SIZE_BYTES);
				}
				cd1_pos++;
				flag1 = !flag1;

				if (digit1 == digit2) {
					cd2_pos++;
					flag2 = !flag2;					
				} else {
					cd2_pos++;
					flag2 = !flag2;					
					while (cd2_pos < digit_cnt2) {
						memcpy(&digit2, &in2[cd2_pos*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);
						if (digit2 > digit1) {
							cd2_pos_prev = cd2_pos;
							break;
						}
						cd2_pos++;
						flag2 = !flag2;					
					}
				}

			} else {
				if (start || !flag_r) {
					memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit2, GAP_SIZE_BYTES);
				} else {
					r_pos++;
					memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit2, GAP_SIZE_BYTES);
				}
				cd2_pos++;
				flag2 = !flag2;

				cd1_pos++;
				flag1 = !flag1;
				while (cd1_pos < digit_cnt1) {
					memcpy(&digit1, &in1[cd1_pos*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);
					if (digit1 > digit2) {
						cd1_pos_prev = cd1_pos;
						break;
					}
					cd1_pos++;
					flag1 = !flag1;
				}

			}
		}
		// case '11'
		else if (flag1 && flag2) {
//			cout << "case: 11 digit1 " << digit1 << " digit2 " << digit2 << endl;
			if (digit1 <= digit2) {
				if (start) {
					memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit1, GAP_SIZE_BYTES);
				} else {
					r_pos++;
					memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit1, GAP_SIZE_BYTES);
				}
				cd1_pos++;
				flag1 = !flag1;

				if (digit1 == digit2) {
					cd2_pos++;
					flag2 = !flag2;
				}

			} else {
				if (start) {
					memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit2, GAP_SIZE_BYTES);
				} else {
					r_pos++;
					memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit2, GAP_SIZE_BYTES);
				}

				cd2_pos++;
				flag2 = !flag2;

			}
		}
		// case '01'
		else if (!flag1 && flag2) {
//			cout << "case: 01 digit1 " << digit1 << " digit2 " << digit2 << endl;
			if (start || !flag_r) {
				memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit1, GAP_SIZE_BYTES);
			} else {
				r_pos++;
				memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit1, GAP_SIZE_BYTES);
			}

			cd1_pos++;
			flag1 = !flag1;

			if (digit1 == digit2) {
				cd2_pos++;
				flag2 = !flag2;
			} else if (digit2 < digit1) {
				cd2_pos++;
				flag2 = !flag2;
				while (cd2_pos < digit_cnt2) {
					memcpy(&digit2, &in2[cd2_pos*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);
					if (digit2 > digit1) {
						cd2_pos_prev = cd2_pos;
						break;
					}
					cd2_pos++;
					flag2 = !flag2;
				}
			}

		}
		// case '10'
		else if (flag1 && !flag2) {
//			cout << "case: 10 digit1 " << digit1 << " digit2 " << digit2 << endl;
			if (start || !flag_r) {
				memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit2, GAP_SIZE_BYTES);
			} else {
				r_pos++;
				memcpy(&res[r_pos*GAP_SIZE_BYTES+1], &digit2, GAP_SIZE_BYTES);
			}

			cd2_pos++;
			flag2 = !flag2;

			if (digit1 == digit2) {
				cd1_pos++;
				flag1 = !flag1;
			} else if (digit1 < digit2) {
				cd1_pos++;
				flag1 = !flag1;
				while (cd1_pos < digit_cnt1) {
					memcpy(&digit1, &in1[cd1_pos*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);
					if (digit1 > digit2) {
						cd1_pos_prev = cd1_pos;
						break;
					}
					cd1_pos++;
					flag1 = !flag1;
				}

			}

		}

		if (start)
			start = false;

	}
	return ((r_pos+1) * GAP_SIZE_BYTES + 1);	

}

////////////////////////////////////////////////////////////
/*
 * It always concatenates to in1, so make sure to have in1
 * large enough.. and returns new in1 size
 */  
unsigned int concatenate_comp_arr(unsigned char *in1, unsigned int size1,
								unsigned char *in2, unsigned int size2)
{
	bool flag1 = in1[0] & 0x01;
	bool flag2 = in2[0] & 0x01;

	unsigned int total_cnt1 = (size1-1)/GAP_SIZE_BYTES;
	unsigned int total_cnt2 = (size2-1)/GAP_SIZE_BYTES;

	bool last_digit_0 = ((!flag1) && (total_cnt1%2)) || ((flag1) && !(total_cnt1%2));
	bool last_digit_1 = (flag1 && (total_cnt1%2)) || ((!flag1) && !(total_cnt1%2));

	bool first_digit_0 = !flag2;
	bool first_digit_1 = flag2;
								
#if USE_MORE_BYTES
	unsigned long tmpcnt1 = 0;
	unsigned long tmpcnt2 = 0;
#else
	unsigned int tmpcnt1 = 0;
	unsigned int tmpcnt2 = 0;
#endif

	//concatenate in case of these conditions
	if ((last_digit_0 && first_digit_0) || (last_digit_1 && first_digit_1)) {
		memcpy(&tmpcnt1, &in1[(total_cnt1-1)*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);
		memcpy(&tmpcnt2, &in2[1], GAP_SIZE_BYTES);

		tmpcnt1 += tmpcnt2;
		memcpy(&in1[(total_cnt1-1)*GAP_SIZE_BYTES + 1], &tmpcnt1, GAP_SIZE_BYTES);
		if (size2 - (GAP_SIZE_BYTES+1) > 0)
			memcpy(&in1[size1], &in2[GAP_SIZE_BYTES+1], size2-(GAP_SIZE_BYTES+1));

		return (size1 + size2 - (GAP_SIZE_BYTES+1));
		
	} else {
		memcpy(&in1[size1], &in2[1], size2-1);
		return (size1 + size2 - 1);
	}

}


////////////////////////////////////////////////////////////
/*
 * cflag is completion flag  
 */  

//TODO: make provision for loading OPS bitmat
/*
void map_to_row_wo_dgap(unsigned char **in, unsigned int pbit, unsigned int obit,
				unsigned int spos, bool cflag, bool start)
{
#ifdef USE_MORE_BYTES	
	unsigned long bitset = 0, tmpcnt = 0;
#else	
	unsigned int bitset = 0, tmpcnt = 0;
#endif	
	unsigned int size = 0;
	unsigned char *rowptr;

//	printf("sbit %u pbit %u obit %u\n", spos, pbit, obit);
	bitset = (pbit - 1) * (gobject_bytes << 3) + obit;

//	printf("prev_bitset %u bitset %u\n", prev_bitset, bitset);

	//Complete earlier row and start a new one
	if (cflag) {
		tmpcnt = gnum_preds * (gobject_bytes << 3) - prev_bitset;
		memcpy(&size, grow, ROW_SIZE_BYTES);

		//for the last 0s
		if (tmpcnt > 0) {
//			printf("last 0s appended\n");
			unsigned char tmp[GAP_SIZE_BYTES+1];
			tmp[0] = 0x00;
			memcpy(&tmp[1], &tmpcnt, GAP_SIZE_BYTES);
			size = concatenate_comp_arr(&grow[ROW_SIZE_BYTES], size, tmp, GAP_SIZE_BYTES+1);
			if (size >= pow(2, 8*ROW_SIZE_BYTES)) {
				printf("**** map_to_row_wo_dgap: ERROR2 size greater than 2^%u %u -- s:%u p:%u o:%u\n", 8*ROW_SIZE_BYTES, size, spos, pbit, obit);
				exit (-1);
			}

		}
		
		//TODO: you need different *in
		if (*in != NULL) {
			printf("*** ERROR: something wrong *in not null\n");
			fflush(stdout);
			exit(-1);
		}
		*in = (unsigned char *) malloc (size + ROW_SIZE_BYTES);
		memcpy(*in, &size, ROW_SIZE_BYTES);
		memcpy(*in+ROW_SIZE_BYTES, &grow[ROW_SIZE_BYTES], size);


		//////////////////////////
		//DEBUG
		//////////////////////////
//		cnt = 0;
//		total_cnt = (size-1)/4;
//
//		rowptr = *in + 2;
//		bitcnt = 0;
//		bool flag = rowptr[0];
//		printf("[%u] ", rowptr[0]);
//		for (cnt = 0; cnt < total_cnt; cnt++, flag = !flag) {
//			memcpy(&tmpcnt, &rowptr[cnt*4+1], 4);
////			printf("%u ", tmpcnt);
//			if (flag)
//				bitcnt += tmpcnt;
//		}
//		printf("\n#triples %u\n", bitcnt);

		///////////////////////////

		if (size > gmaxrowsize)
			gmaxrowsize = size;
		
//		printf("rowsize %u\n", size);
		gtotal_size += size + ROW_SIZE_BYTES;
	}
	if (start) {
//		*in = (unsigned char *) malloc (1024);
		prev_bitset = bitset;
		if (bitset - 1 > 0) {
			grow[ROW_SIZE_BYTES] = 0x00;
			tmpcnt = bitset - 1;
			memcpy(&grow[ROW_SIZE_BYTES+1], &tmpcnt, GAP_SIZE_BYTES);
			tmpcnt = 1;
			memcpy(&grow[ROW_SIZE_BYTES + GAP_SIZE_BYTES + 1], &tmpcnt, GAP_SIZE_BYTES);
			size = 2*GAP_SIZE_BYTES + 1;
			memcpy(grow, &size, ROW_SIZE_BYTES);
		} else {
			grow[ROW_SIZE_BYTES] = 0x01;
			tmpcnt = 1;
			memcpy(&grow[ROW_SIZE_BYTES+1], &tmpcnt, GAP_SIZE_BYTES);
			size = GAP_SIZE_BYTES+1;
			memcpy(grow, &size, ROW_SIZE_BYTES);
		}
	} else {
		unsigned char tmp[GAP_SIZE_BYTES+1];
		memcpy(&size, grow, ROW_SIZE_BYTES);
		//append 0s in between 2 set bits
		if (bitset - prev_bitset > 1) {
//			printf("0s in between 2 bitsets\n");
			tmp[0] = 0x00;
			tmpcnt = bitset - prev_bitset - 1;
			memcpy(&tmp[1], &tmpcnt, GAP_SIZE_BYTES);
//			printf("before concate1\n");
			size = concatenate_comp_arr(&grow[ROW_SIZE_BYTES], size, tmp, GAP_SIZE_BYTES+1);
			if (size >= pow(2, 8*ROW_SIZE_BYTES)) {
				printf("**** map_to_row_wo_dgap: ERROR size greater than 2^%u %u -- s:%u p:%u o:%u\n", 8*ROW_SIZE_BYTES, size, spos, pbit, obit);
				exit (-1);
			}
		} else if (bitset - prev_bitset == 0) {
			//no op
			return;
		}
		//now append the set bit
		tmp[0] = 0x01;
		tmpcnt = 1;
		memcpy(&tmp[1], &tmpcnt, GAP_SIZE_BYTES);
//		memcpy(&size, grow, 2);
//		printf("before concate2\n");
		size = concatenate_comp_arr(&grow[ROW_SIZE_BYTES], size, tmp, GAP_SIZE_BYTES+1);
		if (size >= pow(2, 8*ROW_SIZE_BYTES)) {
			printf("**** map_to_row_wo_dgap: ERROR2 size greater than 2^%u %u -- s:%u p:%u o:%u\n", 8*ROW_SIZE_BYTES, size, spos, pbit, obit);
			exit (-1);
		}
//		printf("after concate2\n");
		memcpy(grow, &size, ROW_SIZE_BYTES);
		prev_bitset = bitset;
	}
	
//	printf("Exiting map_to_row_wo_dgap\n");
}
*/
////////////////////////////////////////////////////////////
unsigned long count_dgaps_in_row(unsigned char *in, unsigned int size)
{
	if (in == NULL)
		return 0;

	unsigned char format = in[0] & 0x02;
	if (format == 0x02)
		return 0;

	return ((size-1)/GAP_SIZE_BYTES);
}
////////////////////////////////////////////////////////////

FILE* map_to_row_wo_dgap_vertical(BitMat *bitmat, unsigned int spos, unsigned int obit,
				unsigned int sprev, bool cflag, bool start, bool ondisk, bool listload, FILE *tmpdump,
				bool changebm)
{
#if USE_MORE_BYTES
	unsigned long bitset = 0, later_0 = 0, ini_0 = 0, mid_0 = 0;
#else	
	unsigned int bitset = 0, later_0 = 0, ini_0 = 0, mid_0 = 0;
#endif	
	unsigned int size = 0;
	unsigned char *rowptr;

	assert(GAP_SIZE_BYTES == sizeof(ini_0));

	bitset = obit;

//	if (ondisk)
//		assert(tmpdump != NULL);

	if (!changebm && bitmat->dimension != PSO_BITMAT && bitmat->dimension != POS_BITMAT && ondisk) {
		bitmat->objfold[(obit-1)/8] |= (0x80 >> ((obit-1) % 8));
	}

	//Complete earlier row and start a new one
	if (cflag) {
//		later_0 = (bitmat->object_bytes << 3) - prev_bitset;
		size = 0;
		memcpy(&size, grow, ROW_SIZE_BYTES);

		//for the last 0s
		//COMMENT: Optimization: you don't need to store last 0s
//		if (later_0 > 0) {
//			unsigned char tmp[GAP_SIZE_BYTES+1];
//			tmp[0] = 0x00;
//			memcpy(&tmp[1], &later_0, GAP_SIZE_BYTES);
//			size = concatenate_comp_arr(&grow[ROW_SIZE_BYTES], size, tmp, GAP_SIZE_BYTES+1);
//			if (size >= pow((double)2, (double)8*ROW_SIZE_BYTES)) {
//				printf("**** map_to_row_wo_dgap_vertical: ERROR2 size greater than 2^%u %u -- s:%u p:%u o:%u\n", 8*ROW_SIZE_BYTES, size, spos, pbit, obit);
//				exit (-1);
//			}
//		}
		
		unsigned int total_setbits = 0, total_gaps = 0;
		total_setbits = count_triples_in_row(&grow[ROW_SIZE_BYTES], size);
		total_gaps = count_dgaps_in_row(&grow[ROW_SIZE_BYTES], size);

		unsigned char *data = NULL;

		if (total_gaps > total_setbits) {
			//Just store the positions of setbits.
//			vector<unsigned int> setbits;
			data = grow + ROW_SIZE_BYTES;
			unsigned char *tmpdata = (unsigned char *) malloc (1 + total_setbits*GAP_SIZE_BYTES + ROW_SIZE_BYTES);
			bool flag = data[0] & 0x01; unsigned char format = data[0] & 0x02;
			if (format == 0x02) assert(0);

			unsigned int cnt = 0, total_bits = 0, gapcnt = 0;
			tmpdata[ROW_SIZE_BYTES] = 0x02; //format
			size = 1;
			while (cnt < total_gaps) {
				memcpy(&gapcnt, &data[cnt*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);
				if (flag) {
					for (unsigned int i = total_bits+1; i <= total_bits+gapcnt; i++) {
						memcpy(&tmpdata[size + ROW_SIZE_BYTES], &i, GAP_SIZE_BYTES);
						size += GAP_SIZE_BYTES;
//						setbits.push_back(i);
					}
				}
				flag = !flag;
				cnt++;
				total_bits += gapcnt;
			}
			assert(size == (1 + total_setbits * GAP_SIZE_BYTES));
			memcpy(tmpdata, &size, ROW_SIZE_BYTES);
			data = tmpdata;
		} else {
			data = (unsigned char *) malloc (size + ROW_SIZE_BYTES);
			memcpy(data, grow, size + ROW_SIZE_BYTES);
		}
//		memcpy(data, &size, ROW_SIZE_BYTES);
//		memcpy(&data[ROW_SIZE_BYTES], &grow[ROW_SIZE_BYTES], size);

		if (tmpdump) {
			unsigned int rowsize = 0;
			memcpy(&rowsize, data, ROW_SIZE_BYTES);
			assert(rowsize == size);
			fwrite(data, rowsize + ROW_SIZE_BYTES, 1, tmpdump);
			free(data);
		} else {
			struct row r = {sprev, data};
			if (listload) {
				bitmat->bm.push_back(r);
			} else {
				bitmat->vbm.push_back(r);
			}
		}

		if (ondisk) {

			if (bitmat->dimension == PSO_BITMAT || bitmat->dimension == POS_BITMAT || comp_folded_arr) {
				unsigned int t = sprev - prev_rowbit;
	//			cout << "sprev " << sprev << " prev_rowbit " << prev_rowbit << " comp_subfold_size " << comp_subfold_size << endl;
				if (comp_subfold_size == 0) {
					//Add some zeros before
					if (t > 0) {
						comp_subfold[ROW_SIZE_BYTES] = 0x00;
						memcpy(&comp_subfold[1+ROW_SIZE_BYTES], &t, GAP_SIZE_BYTES);
						comp_subfold_size += 1 + GAP_SIZE_BYTES;
					} else {
						comp_subfold[ROW_SIZE_BYTES] = 0x01;
						comp_subfold_size += 1;
					}

					t = 1;
					memcpy(&comp_subfold[comp_subfold_size + ROW_SIZE_BYTES], &t, GAP_SIZE_BYTES);
					comp_subfold_size += GAP_SIZE_BYTES;

				} else {
					//Concatenate middle 0s
					unsigned char tmp[1+GAP_SIZE_BYTES];
					if (t - 1 > 0) {
						tmp[0] = 0x00;
						t -= 1;
						memcpy(&tmp[1], &t, GAP_SIZE_BYTES);

						comp_subfold_size = concatenate_comp_arr(&comp_subfold[ROW_SIZE_BYTES], comp_subfold_size, tmp, GAP_SIZE_BYTES+1);
//						assert(comp_subfold_size < pow(2, 8*ROW_SIZE_BYTES));
					}
					//Concatenate a 1
					tmp[0] = 0x01;
					t = 1;
					memcpy(&tmp[1], &t, GAP_SIZE_BYTES);
					comp_subfold_size = concatenate_comp_arr(&comp_subfold[ROW_SIZE_BYTES], comp_subfold_size, tmp, GAP_SIZE_BYTES+1);
//					assert(comp_subfold_size < pow(2, 8*ROW_SIZE_BYTES));
				}
				prev_rowbit = sprev;
			} else {
				bitmat->subfold[(sprev-1)/8] |= (0x80 >> ((sprev-1) % 8));
			}
		}

	}
	if (!changebm) {
		if (start) {
	//		cout << "starting here" << endl;
			prev_bitset = bitset;
			if (bitset - 1 > 0) {
				grow[ROW_SIZE_BYTES] = 0x00;
				ini_0 = bitset - 1;
	//			cout << "before memcpy to grow" << endl;
				memcpy(&grow[ROW_SIZE_BYTES+1], &ini_0, GAP_SIZE_BYTES);
	//			cout << "after memcpy to grow" << endl;
				unsigned int tmpcnt = 1;
				memcpy(&grow[ROW_SIZE_BYTES + GAP_SIZE_BYTES + 1], &tmpcnt, GAP_SIZE_BYTES);
				size = 2*GAP_SIZE_BYTES + 1;
				memcpy(grow, &size, ROW_SIZE_BYTES);
			} else {
				grow[ROW_SIZE_BYTES] = 0x01;
				unsigned int tmpcnt = 1;
				memcpy(&grow[ROW_SIZE_BYTES+1], &tmpcnt, GAP_SIZE_BYTES);
				size = GAP_SIZE_BYTES+1;
				memcpy(grow, &size, ROW_SIZE_BYTES);
			}
		} else {
	//		cout << "not starting here" << endl;
			unsigned char tmp[GAP_SIZE_BYTES+1];
			size = 0;
			memcpy(&size, grow, ROW_SIZE_BYTES);
			//append 0s in between 2 set bits
			if (bitset - prev_bitset > 1) {
				tmp[0] = 0x00;
				mid_0 = bitset - prev_bitset - 1;
				memcpy(&tmp[1], &mid_0, GAP_SIZE_BYTES);
				size = concatenate_comp_arr(&grow[ROW_SIZE_BYTES], size, tmp, GAP_SIZE_BYTES+1);
				if (size >= grow_size)
					assert(0);
				if (size >= pow((double)2, (double)8*ROW_SIZE_BYTES)) {
					printf("**** map_to_row_wo_dgap_vertical: ERROR size greater than 2^%u %u -- s:%u o:%u\n", 8*ROW_SIZE_BYTES, size, spos, obit);
					exit (-1);
				}
			} else if (bitset - prev_bitset == 0) {
				//no op
				return tmpdump;
			}
			//now append the set bit
			tmp[0] = 0x01;
			unsigned int tmpcnt = 1;
			memcpy(&tmp[1], &tmpcnt, GAP_SIZE_BYTES);
			size = concatenate_comp_arr(&grow[ROW_SIZE_BYTES], size, tmp, GAP_SIZE_BYTES+1);
			if (size >= grow_size)
				assert(0);
			if (size >= pow((double)2, (double)8*ROW_SIZE_BYTES)) {
				printf("**** map_to_row_wo_dgap_vertical: ERROR2 size greater than 2^%u %u -- s:%u o:%u\n", 8*ROW_SIZE_BYTES, size, spos, obit);
				exit (-1);
			}
			memcpy(grow, &size, ROW_SIZE_BYTES);
			prev_bitset = bitset;
		}
	}

	return tmpdump;
	
//	cout << "Exiting map_to_row_wo_dgap_vertical" << endl;
}

////////////////////////////////////////////////////////////
//static unsigned int dbg_counter;

/*
void map_to_row_wo_dgap_ops(unsigned char **in, unsigned int pbit, unsigned int obit,
				unsigned int spos, bool cflag, bool start)
{
#ifdef USE_MORE_BYTES	
	unsigned long bitset = 0, tmpcnt = 0;
#else
	unsigned int bitset = 0, tmpcnt = 0;
#endif
	unsigned int size = 0;
	unsigned char *rowptr;

//	printf("sbit %u pbit %u obit %u\n", spos, pbit, obit);
	bitset = (pbit - 1) * (gsubject_bytes << 3) + obit;

	dbg_counter++;

//	printf("prev_bitset %u bitset %u\n", prev_bitset, bitset);

	//Complete earlier row and start a new one
	if (cflag) {
		tmpcnt = gnum_preds * (gsubject_bytes << 3) - prev_bitset;
		memcpy(&size, grow, ROW_SIZE_BYTES);
//		printf("Completion flag set tmpcnt %u prev_bitset %u size %u\n", tmpcnt, prev_bitset, size);

		//for the last 0s
		if (tmpcnt > 0) {
//			printf("last 0s appended\n");
			unsigned char tmp[GAP_SIZE_BYTES+1];
			tmp[0] = 0x00;
			memcpy(&tmp[1], &tmpcnt, GAP_SIZE_BYTES);
//			printf("Before concatenate_comp_arr\n");
			size = concatenate_comp_arr(&grow[ROW_SIZE_BYTES], size, tmp, GAP_SIZE_BYTES+1);
//			printf("After concatenate_comp_arr\n");
		}
		
		//TODO: you need different *in
		if (*in != NULL) {
			printf("*** ERROR: something wrong *in not null for dbg_counter %d\n", dbg_counter);
			fflush(stdout);
			exit(-1);
		}
		*in = (unsigned char *) malloc (size + ROW_SIZE_BYTES);
		memcpy(*in, &size, ROW_SIZE_BYTES);
		memcpy(*in+ROW_SIZE_BYTES, &grow[ROW_SIZE_BYTES], size);

		if (size > gmaxrowsize)
			gmaxrowsize = size;
		
//		printf("rowsize %u\n", size);
		gtotal_size += size + ROW_SIZE_BYTES;
	}
	if (start) {
//		*in = (unsigned char *) malloc (1024);
		prev_bitset = bitset;
		if (bitset - 1 > 0) {
			grow[ROW_SIZE_BYTES] = 0x00;
			tmpcnt = bitset - 1;
			memcpy(&grow[ROW_SIZE_BYTES+1], &tmpcnt, GAP_SIZE_BYTES);
			tmpcnt = 1;
			memcpy(&grow[ROW_SIZE_BYTES + GAP_SIZE_BYTES + 1], &tmpcnt, GAP_SIZE_BYTES);
			size = 2*GAP_SIZE_BYTES + 1;
			memcpy(grow, &size, ROW_SIZE_BYTES);
		} else {
			grow[ROW_SIZE_BYTES] = 0x01;
			tmpcnt = 1;
			memcpy(&grow[ROW_SIZE_BYTES+1], &tmpcnt, GAP_SIZE_BYTES);
			size = GAP_SIZE_BYTES+1;
			memcpy(grow, &size, ROW_SIZE_BYTES);
		}
	} else {
		unsigned char tmp[GAP_SIZE_BYTES+1];
		memcpy(&size, grow, ROW_SIZE_BYTES);
		//append 0s in between 2 set bits
		if (bitset - prev_bitset > 1) {
//			printf("0s in between 2 bitsets\n");
			tmp[0] = 0x00;
			tmpcnt = bitset - prev_bitset - 1;
			memcpy(&tmp[1], &tmpcnt, GAP_SIZE_BYTES);
//			printf("before concate1\n");
			size = concatenate_comp_arr(&grow[ROW_SIZE_BYTES], size, tmp, GAP_SIZE_BYTES+1);
			if (size >= pow(2, 8*ROW_SIZE_BYTES)) {
				printf("**** map_to_row_wo_dgap_ops: ERROR size greater than 2^%u %u -- s:%u p:%u o:%u\n", 8*ROW_SIZE_BYTES, size, spos, pbit, obit);
				exit (-1);
			}
		} else if (bitset - prev_bitset == 0) {
			//no op
			return;
		}
		//now append the set bit
		tmp[0] = 0x01;
		tmpcnt = 1;
		memcpy(&tmp[1], &tmpcnt, GAP_SIZE_BYTES);
//		memcpy(&size, grow, 2);
//		printf("before concate2\n");
		size = concatenate_comp_arr(&grow[ROW_SIZE_BYTES], size, tmp, GAP_SIZE_BYTES+1);
		if (size >= pow(2, 8*ROW_SIZE_BYTES)) {
			printf("**** map_to_row_wo_dgap_ops: ERROR2 size greater than 2^%u %u -- s:%u p:%u o:%u\n", 8*ROW_SIZE_BYTES, size, spos, pbit, obit);
			exit (-1);
		}
//		printf("after concate2\n");
		memcpy(grow, &size, ROW_SIZE_BYTES);
		prev_bitset = bitset;
	}
	
}
*/

///////////////////////////////////////////////////////////

unsigned long count_triples_in_row(unsigned char *in, unsigned int size)
{
	if (in == NULL)
		return 0;

	bool flag = in[0] & 0x01; unsigned char format = in[0] & 0x02;
//	bool flag2;
	unsigned int cnt = 0;
	unsigned int total_cnt = (size-1)/GAP_SIZE_BYTES;
#if USE_MORE_BYTES
	unsigned long triplecnt = 0;
	unsigned long tmpcnt = 0;
#else
	unsigned int triplecnt = 0;
	unsigned int tmpcnt = 0;
#endif	

//	printf("[%u] ", flag);
	while (cnt < total_cnt) {
		memcpy(&tmpcnt, &in[cnt*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);
		if (format == 0x02) {
			triplecnt++;
		} else if (format == 0x00) {
			if (flag) {
				triplecnt += tmpcnt;
			}
		}

		cnt++;
		flag = !flag;
	}
//	printf("\ntriplecnt %u\n", triplecnt);
	return triplecnt;
}

////////////////////////////////////////////////////////////
/*
unsigned int fixed_p_fixed_o (unsigned char *in, unsigned int size, unsigned char *out,
								unsigned int ppos, unsigned int opos)
{
//	printf("Inside fixed_p_fixed_o\n");
#ifdef USE_MORE_BYTES	
	unsigned long obit = (ppos - 1) * (gobject_bytes << 3) + opos;
	unsigned long ini_0 = obit - 1;
	unsigned long later_0 = gnum_preds * (gobject_bytes << 3) - obit;
	unsigned long tmpcnt, bitcnt = 0;
#else	
	unsigned int obit = (ppos - 1) * (gobject_bytes << 3) + opos;
	unsigned int ini_0 = obit - 1;
	unsigned int later_0 = gnum_preds * (gobject_bytes << 3) - obit;
	unsigned int tmpcnt, bitcnt = 0;
#endif	

	bool flag = in[0];
	unsigned int cnt = 0;
	unsigned int total_cnt = (size-1)/GAP_SIZE_BYTES;
	unsigned long t = 1;

	if (obit > 1) {
		out[0] = 0x00;
	} else {
		//obit is at first position
		out[0] = in[0];
		if (out[0]) {
			//obit is set to 1
			memcpy(&out[1], &t, GAP_SIZE_BYTES);
			memcpy(&out[GAP_SIZE_BYTES+1], &later_0, GAP_SIZE_BYTES);
			return (2*GAP_SIZE_BYTES + 1);
		} else {
			//obit is set to 0
//			free(out);
//			out = NULL;
//			later_0 += 1;
//			memcpy(&out[1], &later_0, 4);
			return 0;
		}
	}
	
	while (cnt < total_cnt) {
		memcpy(&tmpcnt, &in[(cnt)*GAP_SIZE_BYTES+1], GAP_SIZE_BYTES);
		bitcnt += tmpcnt;
		if (bitcnt >= obit && flag) {
			memcpy(&out[1], &ini_0, GAP_SIZE_BYTES);
			memcpy(&out[GAP_SIZE_BYTES+1], &t, GAP_SIZE_BYTES);
			if (later_0) {
				memcpy(&out[2*GAP_SIZE_BYTES + 1], &later_0, GAP_SIZE_BYTES);
				return (3*GAP_SIZE_BYTES + 1);
			} else {
				return (2*GAP_SIZE_BYTES + 1);
			}
		} else if (bitcnt >= obit) {
//			free(out);
//			out = NULL;
//			ini_0 += (later_0 + 1);
//			memcpy(&out[1], &ini_0, 4);
			return 0;
		}
		flag = !flag;
		cnt++;
	}
}
*/
////////////////////////////////////////////////////////////
// has got tested

//unsigned int fixed_p_var_o(unsigned char *in, unsigned int size, unsigned char *out,
//							unsigned int ppos)
//{
////	printf("Inside fixed_p_var_o\n");
//#ifdef USE_MORE_BYTES
//	unsigned long ini_0 = (ppos - 1) * (gobject_bytes << 3);
//	unsigned long later_0 = (gnum_preds - ppos) * (gobject_bytes << 3);
//	unsigned long ppos_start_bit = ini_0 + 1, gap;
//	unsigned long ppos_end_bit = ppos_start_bit + (gobject_bytes << 3) - 1;
//	unsigned long tmpcnt, bitcnt = 0;
//	unsigned long t = gobject_bytes *8;
//#else
//	unsigned int ini_0 = (ppos - 1) * (gobject_bytes << 3);
//	unsigned int later_0 = (gnum_preds - ppos) * (gobject_bytes << 3);
//	unsigned int ppos_start_bit = ini_0 + 1, gap;
//	unsigned int ppos_end_bit = ppos_start_bit + (gobject_bytes << 3) - 1;
//	unsigned int tmpcnt, bitcnt = 0;
//	unsigned int t = gobject_bytes *8;
//#endif
//
//	unsigned int idx = 0;
//	bool flag = in[0] & 0x01;
//	unsigned int cnt = 1;
//	unsigned int total_cnt = (size-1)/GAP_SIZE_BYTES;
//	bool inside = false;
//
//	//TODO: change everywhere
//	//ppos_start_bit = ppos_end_bit + 1;
//
//	if (ppos > 1) {
//		out[0] = 0;
//	} else {
//		out[0] = in[0];
//	}
//
//	for (cnt = 1; cnt <= total_cnt; cnt++, flag = !flag) {
//		memcpy(&tmpcnt, &in[(cnt-1)*GAP_SIZE_BYTES+1], GAP_SIZE_BYTES);
//		bitcnt += tmpcnt;
////		printf("bitcnt %u ppost_start %u ppos_end %u\n", bitcnt, ppos_start_bit, ppos_end_bit);
//		if (bitcnt >= ppos_start_bit && bitcnt <= ppos_end_bit) {
//			inside = true;
//			gap = (bitcnt - ppos_start_bit + 1) < tmpcnt ? (bitcnt - ppos_start_bit + 1) : tmpcnt;
//
//			if (gap == gobject_bytes*8 && !flag) {
////				free(out);
////				out = NULL;
//				return 0;
//			} else if (gap == gobject_bytes*8 && flag) {
//				if (ini_0 > 0) {
//					memcpy(&out[idx*GAP_SIZE_BYTES+1], &ini_0, GAP_SIZE_BYTES);
//					idx++;
//				}
//				memcpy(&out[idx*GAP_SIZE_BYTES+1], &gap, GAP_SIZE_BYTES);
//				idx++;
//				if (later_0 > 0) {
//					memcpy(&out[idx*GAP_SIZE_BYTES+1], &later_0, GAP_SIZE_BYTES);
//					idx++;
//				}
//				return (idx*GAP_SIZE_BYTES+1);
//			}
//
//			if (idx == 0) {
//				if (!flag) {
//					ini_0 += gap;
//					memcpy(&out[idx*GAP_SIZE_BYTES+1], &ini_0, GAP_SIZE_BYTES);
//				} else {
//					if (ini_0 > 0) {
//						memcpy(&out[idx*GAP_SIZE_BYTES+1], &ini_0, GAP_SIZE_BYTES);
//						idx++;
//					}
//					memcpy(&out[idx*GAP_SIZE_BYTES+1], &gap, GAP_SIZE_BYTES);
//				}
//				idx++;
//				continue;
//			} else {
//				if (bitcnt == ppos_end_bit) {
//					//this predicate ends here.. so no more processing
//					inside = false;
//					if (!flag) {
//						later_0 += gap;
//					} else {
//						memcpy(&out[idx*GAP_SIZE_BYTES+1], &gap, GAP_SIZE_BYTES);
//						idx++;
//					}
//					if (later_0 > 0) {
//						memcpy(&out[idx*GAP_SIZE_BYTES+1], &later_0, GAP_SIZE_BYTES);
//						idx++;
//					}
//					return (idx*GAP_SIZE_BYTES+1);
//				}
//				memcpy(&out[idx*GAP_SIZE_BYTES+1], &gap, GAP_SIZE_BYTES);
//				idx++;
//			}
//
//		} else if (inside) {
//			//take care of the last gap
//			inside = false;
//			gap = ppos_end_bit - (bitcnt - tmpcnt);
//
//			if (!flag) {
//				later_0 += gap;
//			} else {
//				memcpy(&out[idx*GAP_SIZE_BYTES+1], &gap, GAP_SIZE_BYTES);
//				idx++;
//			}
//			if (later_0 > 0) {
//				memcpy(&out[idx*GAP_SIZE_BYTES+1], &later_0, GAP_SIZE_BYTES);
//				idx++;
//			}
//			return (idx*GAP_SIZE_BYTES+1);
//
//		} else if (bitcnt - tmpcnt < ppos_start_bit && bitcnt > ppos_end_bit) {
//			if (!flag) {
//				//all 0s
////				unsigned int t = num_preds * gobject_bytes *8;
////				memcpy(&out[idx*4+1], &t, 4);
////				idx++;
////				free(out);
////				out = NULL;
//				return 0;
//			} else {
//				//ini_0s then gobject_bytes *8 1s and then later_0s
//				if (ini_0 > 0) {
//					memcpy(&out[idx*GAP_SIZE_BYTES+1], &ini_0, GAP_SIZE_BYTES);
//					idx++;
//				}
//				memcpy(&out[idx*GAP_SIZE_BYTES+1], &t, GAP_SIZE_BYTES);
//				idx++;
//				if (later_0 > 0) {
//					memcpy(&out[idx*GAP_SIZE_BYTES+1], &later_0, GAP_SIZE_BYTES);
//					idx++;
//				}
//				return (idx*GAP_SIZE_BYTES+1);
//			}
//		}
//	}
//
//	return (idx*GAP_SIZE_BYTES+1);
//
//}

////////////////////////////////////////////////////////////

// has got tested
//unsigned int var_p_fixed_o(unsigned char *in, unsigned int size, unsigned char *out,
//							unsigned int opos, unsigned char *and_array, unsigned int and_array_size)
//{
//
//
//   ////////////////////////////////////
//   // DEBUG
//   ////////////////////////////////////
////	unsigned int cnt = 0;
////	unsigned int tmpcnt, bitcnt;
////
////	bitcnt = 0;
////	printf("gobject_bytes * 8 = %u\n", gobject_bytes*8);
////	printf("[%u] ", and_array[0]);
////	while (cnt < (idx-1)/4) {
////		memcpy(&tmpcnt, &and_array[cnt*4+1], 4);
////		bitcnt += tmpcnt;
////		printf("%u ", tmpcnt);
////		cnt++;
////	}
////	printf("\nbitcnt %u %u\n", bitcnt, num_preds*gobject_bytes*8);
//
//	unsigned int out_size = dgap_AND(in, size, and_array, and_array_size, out);
//
////	printf("exiting var_p_fixed_o out_size %u\n", out_size);
//
//	return out_size;
//
//}

////////////////////////////////////////////////////////////

unsigned long count_triples_in_bitmat(BitMat *bitmat)
{
	unsigned int i;
	unsigned char *resptr;
	unsigned int ressize = 0, rowcnt = 0;

//	rowcnt = bitmat->num_subs;

	bitmat->num_triples = 0;

	if (bitmat->bm.size() == 0 && bitmat->vbm.size() == 0) {
		return 0;
	}

	unsigned char *data = NULL;

	for (std::list<struct row>::iterator it = bitmat->bm.begin(); it != bitmat->bm.end(); it++) {
		ressize = 0;
		data = (*it).data;
		memcpy(&ressize, data, ROW_SIZE_BYTES);
//		cout << "Rowid " << (*it).rowid << " size " << ressize << endl;
		bitmat->num_triples += count_triples_in_row(&data[ROW_SIZE_BYTES], ressize);
	}

	if (bitmat->num_triples == 0) {
		for (vector<struct row>::iterator it = bitmat->vbm.begin(); it != bitmat->vbm.end(); it++) {
			ressize = 0;
			data = (*it).data;
			memcpy(&ressize, data, ROW_SIZE_BYTES);
			bitmat->num_triples += count_triples_in_row(&data[ROW_SIZE_BYTES], ressize);
		}
	}

//	printf("Total triples %u\n", bitmat->num_triples);

	return bitmat->num_triples;

}
////////////////////////////////////////////////////////////
/*
 * Rule
 * folded S is always foldarr[NUM_OF_ALL_SUB/8]
 * folded P is always foldarr[NUM_OF_ALL_PRED/8]
 * folded O is always foldarr[object_bytes]
 * the caller function has to make sure that the foldarr
 * array is initialized properly
 */
//assumption - row_id starts from 1
void func_simple_fold(BitMat *bitmat, int ret_dimension, unsigned char *foldarr, unsigned int foldarr_size,std::list<struct row>::iterator it,long long block_sz){
	unsigned int rowbit;
	while(block_sz--){
		rowbit = (*it).rowid - 1;
		assert(rowbit/8 < foldarr_size);
		foldarr[rowbit/8] |= (0x80 >> (rowbit%8));
		advance(it, 1);
	}
}

void func_simple_fold2(BitMat *bitmat, int ret_dimension, unsigned char *foldarr, unsigned int foldarr_size,std::list<struct row>::iterator it,long long block_sz){

	while(block_sz--){
		unsigned char *data = (*it).data;
		unsigned rowsize = 0;
		memcpy(&rowsize, data, ROW_SIZE_BYTES);

		dgap_uncompress(data + ROW_SIZE_BYTES, rowsize, foldarr, foldarr_size);
		it++;
	}

}


void simple_fold(BitMat *bitmat, int ret_dimension, unsigned char *foldarr, unsigned int foldarr_size)
{
//	printf("Inside fold\n");
	memset(foldarr, 0, foldarr_size);

	if (ret_dimension == ROW) {
		if (bitmat->last_unfold == ROW) {
			assert(foldarr_size == bitmat->subject_bytes);
			memcpy(foldarr, bitmat->subfold, bitmat->subject_bytes);
		} else {
			long long sz = bitmat->bm.size(),block_sz;
			long long no_threads = 8;
			block_sz = (sz/no_threads) ;

			std::thread myThread[8];

			// "it[i]" and "endit[i]" are the iterators corresponding to the first and the last element to be operated by thread "i"
			std::list<struct row>::iterator it[no_threads], endit[no_threads];

			it[0] = bitmat->bm.begin();
			endit[0] = it[0];
			
			//------in the i'th iteration, temp_sz is the size of the chunk on which thread "i" operates
			//------in the i'th iteration, tot_sz is the size that has been processed by the previous threads
			long long temp_sz,tot_sz=0;
			// advance(endit[0], temp_sz-1);
			// while(true){
			// 	if( (endit[0]+1)!=bitmat->bm.end() &&  (*(endit[0] +1).rowid -1)/8 == (*(endit[0]).rowid -1)/8 ){
			// 		endit[0]++;
			// 		temp_sz ++ ;
			// 	}
			// 	else break;
			// }

			// myThread[0] = thread(func_simple_fold,bitmat,ret_dimension,foldarr,foldarr_size,it[0],temp_sz);


			for(long long i=0;i<no_threads;i++){

				if(i==0){
					it[0] = bitmat->bm.begin();
					endit[0] = it[0];
				}
				else{
					it[i] = next(endit[i-1],1);  
					endit[i]= it[i];
				}
				
				temp_sz = min(block_sz, sz-tot_sz);
	    		advance(endit[i], temp_sz-1);
	    		while(true){
					if( next(endit[i],1)!=bitmat->bm.end() &&  ((*(next(endit[i],1))).rowid -1)/8 == ((*(endit[i])).rowid -1)/8 ){
						endit[i]++;
						temp_sz ++ ;
					}
					else break;
				}
	    		myThread[i] =thread(func_simple_fold,bitmat,ret_dimension,foldarr,foldarr_size,it[i],temp_sz);
	    		tot_sz += temp_sz;
			}

			for(long long i=0;i<no_threads;i++)
  				myThread[i].join();

			/*
			for (std::list<struct row>::iterator it = bitmat->bm.begin(); it != bitmat->bm.end(); it++) {
				unsigned int rowbit = (*it).rowid - 1;
				assert(rowbit/8 < foldarr_size);
				foldarr[rowbit/8] |= (0x80 >> (rowbit%8));
			}
			*/
		}


	} else if (ret_dimension == COLUMN) {
		if (bitmat->last_unfold == COLUMN) {
			assert(foldarr_size == bitmat->object_bytes);
			memcpy(foldarr, bitmat->objfold, bitmat->object_bytes);
		} else {
			long long sz = bitmat->bm.size(),block_sz;
			long long no_threads = 8;

			block_sz = (sz/no_threads) + (sz%no_threads != 0);
			std::thread myThread[no_threads];
			auto it = bitmat->bm.begin();
			unsigned char *temp[no_threads];//[foldarr_size];
			for(int i=0; i<no_threads; i++) temp[i]	 = (unsigned char *)malloc(foldarr_size*sizeof(unsigned char));

			memset(temp, 0, sizeof(temp));

			myThread[0] =thread(func_simple_fold2,bitmat,ret_dimension,temp[0],foldarr_size,it,min(block_sz,sz - block_sz*(0)));

			for(long long i=1;i<no_threads;i++){
	    		advance(it, block_sz);
	    		myThread[i]= thread(func_simple_fold2,bitmat,ret_dimension,temp[i],foldarr_size,it,min(block_sz,sz - block_sz*(i)));
			}

			for(long long i=0;i<no_threads;i++)
  				myThread[i].join();

			for(long long i=0;i<foldarr_size;i++){
				for(long long j=0;j<no_threads;j++){
					foldarr[i] |= temp[j][i];
				}
			}

			/*
			for (std::list<struct row>::iterator it = bitmat->bm.begin(); it != bitmat->bm.end(); it++) {
				unsigned char *data = (*it).data;
				unsigned rowsize = 0;
				memcpy(&rowsize, data, ROW_SIZE_BYTES);

				dgap_uncompress(data + ROW_SIZE_BYTES, rowsize, foldarr, foldarr_size);

			}
			*/
		}
	} else {
		cout << "simple_fold: **** ERROR unknown dimension " << ret_dimension << endl;
		assert(0);
	}
}


void simple_unfold_func(BitMat *bitmat, unsigned char *maskarr, unsigned int maskarr_size,
					int dimension, int numpass, std::list<struct row>::iterator it,long long block_sz)
{
		unsigned int andres_size = 0;
		unsigned int maskarr_bits = count_bits_in_row(maskarr, maskarr_size);
		unsigned char *andres = (unsigned char *) malloc (GAP_SIZE_BYTES * 2 * maskarr_bits + ROW_SIZE_BYTES + 1 + 1024);

		unsigned int orig_bm_triples = bitmat->num_triples;

		while(block_sz--) {
			unsigned char *data  = (*it).data;
			unsigned int rowsize = 0;
			memcpy(&rowsize, data, ROW_SIZE_BYTES);
			data += ROW_SIZE_BYTES;
			unsigned int cnt = 0, total_cnt = (rowsize-1)/GAP_SIZE_BYTES, tmpcnt = 0, triplecnt = 0,
					prev1_bit = 0, prev0_bit = 0, tmpval = 0;
			bool flag = data[0] & 0x01, begin = true, p1bit_set = false, p0bit_set = false;//, done = false;
			unsigned char format = data[0] & 0x02;
			if (format == 0x02) {
				begin = false;
				andres[ROW_SIZE_BYTES] = 0x02;
				andres_size = ROW_SIZE_BYTES + 1;
			}

//			cout << "cnt " << cnt << " total_cnt " << total_cnt << " rowid " << (*it).rowid << endl;
//			printf("format %x\n", format);

			while (cnt < total_cnt) {
				//tmpcnt is going to be the position of 1s in case of alternate format,
				//positions begin with index 1, and NOT with 0
				memcpy(&tmpcnt, &data[cnt*GAP_SIZE_BYTES+1], GAP_SIZE_BYTES);
//				cout << "triplecnt " << triplecnt << " tmpcnt " << tmpcnt << endl;

				if (format == 0x02) {
					if ((tmpcnt-1)/8 >= maskarr_size) {
//						if (numpass == 1) {
//							break;
//						}
						bitmat->num_triples--;
					} else if (maskarr[(tmpcnt-1)/8] & (0x80 >> ((tmpcnt-1)%8))) {
						//The bit (position) under consideration is present in maskarr
						memcpy(&andres[andres_size], &tmpcnt, GAP_SIZE_BYTES);
						andres_size += GAP_SIZE_BYTES;
					} else {
						bitmat->num_triples--;
					}
				} else if (format == 0x00) {
					//triplecnt is number of bits already read from gaps.
					//if maskarr size is already overshot by the number of bits read
					//then we don't need to do anything.
//					if (numpass == 1 && triplecnt/8 >= maskarr_size) {
//						break;
//					}
					if (flag) {
						if (triplecnt/8 >= maskarr_size) {
							bitmat->num_triples -= tmpcnt;
							//p0bit_set = true;
							prev0_bit = triplecnt + tmpcnt - 1;
//							cout << "prev0_bit set1 " << prev0_bit << endl;
						} else {
							for (unsigned int i = triplecnt; i < triplecnt + tmpcnt; i++) {
								if (i/8 >= maskarr_size) {
									//An existing bit is unset to due to maskarr
									//so decreasing the triple count
									bitmat->num_triples--;
									assert(p0bit_set || p1bit_set);
									if (!p0bit_set && p1bit_set) {
										tmpval = prev1_bit + 1;
										memcpy(&andres[andres_size], &tmpval, GAP_SIZE_BYTES);
										andres_size += GAP_SIZE_BYTES;
										p0bit_set = true;
									} else if (prev1_bit > prev0_bit) {
										tmpval = prev1_bit - prev0_bit;
										memcpy(&andres[andres_size], &tmpval, GAP_SIZE_BYTES);
										andres_size += GAP_SIZE_BYTES;
									}
									prev0_bit = i;
//									cout << "prev0_bit set2 " << prev0_bit << endl;
//									if (numpass == 1) {
//										done = true;
//										break;
//									}
								} else if ((maskarr[i/8] & (0x80 >> (i%8))) == 0x00) {
									//An existing bit is unset to due to maskarr
									bitmat->num_triples--;
									if (begin) {
										begin = false;
										andres[ROW_SIZE_BYTES] = 0x00;
										andres_size = ROW_SIZE_BYTES +1;
										p0bit_set = true;
									} else {
										if (!p0bit_set) {//this means all prev bits are 1s
											//pushing previous # of 1s
											memcpy(&andres[andres_size], &i, GAP_SIZE_BYTES);
											andres_size += GAP_SIZE_BYTES;
											p0bit_set = true;
										} else if (prev0_bit != (i - 1)) {//this means there are one or more 1s between this 0 bit and prev one
											//pushing previous # of 1s
											tmpval = i - prev0_bit -1;
											memcpy(&andres[andres_size], &tmpval, GAP_SIZE_BYTES);
											andres_size += GAP_SIZE_BYTES;
										}
									}
									prev0_bit = i;
//									cout << "prev0_bit set3 " << prev0_bit << endl;

								} else if (maskarr[i/8] & (0x80 >> (i%8))) {
									if (begin) {
										begin = false;
										andres[ROW_SIZE_BYTES] = 0x01;
										andres_size = ROW_SIZE_BYTES + 1;
										p1bit_set = true;
									} else {
										if (!p1bit_set) {//means all prev bits are 0s only
											//pushing previous # of 0s
											memcpy(&andres[andres_size], &i, GAP_SIZE_BYTES);
											andres_size += GAP_SIZE_BYTES;
											p1bit_set = true;
										} else if (prev1_bit != (i - 1)) {
											//pushing previous # of 0s
											tmpval = i - prev1_bit -1;
											memcpy(&andres[andres_size], &tmpval, GAP_SIZE_BYTES);
											andres_size += GAP_SIZE_BYTES;
										}
									}
									prev1_bit = i;
//									cout << "prev1_bit set " << prev1_bit << endl;
								}
							}
						}

//						if (done) break;

					} else {
						if (begin) {
							begin = false;
							andres[ROW_SIZE_BYTES] = 0x00;
							andres_size = ROW_SIZE_BYTES +1;
							p0bit_set = true;
						} else {
							if (!p0bit_set) {
								//pushing previous # of 1s
								tmpval = prev1_bit+1;
								memcpy(&andres[andres_size], &tmpval, GAP_SIZE_BYTES);
								andres_size += GAP_SIZE_BYTES;
								p0bit_set = true;
							} else if (prev0_bit != (triplecnt - 1)) {
								//pushing previous # of 1s
								tmpval = triplecnt - prev0_bit -1;
								memcpy(&andres[andres_size], &tmpval, GAP_SIZE_BYTES);
								andres_size += GAP_SIZE_BYTES;
							}
						}
//						cout << "prev0_bit set4 " << prev0_bit << " triplecnt " << triplecnt << " tmpcnt " << tmpcnt << endl;
					}
				} else {
					printf("Unrecognized format %x\n", format);
					assert(0);
				}

				flag = !flag;
				cnt++;
				triplecnt += tmpcnt;
			}
			if (format == 0x00) {
				//COMMENT: no need to push last 0 bits.
				if (prev1_bit > prev0_bit) {
					//COMMENT: Push last 1 bits.
					if (!p0bit_set) {
						tmpval = (prev1_bit + 1);
						memcpy(&andres[andres_size], &tmpval, GAP_SIZE_BYTES);
					} else {
						tmpval = (prev1_bit - prev0_bit);
						memcpy(&andres[andres_size], &tmpval, GAP_SIZE_BYTES);
					}
					andres_size += GAP_SIZE_BYTES;
				} else if (prev1_bit == prev0_bit && p1bit_set && p0bit_set) {
					//if both bits have been set before and both are 0, then something is wrong here
//					cout << "prev0_bit " << prev0_bit << " prev1_bit " << prev1_bit << endl;
					assert(0);
				}

				if ((!andres[ROW_SIZE_BYTES]) && andres_size == 1+ROW_SIZE_BYTES) {
					//empty AND res
//					cout << "removed rowid1 " << (*it).rowid << endl;
					data -= ROW_SIZE_BYTES;
					free(data);
					it = bitmat->bm.erase(it);
					continue;
				}
			} else {
				if (andres_size == ROW_SIZE_BYTES+1) {
//					cout << "removed rowid2 " << (*it).rowid << endl;
					data -= ROW_SIZE_BYTES;
					free(data);
					it = bitmat->bm.erase(it);
					continue;
				}
			}

//			cumulative_dgap(data + ROW_SIZE_BYTES, rowsize, data + ROW_SIZE_BYTES);
//			andres_size = dgap_AND(data + ROW_SIZE_BYTES, rowsize, cumu_maskarr, maskarr_size, &andres[ROW_SIZE_BYTES]);
//			de_cumulative_dgap(&andres[ROW_SIZE_BYTES], andres_size, &andres[ROW_SIZE_BYTES]);
//			if (!andres[ROW_SIZE_BYTES] && andres_size == 1+GAP_SIZE_BYTES) {
//				//empty AND res
//				free(data);
//				it = bitmat->bm.erase(it);
//				continue;
//			}

//			bool format_changed = false;
			/*
			 * To be enabled if memory performance suffers
			if ((andres[ROW_SIZE_BYTES] & 0x02) == 0x00) {
				unsigned int total_gaps = count_dgaps_in_row(&andres[ROW_SIZE_BYTES], andres_size - ROW_SIZE_BYTES);
				unsigned int total_setbits = count_triples_in_row(&andres[ROW_SIZE_BYTES], andres_size - ROW_SIZE_BYTES);
				if (total_gaps > total_setbits) {
					unsigned char *tmpdata = (unsigned char *) malloc (1 + total_setbits*GAP_SIZE_BYTES + ROW_SIZE_BYTES);
					bool flag = andres[ROW_SIZE_BYTES] & 0x01;
					unsigned int cnt = 0, total_bits = 0, gapcnt = 0;
					tmpdata[ROW_SIZE_BYTES] = 0x02; //format
					unsigned int size = 1;
					while (cnt < total_gaps) {
						memcpy(&gapcnt, &andres[ROW_SIZE_BYTES + cnt*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);
						if (flag) {
							for (unsigned int i = total_bits+1; i <= total_bits+gapcnt; i++) {
								memcpy(&tmpdata[size + ROW_SIZE_BYTES], &i, GAP_SIZE_BYTES);
								size += GAP_SIZE_BYTES;
							}
						}
						flag = !flag;
						cnt++;
						total_bits += gapcnt;
					}
					assert(size == (1 + total_setbits * GAP_SIZE_BYTES));
					memcpy(tmpdata, &size, ROW_SIZE_BYTES);
					data -= ROW_SIZE_BYTES;
					free(data);
					(*it).data = tmpdata;
					format_changed = true;
				}
			}
	 */

//			if (!format_changed) {
				tmpval = (andres_size - ROW_SIZE_BYTES);
				memcpy(andres, &tmpval, ROW_SIZE_BYTES);
				data -= ROW_SIZE_BYTES;
				free(data);
				(*it).data = (unsigned char *) malloc (andres_size);
				memcpy((*it).data, andres, andres_size);
//			}
			it++;
		}
		if (orig_bm_triples - bitmat->num_triples > 0) {
			bitmat->last_unfold = COLUMN;
		}
		free(andres);

}

//////////////////////////////////////////////////////////
/*
 * Modified simple_unfold updates the count of triples in bitmat
 * dynamically as it unfolds on the "column" dimension.
 * For the "row" dimension, it does so only if it is asked
 * to do so through "numpass" variable.
 */
void simple_unfold(BitMat *bitmat, unsigned char *maskarr, unsigned int maskarr_size,
					int dimension, int numpass)
{
//	cout << "simple_unfold " << numpass << " dimension " << ret_dimension << endl;

	if (dimension == ROW) {
		if (count_bits_in_row(maskarr, maskarr_size) == bitmat->num_subs) {
			//don't need to unfold anything just return
			printf("All subjects present.. didn't unfold anything\n");
			return;
		}
		if (maskarr_size == 0) {
			for (std::list<struct row>::iterator it = bitmat->bm.begin(); it != bitmat->bm.end(); ) {
				free((*it).data);
				it = bitmat->bm.erase(it);
			}
			bitmat->num_triples = 0;
			bitmat->last_unfold = ROW;
			return;
		}

		unsigned int bm_orig_rows = bitmat->bm.size();

		for (std::list<struct row>::iterator it = bitmat->bm.begin(); it != bitmat->bm.end(); ) {
			unsigned int rowbit = (*it).rowid - 1;
			if (rowbit/8 >= maskarr_size) {
				free((*it).data);
				it = bitmat->bm.erase(it);
				continue;
			}
			if (!( maskarr[rowbit/8] & (0x80 >> (rowbit%8)))) {
				free((*it).data);
				it = bitmat->bm.erase(it);
				continue;
			}
			it++;
		}
		if (bm_orig_rows - bitmat->bm.size() > 0) {
			bitmat->last_unfold = ROW;
			if (numpass > 1) {
				bitmat->num_triples = count_triples_in_bitmat(bitmat);
			}
		}

////////////////////////////////////////////////////////////////////////////
		
	} else if (dimension == COLUMN) {
		//use concatenation function
//		cout << "*********** simple_unfold OBJ_DIMENSION **********" << endl;
		unsigned int andres_size = 0;
		unsigned int maskarr_bits = count_bits_in_row(maskarr, maskarr_size);

		if (maskarr_bits == bitmat->num_objs) {
			printf("All objects present.. didn't unfold anything\n");
			return;
		}
//		if (count_triples_in_row(maskarr, maskarr_size) == bitmat->num_objs) {
//			//don't need to do anything
//			printf("All objects present.. didn't unfold anything\n");
//			return;
//		}

//		cumulative_dgap(maskarr, maskarr_size, cumu_maskarr);

//		cout << "bitmat->numtriples " << bitmat->num_triples << endl;

		if (maskarr_size == 0) {
			printf("simple_unfold: maskarr_size is 0\n");
			for (std::list<struct row>::iterator it = bitmat->bm.begin(); it != bitmat->bm.end(); ) {
				unsigned char *data  = (*it).data;
				free(data);
				it = bitmat->bm.erase(it);
			}
			bitmat->last_unfold = COLUMN;
			bitmat->num_triples = 0;
			return;
		}

		//////////
		//DEBUG
		//TODO: remove later
		///////
//		cout << "---------- simple_unfold: maskarr_bits" << endl;
//		unsigned int maskarr_set_bits =	print_set_bits_in_row(maskarr, maskarr_size);
		/////// work begins
		long long sz = bitmat->bm.size(),block_sz;
		long long no_threads = 8;

		block_sz = (sz/no_threads) + (sz%no_threads!=0);
		if(block_sz%8!=0)
			block_sz+=8-block_sz%8;
		std::thread myThread[no_threads];
		auto it = bitmat->bm.begin();
		myThread[0] =thread(simple_unfold_func,bitmat,maskarr,maskarr_size,dimension,numpass,it,min(block_sz,sz - block_sz*(0)));



		for(long long i=1;i<no_threads;i++){
    		advance(it, block_sz);
			myThread[i] = thread(simple_unfold_func,bitmat,maskarr,maskarr_size,dimension,numpass,it,min(block_sz,sz - block_sz*(i)));		}

		for(long long i=0;i<no_threads;i++)
				myThread[i].join();

		//////work ends
/*

		unsigned char *andres = (unsigned char *) malloc (GAP_SIZE_BYTES * 2 * maskarr_bits + ROW_SIZE_BYTES + 1 + 1024);
		
		unsigned int orig_bm_triples = bitmat->num_triples;

		for (std::list<struct row>::iterator it = bitmat->bm.begin(); it != bitmat->bm.end(); ) {
			unsigned char *data  = (*it).data;
			unsigned int rowsize = 0;
			memcpy(&rowsize, data, ROW_SIZE_BYTES);
			data += ROW_SIZE_BYTES;
			unsigned int cnt = 0, total_cnt = (rowsize-1)/GAP_SIZE_BYTES, tmpcnt = 0, triplecnt = 0,
					prev1_bit = 0, prev0_bit = 0, tmpval = 0;
			bool flag = data[0] & 0x01, begin = true, p1bit_set = false, p0bit_set = false;//, done = false;
			unsigned char format = data[0] & 0x02;
			if (format == 0x02) {
				begin = false;
				andres[ROW_SIZE_BYTES] = 0x02;
				andres_size = ROW_SIZE_BYTES + 1;
			}

//			cout << "cnt " << cnt << " total_cnt " << total_cnt << " rowid " << (*it).rowid << endl;
//			printf("format %x\n", format);

			while (cnt < total_cnt) {
				//tmpcnt is going to be the position of 1s in case of alternate format,
				//positions begin with index 1, and NOT with 0
				memcpy(&tmpcnt, &data[cnt*GAP_SIZE_BYTES+1], GAP_SIZE_BYTES);
//				cout << "triplecnt " << triplecnt << " tmpcnt " << tmpcnt << endl;

				if (format == 0x02) {
					if ((tmpcnt-1)/8 >= maskarr_size) {
//						if (numpass == 1) {
//							break;
//						}
						bitmat->num_triples--;
					} else if (maskarr[(tmpcnt-1)/8] & (0x80 >> ((tmpcnt-1)%8))) {
						//The bit (position) under consideration is present in maskarr
						memcpy(&andres[andres_size], &tmpcnt, GAP_SIZE_BYTES);
						andres_size += GAP_SIZE_BYTES;
					} else {
						bitmat->num_triples--;
					}
				} else if (format == 0x00) {
					//triplecnt is number of bits already read from gaps.
					//if maskarr size is already overshot by the number of bits read
					//then we don't need to do anything.
//					if (numpass == 1 && triplecnt/8 >= maskarr_size) {
//						break;
//					}
					if (flag) {
						if (triplecnt/8 >= maskarr_size) {
							bitmat->num_triples -= tmpcnt;
							//p0bit_set = true;
							prev0_bit = triplecnt + tmpcnt - 1;
//							cout << "prev0_bit set1 " << prev0_bit << endl;
						} else {
							for (unsigned int i = triplecnt; i < triplecnt + tmpcnt; i++) {
								if (i/8 >= maskarr_size) {
									//An existing bit is unset to due to maskarr
									//so decreasing the triple count
									bitmat->num_triples--;
									assert(p0bit_set || p1bit_set);
									if (!p0bit_set && p1bit_set) {
										tmpval = prev1_bit + 1;
										memcpy(&andres[andres_size], &tmpval, GAP_SIZE_BYTES);
										andres_size += GAP_SIZE_BYTES;
										p0bit_set = true;
									} else if (prev1_bit > prev0_bit) {
										tmpval = prev1_bit - prev0_bit;
										memcpy(&andres[andres_size], &tmpval, GAP_SIZE_BYTES);
										andres_size += GAP_SIZE_BYTES;
									}
									prev0_bit = i;
//									cout << "prev0_bit set2 " << prev0_bit << endl;
//									if (numpass == 1) {
//										done = true;
//										break;
//									}
								} else if ((maskarr[i/8] & (0x80 >> (i%8))) == 0x00) {
									//An existing bit is unset to due to maskarr
									bitmat->num_triples--;
									if (begin) {
										begin = false;
										andres[ROW_SIZE_BYTES] = 0x00;
										andres_size = ROW_SIZE_BYTES +1;
										p0bit_set = true;
									} else {
										if (!p0bit_set) {//this means all prev bits are 1s
											//pushing previous # of 1s
											memcpy(&andres[andres_size], &i, GAP_SIZE_BYTES);
											andres_size += GAP_SIZE_BYTES;
											p0bit_set = true;
										} else if (prev0_bit != (i - 1)) {//this means there are one or more 1s between this 0 bit and prev one
											//pushing previous # of 1s
											tmpval = i - prev0_bit -1;
											memcpy(&andres[andres_size], &tmpval, GAP_SIZE_BYTES);
											andres_size += GAP_SIZE_BYTES;
										}
									}
									prev0_bit = i;
//									cout << "prev0_bit set3 " << prev0_bit << endl;

								} else if (maskarr[i/8] & (0x80 >> (i%8))) {
									if (begin) {
										begin = false;
										andres[ROW_SIZE_BYTES] = 0x01;
										andres_size = ROW_SIZE_BYTES + 1;
										p1bit_set = true;
									} else {
										if (!p1bit_set) {//means all prev bits are 0s only
											//pushing previous # of 0s
											memcpy(&andres[andres_size], &i, GAP_SIZE_BYTES);
											andres_size += GAP_SIZE_BYTES;
											p1bit_set = true;
										} else if (prev1_bit != (i - 1)) {
											//pushing previous # of 0s
											tmpval = i - prev1_bit -1;
											memcpy(&andres[andres_size], &tmpval, GAP_SIZE_BYTES);
											andres_size += GAP_SIZE_BYTES;
										}
									}
									prev1_bit = i;
//									cout << "prev1_bit set " << prev1_bit << endl;
								}
							}
						}

//						if (done) break;

					} else {
						if (begin) {
							begin = false;
							andres[ROW_SIZE_BYTES] = 0x00;
							andres_size = ROW_SIZE_BYTES +1;
							p0bit_set = true;
						} else {
							if (!p0bit_set) {
								//pushing previous # of 1s
								tmpval = prev1_bit+1;
								memcpy(&andres[andres_size], &tmpval, GAP_SIZE_BYTES);
								andres_size += GAP_SIZE_BYTES;
								p0bit_set = true;
							} else if (prev0_bit != (triplecnt - 1)) {
								//pushing previous # of 1s
								tmpval = triplecnt - prev0_bit -1;
								memcpy(&andres[andres_size], &tmpval, GAP_SIZE_BYTES);
								andres_size += GAP_SIZE_BYTES;
							}
						}
						prev0_bit = triplecnt + tmpcnt - 1;
//						cout << "prev0_bit set4 " << prev0_bit << " triplecnt " << triplecnt << " tmpcnt " << tmpcnt << endl;
					}
				} else {
					printf("Unrecognized format %x\n", format);
					assert(0);
				}

				flag = !flag;
				cnt++;
				triplecnt += tmpcnt;
			}
			if (format == 0x00) {
				//COMMENT: no need to push last 0 bits.
				if (prev1_bit > prev0_bit) {
					//COMMENT: Push last 1 bits.
					if (!p0bit_set) {
						tmpval = (prev1_bit + 1);
						memcpy(&andres[andres_size], &tmpval, GAP_SIZE_BYTES);
					} else {
						tmpval = (prev1_bit - prev0_bit);
						memcpy(&andres[andres_size], &tmpval, GAP_SIZE_BYTES);
					}
					andres_size += GAP_SIZE_BYTES;
				} else if (prev1_bit == prev0_bit && p1bit_set && p0bit_set) {
					//if both bits have been set before and both are 0, then something is wrong here
//					cout << "prev0_bit " << prev0_bit << " prev1_bit " << prev1_bit << endl;
					assert(0);
				}

				if ((!andres[ROW_SIZE_BYTES]) && andres_size == 1+ROW_SIZE_BYTES) {
					//empty AND res
//					cout << "removed rowid1 " << (*it).rowid << endl;
					data -= ROW_SIZE_BYTES;
					free(data);
					it = bitmat->bm.erase(it);
					continue;
				}
			} else {
				if (andres_size == ROW_SIZE_BYTES+1) {
//					cout << "removed rowid2 " << (*it).rowid << endl;
					data -= ROW_SIZE_BYTES;
					free(data);
					it = bitmat->bm.erase(it);
					continue;
				}
			}

//			cumulative_dgap(data + ROW_SIZE_BYTES, rowsize, data + ROW_SIZE_BYTES);
//			andres_size = dgap_AND(data + ROW_SIZE_BYTES, rowsize, cumu_maskarr, maskarr_size, &andres[ROW_SIZE_BYTES]);
//			de_cumulative_dgap(&andres[ROW_SIZE_BYTES], andres_size, &andres[ROW_SIZE_BYTES]);
//			if (!andres[ROW_SIZE_BYTES] && andres_size == 1+GAP_SIZE_BYTES) {
//				//empty AND res
//				free(data);
//				it = bitmat->bm.erase(it);
//				continue;
//			}

//			bool format_changed = false;
			/*
			 * To be enabled if memory performance suffers
			if ((andres[ROW_SIZE_BYTES] & 0x02) == 0x00) {
				unsigned int total_gaps = count_dgaps_in_row(&andres[ROW_SIZE_BYTES], andres_size - ROW_SIZE_BYTES);
				unsigned int total_setbits = count_triples_in_row(&andres[ROW_SIZE_BYTES], andres_size - ROW_SIZE_BYTES);
				if (total_gaps > total_setbits) {
					unsigned char *tmpdata = (unsigned char *) malloc (1 + total_setbits*GAP_SIZE_BYTES + ROW_SIZE_BYTES);
					bool flag = andres[ROW_SIZE_BYTES] & 0x01;
					unsigned int cnt = 0, total_bits = 0, gapcnt = 0;
					tmpdata[ROW_SIZE_BYTES] = 0x02; //format
					unsigned int size = 1;
					while (cnt < total_gaps) {
						memcpy(&gapcnt, &andres[ROW_SIZE_BYTES + cnt*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);
						if (flag) {
							for (unsigned int i = total_bits+1; i <= total_bits+gapcnt; i++) {
								memcpy(&tmpdata[size + ROW_SIZE_BYTES], &i, GAP_SIZE_BYTES);
								size += GAP_SIZE_BYTES;
							}
						}
						flag = !flag;
						cnt++;
						total_bits += gapcnt;
					}
					assert(size == (1 + total_setbits * GAP_SIZE_BYTES));
					memcpy(tmpdata, &size, ROW_SIZE_BYTES);
					data -= ROW_SIZE_BYTES;
					free(data);
					(*it).data = tmpdata;
					format_changed = true;
				}
			}


//			if (!format_changed) {
				tmpval = (andres_size - ROW_SIZE_BYTES);
				memcpy(andres, &tmpval, ROW_SIZE_BYTES);
				data -= ROW_SIZE_BYTES;
				free(data);
				(*it).data = (unsigned char *) malloc (andres_size);
				memcpy((*it).data, andres, andres_size);
//			}
			it++;
		}
		if (orig_bm_triples - bitmat->num_triples > 0) {
			bitmat->last_unfold = COLUMN;
		}
		free(andres);
*/
	} else {
		assert(0);
	}
}

////////////////////////////////////////////////////////////
unsigned int count_size_of_bitmat(unsigned char **bitmat)
{
#if USE_MORE_BYTES
	unsigned long tmpcnt = 0;
#else
	unsigned int tmpcnt = 0;
#endif	
	unsigned long size = 0, i;
	unsigned int ptrsize = sizeof(unsigned char *);

	if (bitmat == NULL)
		return 0;

	size += (gnum_subs * ptrsize);
	for (i = 0; i < gnum_subs; i++) {
		if (bitmat[i] == NULL)
			continue;
		memcpy(&tmpcnt, bitmat[i], ROW_SIZE_BYTES);
		size += tmpcnt;
	}

	return size;
}
////////////////////////////////////////////////////////////
//void explore_other_nodes(struct node *n, int curr_nodenum, struct node *parent, FILE *outfile)
//{
//	//COMMENT: instead of maintaining unvisited list
//	//every time when you don't find an unmapped neighbor
//	//of the node or its parent's neighbor, just go over the mapped
//	//nodes in the q_to_gr_node_map and pick an unmapped neighbor
//	//of the first mapped node
//
//	LIST *ptr = n->nextTP;
//
//	//First insert all the unvisited neighbors in the "unvisited" set
//	//every time you call match_query_graph on an unvisited node
//	//remove it from the set
//	for (; ptr; ) {
//
//		assert(ptr->gnode->isNeeded);
////		if (!ptr->gnode->isNeeded) {
////			ptr = ptr->next;
////			continue;
////		}
//
//		if (q_to_gr_node_map.find((TP *)ptr->gnode->data) == q_to_gr_node_map.end()) {
//
//			unvisited.insert(unvisited.end(), ptr->gnode);
//			
//		}
//		ptr = ptr->next;
//	}
//
//	//3.2: Medha: now call match_query_graph on the first unvisited neighbor
//	ptr = n->nextTP;
//	bool explored = false;
////	bool newly_mapped = true;
//
////	for (int k = 0; k < n->numTPs && newly_mapped; k++) 
//	for (; ptr; ) {
//
//		assert(ptr->gnode->isNeeded);
////		if (!ptr->gnode->isNeeded) {
////			ptr = ptr->next;
////			continue;
////		}
//
//		if (q_to_gr_node_map.find((TP *)ptr->gnode->data) == q_to_gr_node_map.end()) {
//			//this neighbor node is unmapped yet.. so explore that and 
//			
//			//make sure that the edge that you
//			//are exploring is a "tree" edge and not backedge
//			vector<struct node*>::iterator it = find(tree_edges_map[n].begin(), tree_edges_map[n].end(), ptr->gnode);
//			if (it != tree_edges_map[n].end()) {
//				explored = true;
//				//remove this neighbor being explored from the unvisited set
//				//before calling match_query_graph on it
//				unvisited.erase(ptr->gnode);
//				TP *tp = (TP *)ptr->gnode->data;
////				cout << "Exploring neighbor node " << tp->sub << " " << tp->pred << " " << tp->obj  << endl;
//				match_query_graph(ptr->gnode, curr_nodenum+1, n, outfile);
////				newly_mapped = false;
//				return;
//			}
//		}
//		ptr = ptr->next;
//	}
//
//	//3.3: Medha: The very fact that you came here means you don't have a pathlike
//	//spanning tree edges so just start exploring the parent of the current node
//
//	if (!explored && parent != NULL) {
////		for (vector<struct node*>::iterator it = tree_edges_map[parent].begin();
////				it != tree_edges_map[parent].end() && newly_mapped; it++) 
//		for (vector<struct node*>::iterator it = tree_edges_map[parent].begin();
//				it != tree_edges_map[parent].end(); it++) {
//			if (q_to_gr_node_map.find((TP *)(*it)->data) == q_to_gr_node_map.end()) {
//				//cout << "4. calling match_query_graph on " << ((TP *)(*it)->data)->sub 
//				//	<< " " << ((TP *)(*it)->data)->pred << " " << ((TP *)(*it)->data)->obj << endl;
//				explored = true;
//				unvisited.erase((*it));
//				TP *tp = (TP *)(*it)->data;
////				cout << "Exploring2 neighbor node " << tp->sub << " " << tp->pred << " " << tp->obj  << endl;
//				match_query_graph((*it), curr_nodenum+1, parent, outfile);
////				newly_mapped = false;
//				return;
//			}
//
//		}
//	}
//
//	//3.4: Medha: if you come here and you still have "explored" == false, that means you have
//	//exhausted all the unvisited neighbors of your parent too but still you haven't
//	//finished visited all query nodes.. so pick a node from the "unvisited" set
//	//This can happen in case of a graph as shown at the top while defining "unvisited" set
//	if (!explored) {
//		//TODO: recheck
//		if (unvisited.size() == 0) {
//			for (int i = 0; i < graph_tp_nodes; i++) {
//				if (!graph[MAX_JVARS_IN_QUERY + i].isNeeded)
//					continue;
//				TP *tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;
//				//Find an already mapped node and then find its unmapped
//				//neighbor
//				if (q_to_gr_node_map.find(tp) != q_to_gr_node_map.end()) {
//					ptr = graph[MAX_JVARS_IN_QUERY + i].nextTP;
//					for (; ptr; ) {
//
//						if (!ptr->gnode->isNeeded) {
//							ptr = ptr->next;
//							continue;
//						}
//
//						if (q_to_gr_node_map.find((TP *)ptr->gnode->data) == q_to_gr_node_map.end()) {
//							unvisited.insert(unvisited.end(), ptr->gnode);
//							break;
//						}
//						ptr = ptr->next;
//					}
//				}
//			}
//		}
//
//		set<struct node *>::iterator it = unvisited.begin();
//		explored = true;
//		unvisited.erase((*it));
//		//parent is set to NULL as we are randomly picking
//		//it up from unvisited set, hence we don't know who
//		//is its parent node.						
//		TP *tp = (TP *)(*it)->data;
////		cout << "Exploring3 neighbor node " << tp->sub << " " << tp->pred << " " << tp->obj  << endl;
//		match_query_graph((*it), curr_nodenum+1, NULL, outfile);
////		newly_mapped = false;
//	}
//
//}
//////////////////////////////////////////////////////////
//void explore_other_nodes(struct node *n, int curr_nodenum, struct node *parent, FILE *outfile)
//{
//
//	//TODO: fix here.. you need 2 vectors -- vec1, vec2, one is drained and
//	//another is populated with the draining values from the other.
//	map<struct node *, vector<struct node *> >::iterator nitr = tree_edges_map.find(n);
//	vector<struct node*> nVec ;
//
//	if (nitr != tree_edges_map.end()) {
//		nVec = tree_edges_map[n];
//	}
//
//	for (vector<struct node*>::iterator it = nVec.begin(); it != nVec.end(); it++) {
//		assert((*it)->isNeeded);
//
//		if (q_to_gr_node_map.find((TP *)(*it)->data) == q_to_gr_node_map.end()) {
//			//this neighbor node is unmapped yet.. so explore that and 
//			unvisited.push_back(*it);
//		}
//	}
//	assert(unvisited.size() != 0);
//	struct node *qfront = unvisited.front();
//	unvisited.pop_front();
//
//	match_query_graph(qfront, curr_nodenum+1, NULL, outfile);
//
//}

//////////////////////////////////////////////////////

//TODO: fix for OPTIONAL queries
//bool check_back_edges(struct node *curr_node, TP curr_matched_triple)
//{
//	struct node *n = curr_node;
//	bool matched = true;
//
//	LIST *ptr = n->nextTP;
//	struct triple t = curr_matched_triple;
//	TP *q_node = (TP *)n->data;
//
//	for(; ptr; ) {
//		// go over all the neighbors.
//
//		assert(ptr->gnode->isNeeded);
////		if (!ptr->gnode->isNeeded) {
////			ptr = ptr->next;
////			continue;
////		}
//
//		TP *q_nt = (TP *)ptr->gnode->data;
//
////		cout << "check_back_edges: Exploring [" << q_nt->sub << " " << q_nt->pred << " " << q_nt->obj << endl; 
//
//		if(q_to_gr_node_map.find(q_nt) != q_to_gr_node_map.end()) {
//			// Found occurrence of mapping for a neighbor in the map.
//			// nt == neighbor triple q_nt == query node associated with that triple
//
//			struct triple nt = q_to_gr_node_map[q_nt];
//
//			// Compare the corresponding entries based on the edge type.
//			int edge_type = ptr->edgetype;
//			switch(edge_type) {
//				case SUB_EDGE:
//					if(t.sub != nt.sub)
//						matched=false;
//					break;
//
//				case PRED_EDGE:
//					if(t.pred != nt.pred)
//						matched=false;
//					break;
//
//				case OBJ_EDGE:
//					if(t.obj != nt.obj)
//						matched=false;
//					break;
//
//				case SO_EDGE:
//					if((q_node->sub < 0) && (q_node->sub == q_nt->obj)) {
//						if(t.sub != nt.obj)
//							matched=false;
//					} else if((q_node->obj < 0) && (q_node->obj == q_nt->sub)) {
//						if(t.obj != nt.sub)
//							matched=false;
//					}
//					break;
//			}
//
//			if(!matched)
//				break;
//				
//		} 
//		ptr=ptr->next;
//
//	}
//
//	return matched;
//
//}
void set_neighbors_null2(struct node *n, set<string> &null_tmp)
{
	LIST *ptr = n->nextTP;
	struct triple t = {0, 0, 0};
	TP *q_node = (TP *)n->data;

	for(; ptr; ) {
		assert(ptr->gnode->isNeeded);

		TP *q_nt = (TP *)ptr->gnode->data;
		map<struct node*, struct triple>::iterator it = q_to_gr_node_map.find(ptr->gnode);

		if(it != q_to_gr_node_map.end()) {
			struct triple tn = (*it).second;
			if (tn.sub != 0 && tn.pred != 0 && tn.obj != 0) {
				if ((q_nt->isSlave(q_node->strlevel))
					||
					(!q_node->isSlave(q_nt->strlevel) && !q_nt->isSlave(q_node->strlevel))
					) {
					q_to_gr_node_map[ptr->gnode] = t;
					null_tmp.insert(q_nt->strlevel);
					set_neighbors_null2(ptr->gnode, null_tmp);
				}
			}
			//TODO: remove later
			else if (tn.sub == 0 && tn.pred == 0 && tn.obj == 0) {} else {assert(0);}
		}
		ptr=ptr->next;
	}
}


//void set_neighbors_null(struct node *n, set<string> &null_tmp)
//{
//	//TODO: you have to do this recursively until entire map is consistent
//	LIST *ptr = n->nextTP;
//	struct triple t = {0, 0, 0};
//	TP *q_node = (TP *)n->data;
//
//	for(; ptr; ) {
//		// go over all the neighbors.
//
//		assert(ptr->gnode->isNeeded);
//
//		TP *q_nt = (TP *)ptr->gnode->data;
//		map<struct node*, struct triple>::iterator it = q_to_gr_node_map.find(ptr->gnode);
//
//		if(it != q_to_gr_node_map.end()) {
//			struct triple tn = (*it).second;
//			if (tn.sub != 0 && tn.pred != 0 && tn.obj != 0) {
//				if (q_node->strlevel.compare(q_nt->strlevel) == 0 ||
//					(!q_node->isSlave(q_nt->strlevel) && !q_nt->isSlave(q_node->strlevel))) {
//					q_to_gr_node_map[ptr->gnode] = t;
//					null_tmp.insert(q_nt->strlevel);
//					set_neighbors_null2(ptr->gnode, null_tmp);
//				}
//
//			}
//		}
//		ptr=ptr->next;
//	}
//}

/////////////////////////////////////////////////////

void set_neighbors_null(struct node *gnode, map<struct node*, struct triple>&resmap)
{
	deque<struct node *> bfsq;
	bfsq.push_back(gnode);
	struct node *node = NULL;

	while (bfsq.size() > 0) {
		node = bfsq.front(); bfsq.pop_front();
		TP *tp = (TP *)node->data;
		LIST *next = node->nextTP;

		for (; next; next = next->next) {
			map<struct node*, struct triple>::iterator it = resmap.find(next->gnode);
			if (it == resmap.end())
				continue;

			TP *neigh = (TP *)next->gnode->data;

			if (neigh->isSlave(tp->strlevel) ||
					(!neigh->isSlave(tp->strlevel) && !tp->isSlave(neigh->strlevel))) {
				struct triple t = it->second;
				if (t.sub != 0 || t.pred != 0 || t.obj != 0) {
					t.sub = 0; t.pred = 0; t.obj = 0;
					resmap[next->gnode] = t;
					bfsq.push_back(next->gnode);
				}
			}
		}
	}
}

void print_res(FILE *outfile, char *num_null_pads, struct node **bfsarr, bool bestm)
{
//	for(map<struct node*, struct triple>::iterator it = copy_of_resmap.begin(); it != copy_of_resmap.end(); it++) {
//		struct triple t = it->second;
//		if (t.sub == 0 || t.pred == 0 || t.obj == 0) {
//			set_neighbors_null(it->first, copy_of_resmap);
//		}
//	}
	if (!bestm) {
		for (int i = (-1)*q_to_gr_node_map.size(); i < 0; i++) {
	//		cout << i << " tp " << ((TP*)bfsarr[i]->data)->toString() << endl;
			struct triple t = q_to_gr_node_map[bfsarr[i]];
			if (((TP*)bfsarr[i]->data)->sub < 0) {
				fprintf(outfile, "%u ", t.sub);
			}
			if (((TP*)bfsarr[i]->data)->pred < 0) {
				fprintf(outfile, "%u ", t.pred);
			}
			if (((TP*)bfsarr[i]->data)->obj < 0) {
				fprintf(outfile, "%u ", t.obj);
			}
		}
	} else {
		for(map<struct node*, struct triple>::iterator it = q_to_gr_node_map.begin();
				it != q_to_gr_node_map.end(); it++) {
			copy_of_resmap[it->first] = it->second;
		}

		for (int i = (-1)*copy_of_resmap.size(); i < 0; i++) {
			struct triple t = copy_of_resmap[bfsarr[i]];
			if (t.sub == 0 || t.pred == 0 || t.obj == 0) {
				set_neighbors_null(bfsarr[i], copy_of_resmap);
			}
		}

		for (int i = (-1)*copy_of_resmap.size(); i < 0; i++) {
	//		cout << i << " tp " << ((TP*)bfsarr[i]->data)->toString() << endl;
			struct triple t = copy_of_resmap[bfsarr[i]];
			if (((TP*)bfsarr[i]->data)->sub < 0) {
				fprintf(outfile, "%u ", t.sub);
			}
			if (((TP*)bfsarr[i]->data)->pred < 0) {
				fprintf(outfile, "%u ", t.pred);
			}
			if (((TP*)bfsarr[i]->data)->obj < 0) {
				fprintf(outfile, "%u ", t.obj);
			}
		}
	}

//	for(map<struct node*, struct triple>::iterator it = copy_of_resmap.begin(); it != copy_of_resmap.end(); it++) {
//		if (((TP*)(it->first)->data)->sub < 0) {
//			fprintf(outfile, "%u ", (it->second).sub);
//		}
//		if (((TP*)it->first->data)->pred < 0) {
//			fprintf(outfile, "%u ", (it->second).pred);
//		}
//		if (((TP*)it->first->data)->obj < 0) {
//			fprintf(outfile, "%u ", (it->second).obj);
//		}
//	}
	fprintf(outfile, "%s", num_null_pads);
//	for (int i = 0; i < num_null_pads; i++) {
//		fprintf(outfile, "0 ");
//	}
	fprintf(outfile, "\n");
}

/////////////////////////////////////////////////////
void match_triples_in_row(BitMat *bitmat, struct row *r, struct node *n, FILE *outfile,
							struct node **bfsarr, int sidx, int eidx, char *num_null_pads, bool bestm)
{
#if USE_MORE_BYTES
	unsigned long opos = 0;
	unsigned long tmpcnt = 0, bitcnt = 0;
#else
	unsigned int column_bit = 0;
	unsigned int tmpcnt = 0, bitcnt = 0;
#endif	

	unsigned char *resptr = r->data + ROW_SIZE_BYTES;
	bool flag = resptr[0] & 0x01;
	unsigned char format = resptr[0] & 0x02;
	unsigned int ressize = 0;
	memcpy(&ressize, r->data, ROW_SIZE_BYTES);
	unsigned int total_cnt = (ressize - 1)/GAP_SIZE_BYTES;
	unsigned int cnt = 0;
	bitcnt = 0;

	while (cnt < total_cnt) {
		memcpy(&tmpcnt, &resptr[cnt*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);
		struct triple t;
		TP *q_node = (TP *)n->data;

		if (format == 0x02) {
			column_bit = tmpcnt;
			switch (bitmat->dimension) {
				case (SPO_BITMAT):
					t.sub = r->rowid; t.pred = q_node->pred; t.obj = column_bit;
					break;
				case (OPS_BITMAT):
					t.sub = column_bit; t.pred = q_node->pred; t.obj = r->rowid;
					break;
				case (PSO_BITMAT):
					t.sub = q_node->sub; t.pred = r->rowid; t.obj = column_bit;
					break;
				case (POS_BITMAT):
					t.sub = column_bit; t.pred = r->rowid; t.obj = q_node->obj;
					break;

			}
			q_to_gr_node_map[n] = t;
			match_query_graph_new(outfile, &bfsarr[1], sidx+1, eidx, num_null_pads, bestm);
			q_to_gr_node_map.erase(n);

		} else {
			if (flag) {
				column_bit = bitcnt + 1;
				while (column_bit <= bitcnt+tmpcnt) {
					switch (bitmat->dimension) {
						case (SPO_BITMAT):
							t.sub = r->rowid; t.pred = q_node->pred; t.obj = column_bit;
							break;
						case (OPS_BITMAT):
							t.sub = column_bit; t.pred = q_node->pred; t.obj = r->rowid;
							break;
						case (PSO_BITMAT):
							t.sub = q_node->sub; t.pred = r->rowid; t.obj = column_bit;
							break;
						case (POS_BITMAT):
							t.sub = column_bit; t.pred = r->rowid; t.obj = q_node->obj;
							break;

					}
					q_to_gr_node_map[n] = t;
					match_query_graph_new(outfile, &bfsarr[1], sidx+1, eidx, num_null_pads, bestm);
					q_to_gr_node_map.erase(n);

					column_bit++;
				}
			}
		}
		bitcnt += tmpcnt;
		cnt++;
		flag = !flag;
	}

}
/////////////////////////////////////////////////////

bool check_bit(struct row *r, unsigned int bit)
{
	unsigned char *resptr = r->data + ROW_SIZE_BYTES;
	bool flag = resptr[0] & 0x01;
	unsigned char format = resptr[0] & 0x02;
	unsigned int ressize = 0;
	memcpy(&ressize, r->data, ROW_SIZE_BYTES);
	unsigned int total_cnt = (ressize - 1)/GAP_SIZE_BYTES;
	unsigned int cnt = 0;
	unsigned int bitcnt = 0, tmpcnt = 0;

	while (cnt < total_cnt) {
		memcpy(&tmpcnt, &resptr[cnt*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);

		if (format == 0x02) {
			if (bit == tmpcnt)
				return true;
		} else {
			if (flag && (bitcnt+1 <= bit && bit <= bitcnt+tmpcnt)) {
				return true;
			} else if (!flag && (bitcnt+1 <= bit && bit <= bitcnt+tmpcnt)) {
				return false;
			}
		}

		bitcnt += tmpcnt;
		cnt++;
		flag = !flag;
	}
	return false;
}

/*
 * In this method, the triple positions which are set by borrowing
 * neighbor's already set values are checked if they actually exist
 * in the respective BitMat of the TP under consideration
 */
void check_set_pos_and_proceed(struct node **bfsarr, struct triple t, FILE *outfile, int sidx,
		int eidx, char *num_null_pads, bool bestm)
{
	TP *tp = (TP *)bfsarr[0]->data;
	BitMat *bitmat = NULL;
	unsigned int rowid = 0, columnid = 0;

	if (tp->sub < 0 && tp->sub > MAX_JVARS && t.sub > 0) {
		if (tp->bitmat.dimension == SPO_BITMAT || tp->bitmat2.dimension == SPO_BITMAT) {
			rowid = t.sub;
			if (tp->bitmat.dimension == SPO_BITMAT) {
				bitmat = &(tp->bitmat);
			} else if (tp->bitmat2.dimension == SPO_BITMAT) {
				bitmat = &(tp->bitmat2);
			}
			if (bitmat != NULL && tp->obj < 0 && tp->obj > MAX_JVARS && t.obj > 0) {
				columnid = t.obj;
			}
		} else if (tp->bitmat.dimension == POS_BITMAT && tp->pred > 0) {
			assert(t.sub != 0 && t.pred != 0 && t.obj != 0);
			q_to_gr_node_map[bfsarr[0]] = t;
			match_query_graph_new(outfile, &bfsarr[1], sidx+1, eidx, num_null_pads, bestm);
			q_to_gr_node_map.erase(bfsarr[0]);
			return;
		} else if (tp->bitmat.dimension == POS_BITMAT) {
			bitmat = &(tp->bitmat);
			if (t.pred > 0) {
				rowid = t.pred;
				columnid = t.sub;
			} else {
				bool atleastone = false;
				for (vector<struct row>::iterator rowitr = bitmat->vbm.begin(); rowitr != bitmat->vbm.end(); rowitr++) {
					struct row *r = &(*rowitr);
					if (!check_bit(r, t.sub)) {
						continue;
					}
					atleastone = true;
					t.pred = r->rowid;
					assert(t.sub != 0 && t.pred != 0 && t.obj != 0);
					q_to_gr_node_map[bfsarr[0]] = t;
					match_query_graph_new(outfile, &bfsarr[1], sidx+1, eidx, num_null_pads, bestm);
					q_to_gr_node_map.erase(bfsarr[0]);
				}
				if (!atleastone) {
					if (tp->rollback)
						assert(0);
					t.sub = 0; t.pred = 0; t.obj = 0;
					q_to_gr_node_map[bfsarr[0]] = t;
					match_query_graph_new(outfile, &bfsarr[1], sidx+1, eidx, num_null_pads, bestm);
					q_to_gr_node_map.erase(bfsarr[0]);
				}
				return;
			}
		} else if (tp->bitmat.dimension == OPS_BITMAT) {
			bitmat = &(tp->bitmat);
			if (t.obj > 0) {
				rowid = t.obj;
				columnid = t.sub;
			} else {
				cout << tp->toString() << endl;
				assert(0);
//				bool atleastone = false;
//				for (vector<struct row>::iterator rowitr = bitmat->vbm.begin(); rowitr != bitmat->vbm.end(); rowitr++) {
//					struct row *r = &(*rowitr);
//					if (!check_bit(r, t.sub)) {
//						continue;
//					}
//					atleastone = true;
//					t.obj = r->rowid;
//					assert(t.sub != 0 && t.pred != 0 && t.obj != 0);
//					q_to_gr_node_map[bfsarr[0]] = t;
//					match_query_graph_new(outfile, &bfsarr[1], sidx+1, eidx);
//					q_to_gr_node_map.erase(bfsarr[0]);
//				}
//				if (!atleastone) {
//					if (tp->rollback)
//						assert(0);
//					t.sub = 0; t.pred = 0; t.obj = 0;
//					q_to_gr_node_map[bfsarr[0]] = t;
//					match_query_graph_new(outfile, &bfsarr[1], sidx+1, eidx);
//					q_to_gr_node_map.erase(bfsarr[0]);
//				}
//				return;
			}
		} else {
			assert(0);
		}
	}

	if (bitmat == NULL && tp->obj < 0 && tp->obj > MAX_JVARS && t.obj > 0) {
		if (tp->bitmat.dimension == OPS_BITMAT || tp->bitmat2.dimension == OPS_BITMAT) {
			rowid = t.obj;
			if (tp->bitmat.dimension == OPS_BITMAT) {
				bitmat = &(tp->bitmat);
			} else if (tp->bitmat2.dimension == OPS_BITMAT) {
				bitmat = &(tp->bitmat2);
			}
			if (bitmat != NULL && tp->sub < 0 && tp->sub > MAX_JVARS && t.sub > 0) {
				columnid = t.sub;
			}
		} else if (tp->bitmat.dimension == PSO_BITMAT && tp->pred > 0) {
			assert(t.sub != 0 && t.pred != 0 && t.obj != 0);
			q_to_gr_node_map[bfsarr[0]] = t;
			match_query_graph_new(outfile, &bfsarr[1], sidx+1, eidx, num_null_pads, bestm);
			q_to_gr_node_map.erase(bfsarr[0]);
			return;

		} else if (tp->bitmat.dimension == PSO_BITMAT) {
			bitmat = &(tp->bitmat);
			if (t.pred > 0) {
				rowid = t.pred;
				columnid = t.obj;
			} else {
				bool atleastone = false;
				for (vector<struct row>::iterator rowitr = bitmat->vbm.begin(); rowitr != bitmat->vbm.end(); rowitr++) {
					struct row *r = &(*rowitr);
					if (!check_bit(r, t.obj)) {
						continue;
					}
					atleastone = true;
					t.pred = r->rowid;
					assert(t.sub != 0 && t.pred != 0 && t.obj != 0);
					q_to_gr_node_map[bfsarr[0]] = t;
					match_query_graph_new(outfile, &bfsarr[1], sidx+1, eidx, num_null_pads, bestm);
					q_to_gr_node_map.erase(bfsarr[0]);
				}
				if (!atleastone) {
					if (tp->rollback)
						assert(0);
					t.sub = 0; t.pred = 0; t.obj = 0;
					q_to_gr_node_map[bfsarr[0]] = t;
					match_query_graph_new(outfile, &bfsarr[1], sidx+1, eidx, num_null_pads, bestm);
					q_to_gr_node_map.erase(bfsarr[0]);
				}
				return;
			}

		} else if (tp->bitmat.dimension == SPO_BITMAT) {
			bitmat = &(tp->bitmat);
			if (t.sub > 0) {
				rowid = t.sub;
				columnid = t.obj;
			} else {
				cout << tp->toString() << endl;
				assert(0);
//				bool atleastone = false;
//				for (vector<struct row>::iterator rowitr = bitmat->vbm.begin(); rowitr != bitmat->vbm.end(); rowitr++) {
//					struct row *r = &(*rowitr);
//					if (!check_bit(r, t.obj)) {
//						continue;
//					}
//					atleastone = true;
//					t.sub = r->rowid;
//					assert(t.sub != 0 && t.pred != 0 && t.obj != 0);
//					q_to_gr_node_map[bfsarr[0]] = t;
//					match_query_graph_new(outfile, &bfsarr[1], sidx+1, eidx);
//					q_to_gr_node_map.erase(bfsarr[0]);
//				}
//				if (!atleastone) {
//					if (tp->rollback)
//						assert(0);
//					t.sub = 0; t.pred = 0; t.obj = 0;
//					q_to_gr_node_map[bfsarr[0]] = t;
//					match_query_graph_new(outfile, &bfsarr[1], sidx+1, eidx);
//					q_to_gr_node_map.erase(bfsarr[0]);
//				}
//				return;
			}
		} else {
			assert(0);
		}
	}

	if (bitmat == NULL && tp->pred < 0 && tp->pred > MAX_JVARS && t.pred > 0) {
		//pred position is a jvar and set through neighbor's set val
		rowid = t.pred;
		if (tp->bitmat.dimension == PSO_BITMAT || tp->bitmat.dimension == POS_BITMAT) {
			bitmat = &(tp->bitmat);
			if (tp->sub < 0 && tp->sub > MAX_JVARS && t.sub > 0) {
				assert(bitmat->dimension == POS_BITMAT);
				columnid = t.sub;
			} else if (tp->obj < 0 && tp->obj > MAX_JVARS && t.obj > 0) {
				assert(bitmat->dimension == PSO_BITMAT);
				columnid = t.obj;
			}
		} else {
			assert(0);
		}
	}


	if (bitmat != NULL) {
		struct row r = {rowid, NULL};
		vector<struct row>::iterator rowitr = mybinary_search(bitmat->vbm.begin(), bitmat->vbm.end(), r);
		if (rowitr == bitmat->vbm.end()) {
			if (tp->rollback) {
				//COMMENT: For queries with cyclic dependency among jvars
				//it can happen that a value that got deleted from one bitmat
				//while joining over one var was not reflected in other
				//e.g. LUBM Q12 or OPTIONAL joins
//				cout << tp->toString() << " rowid " << rowid << endl;
//				assert(0);
				return;
			}

			//COMMENT: whenever setting values to 'null' is triggered by
			//absence of required values, that particular tp-node takes care
			//of nullifying all its peers and their slaves.
			//When 'null' values are acquired from a peer p1 or master m1, nullification
			//of other peers and slaves is not necessary as that p1 or m1 has taken
			//care of nullifying its peers and their slaves
			assert(tp->strlevel.find("s") != string::npos);
			t.sub = 0; t.pred = 0; t.obj = 0;
//			set_neighbors_null(bfsarr[0]);
			q_to_gr_node_map[bfsarr[0]] = t;
			match_query_graph_new(outfile, &bfsarr[1], sidx+1, eidx, num_null_pads, bestm);
			q_to_gr_node_map.erase(bfsarr[0]);
			return;
		}
		r.data = (*rowitr).data;
		if (columnid > 0) {
			if (!check_bit(&r, columnid)) {
				if (tp->rollback)
					return;
				t.sub = 0; t.pred = 0; t.obj = 0;
//				set_neighbors_null(bfsarr[0]);
			}
			q_to_gr_node_map[bfsarr[0]] = t;
			match_query_graph_new(outfile, &bfsarr[1], sidx+1, eidx, num_null_pads, bestm);
			q_to_gr_node_map.erase(bfsarr[0]);
			return;

		} else {
			match_triples_in_row(bitmat, &(*rowitr), bfsarr[0], outfile, bfsarr, sidx, eidx, num_null_pads, bestm);
			return;
		}
	}

//	if (sidx < eidx) {
//		TP *tpnext = (TP *)(bfsarr[1])->data;
////		cout << "tp is "<< tp->sub << " " << tp->pred << " " << tp->obj << endl;
////		cout << "tpnext is "<< tpnext->sub << " " << tpnext->pred << " " << tpnext->obj << endl;
//
//		if ( (tp->sub < 0 && tpnext->sub < 0 && tp->sub == tpnext->sub)
//			|| 	(tp->sub < 0 && tpnext->obj < 0 && tp->sub == tpnext->obj)
//			) {
////			cout << "CAME HERE1 tp->bitmat.dim " << tp->bitmat.dimension << " bitmat2.dim " << tp->bitmat2.dimension << endl;
//			if (tp->bitmat.dimension == SPO_BITMAT) {
////				cout << "bitmat got set to tp->bitmat SPO" << endl;
//				bitmat = &(tp->bitmat);
//			} else if (tp->bitmat2.dimension == SPO_BITMAT) {
////				cout << "bitmat got set to tp->bitmat2 SPO" << endl;
//				bitmat = &(tp->bitmat2);
//			} else if (tp->bitmat.dimension == POS_BITMAT) {
//				bitmat = &(tp->bitmat);
//			} else {
//				assert(0);
//			}
//		}
//		if (bitmat == NULL &&
//			((tp->obj < 0 && tpnext->sub < 0 && tp->obj == tpnext->sub)
//				|| 	(tp->obj < 0 && tpnext->obj < 0 && tp->obj == tpnext->obj))
//			) {
////			cout << "CAME HERE2" << endl;
//			if (tp->bitmat.dimension == OPS_BITMAT) {
////				cout << "bitmat got set to tp->bitmat OPS" << endl;
//				bitmat = &(tp->bitmat);
//			} else if (tp->bitmat2.dimension == OPS_BITMAT) {
////				cout << "bitmat got set to tp->bitmat2 OPS" << endl;
//				bitmat = &(tp->bitmat2);
//			} else if (tp->bitmat.dimension == PSO_BITMAT) {
//				bitmat = &(tp->bitmat);
//			} else {
//				assert(0);
//			}
//		}
//		if (bitmat == NULL && tp->pred < 0 && tpnext->pred < 0 && tp->pred == tpnext->pred) {
////			cout << "CAME HERE3" << endl;
//			if (tp->bitmat.dimension == POS_BITMAT || tp->bitmat.dimension == PSO_BITMAT) {
//				cout << "bitmat got set to tp->bitmat PSO or POS" << endl;
//				bitmat = &(tp->bitmat);
//			} else {
//				assert(0);
//			}
//		}
//	}
//
//	if (bitmat == NULL && (sidx < eidx)){
//		cout << "check_set_pos_and_proceed: sidx " << sidx << " eidx " << eidx << endl;
//		assert(0);// bitmat = &(tp->bitmat);
//	}
//	if (bitmat == NULL) {
	bitmat = &(tp->bitmat);
//	}

	for (vector<struct row>::iterator rowitr = bitmat->vbm.begin(); rowitr != bitmat->vbm.end(); rowitr++) {
		match_triples_in_row(bitmat, &(*rowitr), bfsarr[0], outfile, bfsarr, sidx, eidx, num_null_pads, bestm);
	}
}

void match_query_graph_new(FILE *outfile, struct node **bfsarr, int sidx, int eidx, char *null_pad_str, bool bestm)
{
	if (sidx > eidx) {
		print_res(outfile, null_pad_str, bfsarr, bestm);
		return;
	}

	struct node *n = bfsarr[0];
	TP *tp = (TP *)n->data;
	struct triple t = {0, 0, 0};
	unsigned int sub_pos = 0, pred_pos = 0, obj_pos = 0;
	bool sub_set = false, pred_set = false, obj_set = false;

	if (tp->isSlaveOfAnyone(null_perm)) {
		assert(tp->strlevel.find("s") != string::npos);
		q_to_gr_node_map[n] = t;
		match_query_graph_new(outfile, &bfsarr[1], sidx+1, eidx, null_pad_str, bestm);
		q_to_gr_node_map.erase(n);
		return;
	}
	if (tp->sub > 0)
		t.sub = tp->sub;
	if (tp->pred > 0)
		t.pred = tp->pred;
	if (tp->obj > 0)
		t.obj = tp->obj;

	for (LIST *q_neighbor = n->nextTP; q_neighbor; ) {
		TP *other = (TP *)q_neighbor->gnode->data;
		if (q_to_gr_node_map.find(q_neighbor->gnode) == q_to_gr_node_map.end()) {
			q_neighbor = q_neighbor->next;
			continue;
		}
		struct triple nt = q_to_gr_node_map[q_neighbor->gnode];

		if (other->sub < 0 && other->sub == tp->sub) {
			//S-S join
			if (t.sub != 0 && nt.sub != 0 && t.sub != nt.sub)
				assert(0);
			t.sub = nt.sub;
			sub_set = true;
		} else if (other->obj < 0 && other->obj == tp->sub) {
			//S-O join
			if (t.sub != 0 && nt.obj != 0 && t.sub != nt.obj)
				assert(0);
			t.sub = nt.obj;
			sub_set = true;
		}
		if (sub_set && t.sub == 0) {
			//nullification through transitivity
			t.pred = 0; t.obj = 0;
			q_to_gr_node_map[n] = t;
			match_query_graph_new(outfile, &bfsarr[1], sidx+1, eidx, null_pad_str, bestm);
			q_to_gr_node_map.erase(n);
			return;
		}

		if (other->sub < 0 && other->sub == tp->obj) {
			//S-O join
			if (t.obj != 0 && nt.sub != 0 && t.obj != nt.sub)
				assert(0);
			t.obj = nt.sub;
			obj_set = true;
		} else if (other->obj < 0 && other->obj == tp->obj) {
			//O-O join
			if (t.obj != 0 && nt.obj != 0 && t.obj != nt.obj)
				assert(0);
			t.obj = nt.obj;
			obj_set = true;
		}
		if (obj_set && t.obj == 0) {
			//nullification through transitivity
			t.pred = 0; t.sub = 0;
			q_to_gr_node_map[n] = t;
			match_query_graph_new(outfile, &bfsarr[1], sidx+1, eidx, null_pad_str, bestm);
			q_to_gr_node_map.erase(n);
			return;
		}

		if (other->pred < 0 && other->pred == tp->pred) {
			if (t.pred != 0 && nt.pred != 0 && t.pred != nt.pred)
				assert(0);
			t.pred = nt.pred;
			pred_set = true;
		}
		if (pred_set && t.pred == 0) {
			t.obj = 0; t.sub = 0;
			q_to_gr_node_map[n] = t;
			match_query_graph_new(outfile, &bfsarr[1], sidx+1, eidx, null_pad_str, bestm);
			q_to_gr_node_map.erase(n);
			return;
		}
		q_neighbor = q_neighbor->next;
	}

	check_set_pos_and_proceed(bfsarr, t, outfile, sidx, eidx, null_pad_str, bestm);

}

FILE * print_triples_to_file(unsigned int rownum, unsigned int bmnum, unsigned int column, int bmdim, FILE *outfile)
{
	switch(bmdim) {
		case(SPO_BITMAT):
			if (column > gnum_comm_so) {
				column = column - gnum_comm_so + gnum_subs;
			}
			fprintf(outfile, "%d:%d:%d\n", rownum, bmnum, column);
			break;
		case(OPS_BITMAT):
			if (rownum > gnum_comm_so) {
				rownum = rownum - gnum_comm_so + gnum_subs;
			}

			fprintf(outfile, "%d:%d:%d\n", column, bmnum, rownum);
			break;
		case(PSO_BITMAT):
			if (column > gnum_comm_so) {
				column = column - gnum_comm_so + gnum_subs;
			}
			fprintf(outfile, "%d:%d:%d\n", bmnum, rownum, column);
			break;
		case(POS_BITMAT):
			if (bmnum > gnum_comm_so) {
				bmnum = bmnum - gnum_comm_so + gnum_subs;
			}
			fprintf(outfile, "%d:%d:%d\n", column, rownum, bmnum);
			break;
	}
	return outfile;
}

/////////////////////////////////////////////////////////
void list_enctrips_bitmat_new(BitMat *bitmat, unsigned int bmnum, vector<twople> &twoplist, FILE *outfile)
{
#if USE_MORE_BYTES
	unsigned long opos = 0;
	unsigned long tmpcnt = 0, bitcnt = 0;
#else
	unsigned int opos = 0;
	unsigned int tmpcnt = 0, bitcnt = 0;
#endif
	unsigned int total_cnt = 0;
	bool flag;
	unsigned int cnt = 0;
//	FILE *fp;
	unsigned int ressize = 0, rowcnt;

	if (outfile == NULL)
		twoplist.clear();

	for (std::list<struct row>::iterator it = bitmat->bm.begin(); it != bitmat->bm.end(); it++) {
		unsigned char *data = (*it).data;
		unsigned int rownum = (*it).rowid;
		flag = data[ROW_SIZE_BYTES] & 0x01;
		unsigned char format = data[ROW_SIZE_BYTES] & 0x02;
		ressize = 0;
		memcpy(&ressize, data, ROW_SIZE_BYTES);
		total_cnt = (ressize - 1)/GAP_SIZE_BYTES;
		cnt = 0;
		bitcnt = 0;

		while (cnt < total_cnt) {
			memcpy(&tmpcnt, &data[cnt*GAP_SIZE_BYTES + 1 + ROW_SIZE_BYTES], GAP_SIZE_BYTES);

			if (format == 0x02) {
//				cout << rownum << ":" << bmnum << ":"<< tmpcnt << endl;
				if (outfile != NULL) {
					outfile = print_triples_to_file(rownum, bmnum, tmpcnt, bitmat->dimension, outfile);
//					fprintf(outfile, "%d:%d:%d\n", rownum, bmnum, tmpcnt);
				} else {
					struct twople t = {rownum, tmpcnt};
					twoplist.push_back(t);
				}
			} else {
				if (flag) {
					for (opos = bitcnt + 1; opos <= bitcnt+tmpcnt; opos++) {
						if (outfile != NULL) {
							outfile = print_triples_to_file(rownum, bmnum, opos, bitmat->dimension, outfile);
//							fprintf(outfile, "%d:%d:%d\n", rownum, bmnum, opos);
						} else {
							struct twople t = {rownum, opos};
							twoplist.push_back(t);
						}
					}
				}
			}
			bitcnt += tmpcnt;
			cnt++;
			flag = !flag;
		}
	}
}
/////////////////////////////
void list_enctrips_bitmat2(BitMat *bitmat, unsigned int bmnum, vector<twople> &twoplist, FILE *outfile)
{
#if USE_MORE_BYTES
	unsigned long opos = 0;
	unsigned long tmpcnt = 0, bitcnt = 0;
#else
	unsigned int opos = 0;
	unsigned int tmpcnt = 0, bitcnt = 0;
#endif
	unsigned int total_cnt;
	bool flag;
	unsigned int cnt;
//	FILE *fp;
	unsigned int ressize = 0, rowcnt;

	if (outfile == NULL)
		twoplist.clear();

	//Depending on the dimension of the bitmat
	//it's either a SPO or OPS bitmat
	//TODO: this will change for large datasets
	//if you are maining a hash_map instead of char**

	for (std::vector<struct row>::iterator it = bitmat->vbm.begin(); it != bitmat->vbm.end(); it++) {

		unsigned char *data = (*it).data;
		unsigned int rownum = (*it).rowid;
		flag = data[ROW_SIZE_BYTES] & 0x01;
		unsigned char format = data[ROW_SIZE_BYTES] & 0x02;
		ressize = 0;
		memcpy(&ressize, data, ROW_SIZE_BYTES);
		total_cnt = (ressize - 1)/GAP_SIZE_BYTES;
		cnt = 0;
		bitcnt = 0;

		while (cnt < total_cnt) {
			memcpy(&tmpcnt, &data[cnt*GAP_SIZE_BYTES + 1 + ROW_SIZE_BYTES], GAP_SIZE_BYTES);
			if (format == 0x02) {
				if (outfile != NULL) {
					outfile = print_triples_to_file(rownum, bmnum, tmpcnt, bitmat->dimension, outfile);
//					fprintf(outfile, "%u:%u:%u\n", rownum, bmnum, tmpcnt);
				} else {
					struct twople t = {rownum, tmpcnt};
					twoplist.push_back(t);
				}
			} else {
				if (flag) {
					for (opos = bitcnt+1; opos <= bitcnt+tmpcnt; opos++) {
						if (outfile != NULL) {
							outfile = print_triples_to_file(rownum, bmnum, opos, bitmat->dimension, outfile);
//							fprintf(outfile, "%u:%u:%u\n", rownum, bmnum, opos);
						} else {
							struct twople t = {rownum, opos};
							twoplist.push_back(t);
						}
					}
				}
			}
			bitcnt += tmpcnt;
			cnt++;
			flag = !flag;
		}

	}

}

unsigned long get_size_of_bitmat(int dimension, unsigned int node)
{
	char dumpfile[1024];
	off64_t off1 = 0, off2 = 0;
	switch (dimension) {
	case SPO_BITMAT:
		sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_SPO")].c_str());
		off1 = get_offset(dumpfile, node);
		if (node == gnum_preds) {
			off2 = vectfd[0].second;
		} else {
			off2 = get_offset(dumpfile, node+1);
		}
		return (off2 - off1);
		break;
	case OPS_BITMAT:
		sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_OPS")].c_str());
		off1 = get_offset(dumpfile, node);
		if (node == gnum_preds) {
			off2 = vectfd[2].second;
		} else {
			off2 = get_offset(dumpfile, node+1);
		}
		return (off2 - off1);
		break;
	case PSO_BITMAT:
		sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_PSO")].c_str());
		off1 = get_offset(dumpfile, node);
		if (node == gnum_subs) {
			off2 = vectfd[4].second;
		} else {
			off2 = get_offset(dumpfile, node+1);
		}
		return (off2 - off1);
		break;
	case POS_BITMAT:
		sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_POS")].c_str());
		off1 = get_offset(dumpfile, node);
		if (node == gnum_objs) {
			off2 = vectfd[6].second;
		} else {
			off2 = get_offset(dumpfile, node+1);
		}
		return (off2 - off1);
		break;
	default:
		assert(0);
		break;
	}
	return 0;
}

void list_all_data(int dimension, unsigned int node, char *filename)
{
	BitMat bitmat;
	char dumpfile[1024];
	FILE *file = NULL;
	if (filename != NULL) {
		file = fopen64(filename, "a");
		assert(file != NULL);
		setvbuf(file, NULL, _IOFBF, 0x8000000);

	}
	switch (dimension) {
		case SPO_BITMAT:
			sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_SPO")].c_str());
			init_bitmat(&bitmat, gnum_subs, gnum_preds, gnum_objs, gnum_comm_so, SPO_BITMAT);
			cout << "Listing triples from SPO bitmats" << endl;
			if (node != 0) {
//				unsigned long offset = get_offset(dumpfile, node);
//				if (offset == 0 && node > 1) {
//					cout << "Non-existing predicate " << node << endl;
//					assert(0);
//				}
//				if (offset == 0 && node > 1 && offset == get_offset(dumpfile, node+1)) {
//					cout << "Non-existing predicate " << node << endl;
//					assert(0);
//				}
				load_from_dump_file(dumpfile, node, &bitmat, true, true, NULL, 0, 0, NULL, 0, true);
				vector<twople> twoples;
				list_enctrips_bitmat_new(&bitmat, node, twoples, file);
				for (vector<twople>::iterator it = twoples.begin(); it != twoples.end(); it++) {
					fprintf(file, "%u:%u:%u\n", (*it).sub, node, (*it).obj);
				}
				twoples.clear();
				bitmat.freebm();
//				clear_rows(&bitmat, true, true, true);
				if (file != NULL) {
					fclose(file);
				}

				return;
			}
//			{
//				unsigned long offset1 = get_offset(dumpfile, 1);
//				unsigned long offset2 = get_offset(dumpfile, 2);
//				cout << "Size of bitmat 1 " << (offset2 - offset1) << endl;
//				offset1 = get_offset(dumpfile, 3);
//				cout << "Size of bitmat 2 " << (offset1 - offset2) << endl;
//				offset2 = get_offset(dumpfile, 4);
//				cout << "Size of bitmat 3 " << (offset2 - offset1) << endl;
//				offset1 = get_offset(dumpfile, 8);
//				cout << "Size of bitmat 4 " << (offset1 - offset2) << endl;
//			}
			for (unsigned int i = 1; i <= gnum_preds; i++) {
//				unsigned long offset = get_offset(dumpfile, i);
				//This offset is just a placeholder
//				if (offset == 0 && i > 1) {
//					cout << "Non-existing predicate " << i << endl;
//					continue;
//				}
//				if (offset == 0 && i == 1 && offset == get_offset(dumpfile, i+1)) {
//					cout << "Non-existing predicate " << i << endl;
//					continue;
//				}

				cout << "Loading bitmat " << i << endl;
				load_from_dump_file(dumpfile, i, &bitmat, true, true, NULL, 0, 0, NULL, 0, true);
				cout << "Done loading bitmat " << i << endl;
				vector<twople> twoples;
				cout << "Listing triples in bitmat " << i << endl;
				list_enctrips_bitmat_new(&bitmat, i, twoples, file);
				cout << "Done listing triples in bitmat " << i << endl;
				for (vector<twople>::iterator it = twoples.begin(); it != twoples.end(); it++) {
					fprintf(file, "%u:%u:%u\n", (*it).sub, i, (*it).obj);
//					cerr << (*it).sub << ":" << i << ":" << (*it).obj << endl;
				}
				twoples.clear();
				cout << "Clearing rows for bitmat " << i << endl;
				bitmat.freebm();
//				clear_rows(&bitmat, true, true, true);
			}
			break;

		case OPS_BITMAT:
			sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_OPS")].c_str());
			init_bitmat(&bitmat, gnum_objs, gnum_preds, gnum_subs, gnum_comm_so, OPS_BITMAT);
			cout << "Listing triples from OPS bitmats" << endl;

			if (node != 0) {
//				unsigned long offset = get_offset(dumpfile, node);
//				cout << "Offset for bmnum " << node << " is "<< offset << endl;
				//This offset is just a placeholder
//				if (offset == 0 && node > 1) {
//					cout << "Non-existing predicate " << node << endl;
//					assert(0);
//				}
//				if (offset == 0 && node == 1 && offset == get_offset(dumpfile, node+1)) {
//					cout << "Non-existing predicate " << node << endl;
//					assert(0);
//				}

				load_from_dump_file(dumpfile, node, &bitmat, true, true, NULL, 0, 0, NULL, 0, true);
				vector<twople> twoples;
				list_enctrips_bitmat_new(&bitmat, node, twoples, file);
				for (vector<twople>::iterator it = twoples.begin(); it != twoples.end(); it++) {
					fprintf(file, "%u:%u:%u\n", (*it).obj, node, (*it).sub);
//					cerr << (*it).obj << ":" << node << ":" << (*it).sub << endl;
				}
				twoples.clear();
				bitmat.freebm();
//				clear_rows(&bitmat, true, true, true);
				if (file != NULL) {
					fclose(file);
				}

				return;

			}

			for (unsigned int i = 1; i <= gnum_preds; i++) {
//				unsigned long offset = get_offset(dumpfile, i);
				//This offset is just a placeholder
//				if (offset == 0 && i > 1) {
//					cout << "Non-existing predicate " << i << endl;
//					continue;
//				}
//				if (offset == 0 && i == 1 && offset == get_offset(dumpfile, i+1)) {
//					cout << "Non-existing predicate " << i << endl;
//					continue;
//				}
//
				load_from_dump_file(dumpfile, i, &bitmat, true, true, NULL, 0, 0, NULL, 0, true);
				vector<twople> twoples;
				list_enctrips_bitmat_new(&bitmat, i, twoples, file);
				for (vector<twople>::iterator it = twoples.begin(); it != twoples.end(); it++) {
					fprintf(file, "%u:%u:%u\n", (*it).obj, i, (*it).sub);
//					cerr << (*it).obj << ":" << i << ":" << (*it).sub << endl;
				}
				twoples.clear();
				bitmat.freebm();
//				clear_rows(&bitmat, true, true, true);
			}
			break;

		case PSO_BITMAT:
			sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_PSO")].c_str());
			init_bitmat(&bitmat, gnum_preds, gnum_subs, gnum_objs, gnum_comm_so, PSO_BITMAT);
			cout << "Listing triples from PSO bitmats" << endl;

			if (node != 0) {
//				unsigned long offset = get_offset(dumpfile, node);
				//This offset is just a placeholder
//				if (offset == 0 && node > 1) {
//					cout << "Non-existing predicate " << node << endl;
//					assert(0);;
//				}
//
//				if (offset == 0 && node == 1 && offset == get_offset(dumpfile, node+1)) {
//					cout << "Non-existing predicate " << node << endl;
//					assert(0);
//				}
				load_from_dump_file(dumpfile, node, &bitmat, true, true, NULL, 0, 0, NULL, 0, true);
				vector<twople> twoples;
				list_enctrips_bitmat_new(&bitmat, node, twoples, file);
				for (vector<twople>::iterator it = twoples.begin(); it != twoples.end(); it++) {
					fprintf(file, "%u:%u:%u\n", node, (*it).sub, (*it).obj);
//					cerr << node << ":" << (*it).sub << ":" << (*it).obj << endl;
				}
				twoples.clear();
//				clear_rows(&bitmat, true, true, true);
				bitmat.freebm();

				if (file != NULL) {
					fclose(file);
				}
				return;

			}

			for (unsigned int i = 1; i <= gnum_subs; i++) {
//				unsigned long offset = get_offset(dumpfile, i);
				//This offset is just a placeholder
//				if (offset == 0 && i > 1) {
//					cout << "Non-existing predicate " << i << endl;
//					continue;
//				}
//
//				if (offset == 0 && i == 1 && offset == get_offset(dumpfile, i+1)) {
//					cout << "Non-existing predicate " << i << endl;
//					continue;
//				}

				load_from_dump_file(dumpfile, i, &bitmat, true, true, NULL, 0, 0, NULL, 0, true);
				vector<twople> twoples;
				list_enctrips_bitmat_new(&bitmat, i, twoples, file);
				for (vector<twople>::iterator it = twoples.begin(); it != twoples.end(); it++) {
					fprintf(file, "%u:%u:%u\n", i, (*it).sub, (*it).obj);
//					cerr << i << ":" << (*it).sub << ":" << (*it).obj << endl;
				}
				twoples.clear();
				bitmat.freebm();
//				clear_rows(&bitmat, true, true, true);
			}
			break;

		case POS_BITMAT:
			sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_POS")].c_str());
			init_bitmat(&bitmat, gnum_preds, gnum_objs, gnum_subs, gnum_comm_so, POS_BITMAT);
			cout << "Listing triples from POS bitmats" << endl;
			if (node != 0) {
//				unsigned long offset = get_offset(dumpfile, node);
				//This offset is just a placeholder
//				if (offset == 0 && node > 1) {
//					cout << "Non-existing predicate " << node << endl;
//					assert(0);;
//				}
//				if (offset == 0 && node == 1 && offset == get_offset(dumpfile, node+1)) {
//					cout << "Non-existing predicate " << node << endl;
//					assert(0);
//				}

				load_from_dump_file(dumpfile, node, &bitmat, true, true, NULL, 0, 0, NULL, 0, true);
				vector<twople> twoples;
				list_enctrips_bitmat_new(&bitmat, node, twoples, file);
				for (vector<twople>::iterator it = twoples.begin(); it != twoples.end(); it++) {
					fprintf(file, "%u:%u:%u\n", (*it).obj, (*it).sub, node);
//					cerr << (*it).obj << ":" << (*it).sub << ":" << node << endl;
				}
				twoples.clear();
//				clear_rows(&bitmat, true, true, false);
				bitmat.freebm();
				if (file != NULL) {
					fclose(file);
				}

				return;
			}

			for (unsigned int i = 1; i <= gnum_objs; i++) {
//				unsigned long offset = get_offset(dumpfile, i);
				//This offset is just a placeholder
//				if (offset == 0 && i > 1) {
//					cout << "Non-existing predicate " << i << endl;
//					continue;
//				}
//
//				if (offset == 0 && i == 1 && offset == get_offset(dumpfile, i+1)) {
//					cout << "Non-existing predicate " << i << endl;
//					continue;
//				}

				load_from_dump_file(dumpfile, i, &bitmat, true, true, NULL, 0, 0, NULL, 0, true);
				vector<twople> twoples;
				list_enctrips_bitmat_new(&bitmat, i, twoples, file);
				for (vector<twople>::iterator it = twoples.begin(); it != twoples.end(); it++) {
					fprintf(file, "%u:%u:%u\n", (*it).obj, (*it).sub, i);
//					cerr << (*it).obj << ":" << (*it).sub << ":" << i << endl;
				}
				twoples.clear();
				clear_rows(&bitmat, true, true, false);
			}
			break;
		default:
			break;

	}
	if (file != NULL) {
		fclose(file);
	}

}

////////////////////////////////////////////////////////////

unsigned long list_enctriples_in_bitmat(unsigned char **bitmat, unsigned int dimension, unsigned int num_triples, char *triplefile)
{
#if USE_MORE_BYTES
	unsigned long ppos_start_bit, ppos_end_bit, opos = 0;
	unsigned long tmpcnt = 0, bitcnt = 0, gap;
	unsigned long triplecnt = 0;
#else
	unsigned int ppos_start_bit, ppos_end_bit, opos = 0;
	unsigned int tmpcnt = 0, bitcnt = 0, gap;
	unsigned int triplecnt = 0;
#endif
	unsigned long pred_cnt;
	unsigned int i, total_cnt;
	bool flag, inside;
	unsigned int cnt;
	FILE *fp;
	unsigned int ressize = 0, rowcnt, chunk_bytes;

//	if (num_triples == 0) {
//		return;
//	}

	fp = fopen64(triplefile, "w");
	if (fp == NULL) {
		printf("****ERROR opening triplefile %s\n", triplefile);
		assert(0);
	}

	//Depending on the dimension of the bitmat
	//it's either a SPO or OPS bitmat
	if (dimension == SUB_DIMENSION) {
		rowcnt = gnum_subs;
		chunk_bytes = gobject_bytes;
	} else if (dimension == OBJ_DIMENSION) {
		rowcnt = gnum_objs;
		chunk_bytes = gsubject_bytes;
	}
	for (i = 0; i < rowcnt; i++) {
		if (bitmat[i] == NULL)
			continue;
		unsigned char *resptr = bitmat[i] + ROW_SIZE_BYTES;
		flag = resptr[0] & 0x01;
		ressize = 0;
		memcpy(&ressize, bitmat[i], ROW_SIZE_BYTES);
		total_cnt = (ressize - 1)/GAP_SIZE_BYTES;
		cnt = 0;
		bitcnt = 0;
		pred_cnt = 0;
		ppos_start_bit = pred_cnt * (chunk_bytes << 3) + 1;
		ppos_end_bit = ppos_start_bit + (chunk_bytes << 3) - 1;

		memcpy(&tmpcnt, &resptr[cnt*GAP_SIZE_BYTES + 1], GAP_SIZE_BYTES);
		bitcnt += tmpcnt;
		inside = false;

		while (cnt < total_cnt) {
			if (bitcnt >= ppos_start_bit && bitcnt <= ppos_end_bit) {
				inside = true;
				if (flag) {
					gap = (bitcnt - ppos_start_bit + 1) < tmpcnt ?
								(bitcnt - ppos_start_bit + 1)	:  tmpcnt;
					opos = bitcnt - gap + 1 - (pred_cnt * (chunk_bytes << 3));
					while (gap > 0) {
//						triples[triplecnt] = (unsigned char *) malloc (TRIPLE_STR_SPACE);
//						memset(triples[triplecnt], 0, TRIPLE_STR_SPACE);
						if (dimension == SUB_DIMENSION)
							fprintf(fp, "%u:%u:%u\n", i+1, pred_cnt+1, opos);
						else if (dimension == OBJ_DIMENSION)
							fprintf(fp, "%u:%u:%u\n", opos, pred_cnt+1, i+1);

						triplecnt++;
						gap--;
						opos++;
					}
				}
				if (bitcnt == ppos_end_bit) {
					inside = false;
					pred_cnt++;
					ppos_start_bit = pred_cnt * (chunk_bytes << 3) + 1;
					ppos_end_bit = ppos_start_bit + (chunk_bytes << 3) - 1;
				}
				cnt++;
				if (cnt >= total_cnt)
					break;
				flag = !flag;
				memcpy(&tmpcnt, &resptr[cnt*GAP_SIZE_BYTES+1], GAP_SIZE_BYTES);
				bitcnt += tmpcnt;
			} else if (inside) {
				inside = false;
				if (flag) {
					gap = ppos_end_bit - (bitcnt - tmpcnt);
					opos = ppos_end_bit - gap + 1 - (pred_cnt * (chunk_bytes << 3));

					while (gap > 0) {
//						triples[triplecnt] = (unsigned char *) malloc (TRIPLE_STR_SPACE);
//						memset(triples[triplecnt], 0, TRIPLE_STR_SPACE);
						if (dimension == SUB_DIMENSION)
							fprintf(fp, "%u:%u:%u\n", i+1, pred_cnt+1, opos);
						else if (dimension == OBJ_DIMENSION)
							fprintf(fp, "%u:%u:%u\n", opos, pred_cnt+1, i+1);
						triplecnt++;
						gap--;
						opos++;
					}
				}
				pred_cnt++;
				ppos_start_bit = pred_cnt * (chunk_bytes << 3) + 1;
				ppos_end_bit = ppos_start_bit + (chunk_bytes << 3) - 1;

			} else {
				pred_cnt++;
				ppos_start_bit = pred_cnt * (chunk_bytes << 3) + 1;
				ppos_end_bit = ppos_start_bit + (chunk_bytes << 3) - 1;
			}

		}

	}

	fclose(fp);

	return triplecnt;

//	printf("Total triples %u\n", num_triples);

}
////////////////////////////////////////////////////////////

void dump_out_data(FILE *fdump_fp, BitMat *bitmat, char *fname_tmp)
{
//	int fd = 0;
	unsigned int i = 0;
	unsigned int size = 0;

	if (bitmat->num_triples != 0) {
//		write(fd, &bitmat->num_triples, sizeof(unsigned int));
//		cout << "Num triples - " << bitmat->num_triples << endl;;
		fwrite(&bitmat->num_triples, sizeof(unsigned int), 1, fdump_fp);
		gtotal_size += sizeof(unsigned int);
	}
	if (bitmat->subfold != NULL && bitmat->objfold != NULL) {
		if (bitmat->dimension == PSO_BITMAT || bitmat->dimension == POS_BITMAT || comp_folded_arr) {
//			write(fd, comp_subfold, comp_subfold_size + ROW_SIZE_BYTES);
			fwrite(comp_subfold, comp_subfold_size + ROW_SIZE_BYTES, 1, fdump_fp);
			gtotal_size += comp_subfold_size + ROW_SIZE_BYTES;
		} else {
			fwrite(bitmat->subfold, bitmat->subject_bytes, 1, fdump_fp);
			gtotal_size += bitmat->subject_bytes;
		}

		if (bitmat->dimension != PSO_BITMAT && bitmat->dimension != POS_BITMAT) {
			if (comp_folded_arr) {
				//compress the array
				unsigned int comp_objfold_size = dgap_compress_new(bitmat->objfold, bitmat->object_bytes, comp_objfold);
				fwrite(&comp_objfold_size, ROW_SIZE_BYTES, 1, fdump_fp);
				fwrite(comp_objfold, comp_objfold_size, 1, fdump_fp);
				gtotal_size += ROW_SIZE_BYTES + comp_objfold_size;
			} else {
				fwrite(bitmat->objfold, bitmat->object_bytes, 1, fdump_fp);
				gtotal_size += bitmat->object_bytes;
			}
		}

	}
	size = 0;

	if (0 == bitmat->bm.size()) {
		int tmpdump = open(fname_tmp, O_RDONLY | O_LARGEFILE);
		if (tmpdump < 0)
			assert(0);

		unsigned char readbuf[0x1000000];
		ssize_t rbytes = 1;

		while (rbytes > 0) {
			rbytes = read(tmpdump, readbuf, 0x1000000);
			if (rbytes > 0) {
				fwrite(readbuf, rbytes, 1, fdump_fp);
				gtotal_size += rbytes;
			}
		}
		close(tmpdump);

	} else {
		for (std::list<struct row>::iterator it = bitmat->bm.begin(); it != bitmat->bm.end(); it++) {
			memcpy(&size, (*it).data, ROW_SIZE_BYTES);
			fwrite((*it).data, size + ROW_SIZE_BYTES, 1, fdump_fp);
			gtotal_size += (size + ROW_SIZE_BYTES);
		}
	}

//	printf("dump_out_data: Total bytes to file %u #triples %u\n", gtotal_size, bitmat->num_triples);

}

///////////////////////////////////////////////

void load_data_vertically(char *file, vector<struct twople> &twoplelist, BitMat *bitmat, char *fname_dump,
						bool ondisk, bool invert, bool listload, char *tmpfile)
{
	FILE *fp = NULL, *fp_table = NULL;
	FILE *fdump_fp = NULL;
	FILE *tmpdump = NULL;
	unsigned int spos = 0, ppos = 0, opos = 0;
	unsigned int sprev = 0, pprev = 0;
	unsigned char l, m, n;
	bool start = true;
//	unsigned char **table;
	//TODO: change later
//	unsigned char table[bitmat->num_preds][table_col_bytes];
//	unsigned char table[20000000][table_col_bytes];
//	int fdump_fd = -1;

//	cout << "Inside load_data_vertically" << endl;

	if (file != NULL) {
		fp = fopen64(file, "r");
		if (fp == NULL) {
			printf("Error opening file %s\n", file);
			return;
		}
	}

//	cout << "load_data_vertically : HERE1" << endl;
	if (grow != NULL) {
		free(grow); grow = NULL;
	}
	grow = (unsigned char *) malloc (GAP_SIZE_BYTES * (bitmat->num_objs / 2));
	memset(grow, 0, GAP_SIZE_BYTES * (bitmat->num_objs / 2));
	grow_size = GAP_SIZE_BYTES * (bitmat->num_objs / 2);
//	cout << "load_data_vertically : HERE2" << endl;

	if (ondisk) {
		//TODO: while loading in chunks no need to alloc num_preds
		char tablefile[1024];
		sprintf(tablefile, "%s_table", fname_dump);

		fp_table = fopen(tablefile, "wb");
		if (fp_table == NULL) {
			cout << "*** ERROR: Could not open tablefile " << tablefile << endl;
			assert(0);
		}
		if (setvbuf(fp_table, buf_table, _IOFBF, 0xf000000) != 0)
			assert(0);

//		table = (unsigned char **) malloc (bitmat->num_preds * sizeof(unsigned char *));
//
//		for (unsigned int i = 0; i < bitmat->num_preds; i++) {
//			table[i] = (unsigned char *) malloc (table_col_bytes * sizeof (unsigned char));
//			memset(table[i], 0, table_col_bytes * sizeof (unsigned char));
//		}
//		memset(table, 0, bitmat->num_preds * table_col_bytes);
	}

//	cout << "load_data_vertically : HERE3" << endl;
	unsigned int total_triples = 0;
	prev_bitset = 0;
	prev_rowbit = 1, comp_subfold_size = 0;
	gtotal_size = 0;

	bitmat->bm.clear();

//	cout << "load_data_vertically : HERE4" << endl;
	if (ondisk) {

		if (tmpfile != NULL) {
			tmpdump = fopen64(tmpfile, "wb");
			if (setvbuf(tmpdump, buf_tmp, _IOFBF, 0xf000000) != 0)
				assert(0);

			if (tmpdump == NULL) {
				cout << "Cannot open " << config[string("TMP_STORAGE")] << endl;
				assert(0);
			}
		}

		if (bitmat->dimension == PSO_BITMAT || bitmat->dimension == POS_BITMAT || comp_folded_arr) {
			comp_subfold = (unsigned char *) malloc (GAP_SIZE_BYTES * bitmat->num_subs);
			memset(comp_subfold, 0, GAP_SIZE_BYTES * bitmat->num_subs);
			if (bitmat->dimension != PSO_BITMAT && bitmat->dimension != POS_BITMAT) {
				comp_objfold = (unsigned char *) malloc (GAP_SIZE_BYTES * bitmat->num_objs);
				memset(comp_objfold, 0, GAP_SIZE_BYTES * bitmat->num_objs);
			}
		} else if (bitmat->subfold == NULL) {
			//TODO:
			bitmat->subfold = (unsigned char *) malloc (bitmat->subject_bytes * sizeof (unsigned char));
			memset(bitmat->subfold, 0, bitmat->subject_bytes * sizeof (unsigned char));
		}

		if (bitmat->objfold == NULL) {
			bitmat->objfold = (unsigned char *) malloc (bitmat->object_bytes * sizeof (unsigned char));
			memset(bitmat->objfold, 0, bitmat->object_bytes * sizeof (unsigned char));
		}

		fdump_fp = fopen64(fname_dump, "wb");
		if (fdump_fp == NULL) {
			cout << "Could not open " << fname_dump << endl;
			assert(0);
		}
		if (setvbuf(fdump_fp, buf_dump, _IOFBF, 0xf000000) != 0)
			assert(0);
	}

//	unsigned int pcnt = 0;
	unsigned int swap = 0;

	if (file == NULL) {
		//Load from the triplelist vector
//		cout << "load_data_vertically : HERE5" << endl;
		bitmat->num_triples = twoplelist.size();
		for (vector<struct twople>::iterator it = twoplelist.begin(); it != twoplelist.end(); it++) {
			spos = (*it).sub;
			opos = (*it).obj;
			if (invert) {
				swap = spos;
				spos = opos;
				opos = swap;
			}
			if (sprev != spos) {
				if (start) {
					map_to_row_wo_dgap_vertical(bitmat, spos, opos, sprev, false, true, ondisk,
							listload, NULL, false);
					start = false;
				} else {
					map_to_row_wo_dgap_vertical(bitmat, spos, opos, sprev, true, true, ondisk,
							listload, NULL, false);
				}
				sprev = spos;
			} else {
				map_to_row_wo_dgap_vertical(bitmat, spos, opos, sprev, false, false, ondisk,
						listload, NULL, false);
			}
		}

		//For the last row entries
		map_to_row_wo_dgap_vertical(bitmat, spos, opos, sprev, true, false, ondisk, listload, NULL, false);

		if (grow != NULL) {
			free(grow);
			grow = NULL;
		}

		return;

	}

	unsigned int tcount = 0;
	while (!feof(fp)) {
//		cout << "loading line" << endl;
		char line[50];
		char s[10], p[10], o[10];

		memset(line, 0, 50);
		if(fgets(line, sizeof(line), fp) != NULL) {
			char *c = NULL, *c2=NULL;
			c = strchr(line, ':');
			*c = '\0';
			strcpy(s, line);
			c2 = strchr(c+1, ':');
			*c2 = '\0';
			strcpy(p, c+1);
			c = strchr(c2+1, '\n');
			*c = '\0';
			strcpy(o, c2+1);
			tcount++;
			spos = atoi(s); ppos = atoi(p); opos = atoi(o);
			if (invert) {
				swap = spos;
				spos = opos;
				opos = swap;
			}
			if (pprev != ppos || sprev != spos) {
				if (start) {
					tmpdump = map_to_row_wo_dgap_vertical(bitmat, spos, opos, sprev, false, true,
								ondisk, listload, tmpdump, false);
					start = false;
					if (ppos != 1) {
						unsigned long tmpval = 0;
						for (unsigned int k = 1; k < ppos; k++) {
							fwrite(&tmpval, table_col_bytes, 1, fp_table);
						}
					}
					fwrite(&gtotal_size, table_col_bytes, 1, fp_table);

				} else {
					if (pprev != ppos) {
						if (ondisk) {
							tmpdump = map_to_row_wo_dgap_vertical(bitmat, spos, opos, sprev, true,
										false, ondisk, listload, tmpdump, true);
//							bitmat->num_triples = count_triples_in_bitmat(bitmat, bitmat->dimension);
							bitmat->num_triples = tcount-1;
							tcount = 1;
//							cout << fname_dump << " " << bitmat->num_triples << endl;
							total_triples += bitmat->num_triples;

							//TODO: remove this later
//							memcpy(table[pprev-1], &gtotal_size, table_col_bytes);
//							memcpy(table[pcnt], &gtotal_size, table_col_bytes);
//							pcnt++;

							if (comp_subfold_size != 0) {
								//COMMENT: No need to add last zeros
//								if (prev_rowbit != (bitmat->subject_bytes * 8)) {
//									//You will have to append later_0s
//									unsigned int later_0 = (bitmat->subject_bytes * 8) - prev_rowbit;
//									memcpy(&comp_subfold[ROW_SIZE_BYTES + comp_subfold_size], &later_0, GAP_SIZE_BYTES);
//									comp_subfold_size += GAP_SIZE_BYTES;
//								}
								memcpy(comp_subfold, &comp_subfold_size, ROW_SIZE_BYTES);
							}
							if (bitmat->dimension == SPO_BITMAT || bitmat->dimension == OPS_BITMAT) {
								cout << "load_data_vertically: Dumping data for BM " << pprev << endl;
							}

							if (tmpdump) {
								fclose(tmpdump);
								tmpdump = NULL;
							}

							dump_out_data(fdump_fp, bitmat, (char *)config[string("TMP_STORAGE")].c_str());

							if (ppos - 1 != pprev) {
								unsigned long tmpval = 0;
								for (unsigned int k = pprev+1; k < ppos; k++) {
									fwrite(&tmpval, table_col_bytes, 1, fp_table);
								}
							}

							fwrite(&gtotal_size, table_col_bytes, 1, fp_table);

							if (tmpfile != NULL) {
								tmpdump = fopen64(tmpfile, "wb");
								if (setvbuf(tmpdump, buf_tmp, _IOFBF, 0xf000000) != 0)
									assert(0);

								if (tmpdump == NULL) {
									cout << "Cannot open " << config[string("TMP_STORAGE")] << endl;
									assert(0);
								}
							}
							//Empty the earlier bitmat
							clear_rows(bitmat, true, true, true);
							comp_subfold_size = 0;
							prev_rowbit = 1;

							tmpdump = map_to_row_wo_dgap_vertical(bitmat, spos, opos, sprev, false,
												true, ondisk, listload, tmpdump, false);
						}
//						cout << "Cleared rows after pred " <<  pprev << endl;

					} else {
						tmpdump = map_to_row_wo_dgap_vertical(bitmat, spos, opos, sprev, true, true,
											ondisk, listload, tmpdump, false);
					}
				}
				sprev = spos;
				pprev = ppos;
			} else {
				tmpdump = map_to_row_wo_dgap_vertical(bitmat, spos, opos, sprev, false, false, ondisk,
											listload, tmpdump, false);
			}
		}
//		cout << "Done line" << endl;
	}
	//for the last entries
	if (spos != 0 && ppos != 0 && opos != 0) {
		tmpdump = map_to_row_wo_dgap_vertical(bitmat, spos, opos, sprev, true, false, ondisk, listload, tmpdump, false);

		if (ondisk) {
//			bitmat->num_triples = count_triples_in_bitmat(bitmat, bitmat->dimension);
			bitmat->num_triples = tcount;
			tcount = 0;
//			cout << fname_dump << " " << bitmat->num_triples << endl;
			total_triples += bitmat->num_triples;
//			memcpy(table[pprev-1], &gtotal_size, table_col_bytes);
//			memcpy(table[pcnt], &gtotal_size, table_col_bytes);
//			pcnt++;
			//TODO:
			if (comp_subfold_size != 0) {
//				if (prev_rowbit != (bitmat->subject_bytes * 8)) {
//					//You will have to append later_0s
//					unsigned int later_0 = (bitmat->subject_bytes * 8) - prev_rowbit;
//					memcpy(&comp_subfold[ROW_SIZE_BYTES + comp_subfold_size], &later_0, GAP_SIZE_BYTES);
//					comp_subfold_size += GAP_SIZE_BYTES;
//				}
				memcpy(comp_subfold, &comp_subfold_size, ROW_SIZE_BYTES);
			}
			cout << "load_data_vertically: Dumping data for the last BM " << ppos << " ";
			if (tmpdump) {
				fclose(tmpdump);
				tmpdump = NULL;
			}
			dump_out_data(fdump_fp, bitmat, (char *)config[string("TMP_STORAGE")].c_str());

//			if ( (pcnt % 1048576) == 0)
//				cout << "**** Done with BM num " << pcnt << endl;

			//Empty the earlier bitmat
			clear_rows(bitmat, true, true, true);
			comp_subfold_size = 0;
			prev_rowbit = 1;
			fclose(fdump_fp);
			fclose(fp_table);
		}

	}

	fclose(fp);

	if (ondisk) {

		cout << "***Total triples in all bitmats " << total_triples <<  endl;
//		cout << "*** Now writing out the table" << endl;
//
//		char tablefile[1024];
//		sprintf(tablefile, "%s_table", fname_dump);
//
//		FILE *fp = fopen(tablefile, "wb");
//		if (fp == NULL) {
//			cout << "*** ERROR: Could not open tablefile " << tablefile << endl;
//			exit (-1);
//		}
//
//		for (unsigned int i = 0; i < bitmat->num_preds; i++) {
//			fwrite(table[i], table_col_bytes, 1, fp);
//			free(table[i]);
//		}

		if (comp_subfold != NULL)
			free(comp_subfold);
		comp_subfold = NULL;
		comp_subfold_size = 0;
//		fclose(fp);
//		free(table);
		if (bitmat->subfold != NULL) {
			free(bitmat->subfold);
			bitmat->subfold = NULL;
		}
		if (bitmat->objfold != NULL) {
			free(bitmat->objfold);
			bitmat->objfold = NULL;
		}
	}

	if (grow != NULL) {
		free(grow);
		grow = NULL;
	}
}

///////////////////////////////////////////////////////////

unsigned int wrapper_load_from_dump_file(char *fname_dump, unsigned int bmnum, struct node *gnode,
		bool readtcnt, bool readarray)
{
//	if(tp->bitmat.dimension == POS_BITMAT || tp->bitmat.dimension == PSO_BITMAT) {
//		return load_from_dump_file(fname_dump, offset, &tp->bitmat, readtcnt, readarray, NULL, 0);
//	}

	BitMat *bitmat = &((TP *)gnode->data)->bitmat;
	int fd = 0;
	unsigned int size = 0;
	unsigned int total_size = 0;
	char *fpos = NULL;

#if MMAPFILES
	fpos = mmap_table[string(fname_dump)];
	fpos += get_offset(fname_dump, bmnum);
#else
	fd = open(fname_dump, O_RDONLY | O_LARGEFILE);
	if (fd < 0) {
		printf("*** ERROR opening dump file %s\n", fname_dump);
		assert(0);
	}
	unsigned long offset = get_offset(fname_dump, bmnum);
	if (offset > 0) {
		lseek(fd, offset, SEEK_CUR);
	}
#endif

	assert((fd == 0) ^ (NULL == fpos));

	if (readtcnt) {
		bitmat->num_triples = 0;
#if MMAPFILES
		memcpy(&bitmat->num_triples, fpos, sizeof(unsigned int));
		fpos += sizeof(unsigned int);
#else
		read(fd, &bitmat->num_triples, sizeof(unsigned int));
#endif
//		total_size += sizeof(unsigned int);
		if (bitmat->num_triples == 0) {
			cout << "wrapper_load_from_dump_file: 0 results" << endl;
			return bitmat->num_triples;
		}
	}
	if (readarray) {
		if (bitmat->subfold == NULL) {
			bitmat->subfold = (unsigned char *) malloc (bitmat->subject_bytes * sizeof (unsigned char));
			memset(bitmat->subfold, 0, bitmat->subject_bytes * sizeof (unsigned char));

		}
		if (bitmat->objfold == NULL) {
			bitmat->objfold = (unsigned char *) malloc (bitmat->object_bytes * sizeof (unsigned char));
			memset(bitmat->objfold, 0, bitmat->object_bytes * sizeof (unsigned char));
		}

		if (bitmat->dimension == PSO_BITMAT || bitmat->dimension == POS_BITMAT || comp_folded_arr) {
			//first read size of the compressed array
			unsigned int comp_arr_size = 0;
#if MMAPFILES
			memcpy(&comp_arr_size, fpos, ROW_SIZE_BYTES);
			fpos += ROW_SIZE_BYTES;
#else
			read(fd, &comp_arr_size, ROW_SIZE_BYTES);
#endif
//			total_size += ROW_SIZE_BYTES;
			unsigned char *comp_arr = (unsigned char *) malloc (comp_arr_size * sizeof(unsigned char));
#if MMAPFILES
			memcpy(comp_arr, fpos, comp_arr_size);
			fpos += comp_arr_size;
#else
			read(fd, comp_arr, comp_arr_size);
#endif
			dgap_uncompress(comp_arr, comp_arr_size, bitmat->subfold, bitmat->subject_bytes);
//			total_size += comp_arr_size;

			free(comp_arr);

			if (bitmat->dimension != PSO_BITMAT && bitmat->dimension != POS_BITMAT) {
				comp_arr_size = 0;
#if MMAPFILES
				memcpy(&comp_arr_size, fpos, ROW_SIZE_BYTES);
				fpos += ROW_SIZE_BYTES;
#else
				read(fd, &comp_arr_size, ROW_SIZE_BYTES);
#endif
//				total_size += ROW_SIZE_BYTES;

				comp_arr = (unsigned char *) malloc (comp_arr_size * sizeof(unsigned char));
#if MMAPFILES
				memcpy(comp_arr, fpos, comp_arr_size);
				fpos += comp_arr_size;
#else
				read(fd, comp_arr, comp_arr_size);
#endif
				dgap_uncompress(comp_arr, comp_arr_size, bitmat->objfold, bitmat->object_bytes);
	//			total_size += comp_arr_size;

				free(comp_arr);
			}

		} else {
#if MMAPFILES
			memcpy(bitmat->subfold, fpos, bitmat->subject_bytes);
			fpos += bitmat->subject_bytes;
			memcpy(bitmat->objfold, fpos, bitmat->object_bytes);
			fpos += bitmat->object_bytes;
#else
			read(fd, bitmat->subfold, bitmat->subject_bytes);
			read(fd, bitmat->objfold, bitmat->object_bytes);
#endif
		}

	}

//#if MMAPFILES
//#else
//	close(fd);
//#endif

//	cout << "Num objects here " << count_bits_in_row(bitmat->objfold, bitmat->object_bytes) << " Num subjects here " << count_bits_in_row(bitmat->subfold, bitmat->subject_bytes) << endl;

//	cout << "-----------------" << endl;
//	print_set_bits_in_row(bitmat->objfold, bitmat->object_bytes);
//	cout << "-----------------" << endl;

//	char dumpfile[1024];
//	strcpy(dumpfile, fname_dump);

//	unsigned int rows = count_bits_in_row(bitmat->subfold, bitmat->subject_bytes);
//	unsigned int columns = count_bits_in_row(bitmat->objfold, bitmat->object_bytes);
//	unsigned char *maskarr = NULL;
//	unsigned int maskarr_size = 0;
	TP *tp = (TP *)gnode->data;
	LIST *next = gnode->nextTP;
	unsigned char *rowarr = NULL;
	int rowarr_dim = 0, rowarr_pos = 0, rowarr_jvar = 0;
	unsigned int rowarr_size = 0, limit_bytes = 0;

	//TODO: Needs a fix for dimensions of bitmat, e.g. LUBM query9
	for (; next; next=next->next) {
		TP *other = (TP *)next->gnode->data;
		if (other->bitmat.bm.size() == 0)
			continue;

		if (!tp->isSlave(other->strlevel) && other->isSlave(tp->strlevel))
			continue;
		//S-S join
		if (tp->sub < 0 && other->sub == tp->sub) {
			if (other->bitmat.dimension == SPO_BITMAT && tp->bitmat.dimension == SPO_BITMAT) {
//				cout << "------taking bindings from other " << other->toString() << endl;
				if (rowarr == NULL) {
					rowarr = (unsigned char *) malloc (tp->bitmat.subject_bytes);
					rowarr_size = tp->bitmat.subject_bytes;
					rowarr_dim = ROW;
					rowarr_pos = SUB_DIMENSION;
					rowarr_jvar = tp->sub;
					for (unsigned int i = 0; i < tp->bitmat.subject_bytes; i++) {
						rowarr[i] = tp->bitmat.subfold[i] & other->bitmat.subfold[i];
					}
				} else if (rowarr_jvar == tp->sub) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					limit_bytes = rowarr_size < other->bitmat.subject_bytes ? rowarr_size : other->bitmat.subject_bytes;
					for (unsigned int i = 0; i < limit_bytes; i++) {
						rowarr[i] = rowarr[i] & other->bitmat.subfold[i];
					}
				}
			} else if ((other->bitmat.dimension == POS_BITMAT || other->bitmat.dimension == OPS_BITMAT)
							&& tp->bitmat.dimension == SPO_BITMAT) {
				if (rowarr == NULL) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					rowarr = (unsigned char *) malloc (tp->bitmat.subject_bytes);
					rowarr_size = tp->bitmat.subject_bytes;
					assert(other->bitmat.object_bytes == tp->bitmat.subject_bytes);
					rowarr_dim = ROW;
					rowarr_pos = SUB_DIMENSION;
					rowarr_jvar = tp->sub;
					for (unsigned int i = 0; i < tp->bitmat.subject_bytes; i++) {
						rowarr[i] = tp->bitmat.subfold[i] & other->bitmat.objfold[i];
					}
				} else if (rowarr_jvar == tp->sub) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					limit_bytes = rowarr_size < other->bitmat.object_bytes ? rowarr_size : other->bitmat.object_bytes;
					for (unsigned int i = 0; i < limit_bytes; i++) {
						rowarr[i] = rowarr[i] & other->bitmat.objfold[i];
					}
				}
			} else if (other->bitmat.dimension == SPO_BITMAT && tp->bitmat.dimension == POS_BITMAT) {
				if (rowarr == NULL) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					rowarr = (unsigned char *) malloc (tp->bitmat.object_bytes);
					rowarr_size = tp->bitmat.object_bytes;
					assert(other->bitmat.subject_bytes == tp->bitmat.object_bytes);
					rowarr_dim = COLUMN;
					rowarr_pos = SUB_DIMENSION;
					rowarr_jvar = tp->sub;
					for (unsigned int i = 0; i < tp->bitmat.object_bytes; i++) {
						rowarr[i] = tp->bitmat.objfold[i] & other->bitmat.subfold[i];
					}
				} else if (rowarr_jvar == tp->sub) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					limit_bytes = rowarr_size < other->bitmat.subject_bytes ? rowarr_size : other->bitmat.subject_bytes;
					for (unsigned int i = 0; i < limit_bytes; i++) {
						rowarr[i] = rowarr[i] & other->bitmat.subfold[i];
					}
				}
			} else if ((other->bitmat.dimension == POS_BITMAT || other->bitmat.dimension == OPS_BITMAT)
							&& tp->bitmat.dimension == POS_BITMAT) {
				if (rowarr == NULL) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					rowarr = (unsigned char *) malloc (tp->bitmat.object_bytes);
					rowarr_size = tp->bitmat.object_bytes;
					assert(other->bitmat.object_bytes == tp->bitmat.object_bytes);
					rowarr_dim = COLUMN;
					rowarr_pos = SUB_DIMENSION;
					rowarr_jvar = tp->sub;
					for (unsigned int i = 0; i < tp->bitmat.object_bytes; i++) {
						rowarr[i] = tp->bitmat.objfold[i] & other->bitmat.objfold[i];
					}
				} else if (rowarr_jvar == tp->sub) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					limit_bytes = rowarr_size < other->bitmat.object_bytes ? rowarr_size : other->bitmat.object_bytes;
					for (unsigned int i = 0; i < limit_bytes; i++) {
						rowarr[i] = rowarr[i] & other->bitmat.objfold[i];
					}
				}
			}
		} else if (tp->pred < 0 && other->pred == tp->pred) {
			//P-P join
			if ((other->bitmat.dimension == POS_BITMAT || other->bitmat.dimension == PSO_BITMAT) &&
				(tp->bitmat.dimension == POS_BITMAT || tp->bitmat.dimension == PSO_BITMAT)) {
				if (rowarr == NULL) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					rowarr = (unsigned char *) malloc (tp->bitmat.subject_bytes);
					rowarr_size = tp->bitmat.subject_bytes;
					rowarr_dim = ROW;
					rowarr_pos = PRED_DIMENSION;
					rowarr_jvar = tp->pred;
					for (unsigned int i = 0; i < tp->bitmat.subject_bytes; i++) {
						rowarr[i] = tp->bitmat.subfold[i] & other->bitmat.subfold[i];
					}
				} else if (rowarr_jvar == tp->pred) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					assert(rowarr_size == other->bitmat.subject_bytes);
					for (unsigned int i = 0; i < other->bitmat.subject_bytes; i++) {
						rowarr[i] = rowarr[i] & other->bitmat.subfold[i];
					}
				}
			} else {
				assert(0);
			}
		} else if (tp->obj < 0 && other->obj == tp->obj) {
			//O-O join
			if (other->bitmat.dimension == OPS_BITMAT && tp->bitmat.dimension == OPS_BITMAT) {
				if (rowarr == NULL) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					rowarr = (unsigned char *) malloc (tp->bitmat.subject_bytes);
					rowarr_size = tp->bitmat.subject_bytes;
					assert(other->bitmat.subject_bytes == tp->bitmat.subject_bytes);
					rowarr_dim = ROW;
					rowarr_pos = OBJ_DIMENSION;
					rowarr_jvar = tp->obj;
					for (unsigned int i = 0; i < tp->bitmat.subject_bytes; i++) {
						rowarr[i] = tp->bitmat.subfold[i] & other->bitmat.subfold[i];
					}
				} else if (rowarr_jvar == tp->obj) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					limit_bytes = rowarr_size < other->bitmat.subject_bytes ? rowarr_size : other->bitmat.subject_bytes;
					for (unsigned int i = 0; i < limit_bytes; i++) {
						rowarr[i] = rowarr[i] & other->bitmat.subfold[i];
					}
				}
			} else if ((other->bitmat.dimension == PSO_BITMAT || other->bitmat.dimension == SPO_BITMAT) && tp->bitmat.dimension == OPS_BITMAT) {
				if (rowarr == NULL) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					rowarr = (unsigned char *) malloc (tp->bitmat.subject_bytes);
					rowarr_size = tp->bitmat.subject_bytes;
					assert(other->bitmat.object_bytes == tp->bitmat.subject_bytes);
					rowarr_dim = ROW;
					rowarr_pos = OBJ_DIMENSION;
					rowarr_jvar = tp->obj;
					for (unsigned int i = 0; i < tp->bitmat.subject_bytes; i++) {
						rowarr[i] = tp->bitmat.subfold[i] & other->bitmat.objfold[i];
					}
				} else if (rowarr_jvar == tp->obj) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					limit_bytes = rowarr_size < other->bitmat.object_bytes ? rowarr_size : other->bitmat.object_bytes;
					for (unsigned int i = 0; i < limit_bytes; i++) {
						rowarr[i] = rowarr[i] & other->bitmat.objfold[i];
					}
				}
			} else if (other->bitmat.dimension == OPS_BITMAT && tp->bitmat.dimension == PSO_BITMAT) {
				if (rowarr == NULL) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					rowarr = (unsigned char *) malloc (tp->bitmat.object_bytes);
					rowarr_size = tp->bitmat.object_bytes;
					assert(other->bitmat.subject_bytes == tp->bitmat.object_bytes);
					rowarr_dim = COLUMN;
					rowarr_pos = OBJ_DIMENSION;
					rowarr_jvar = tp->obj;
					for (unsigned int i = 0; i < other->bitmat.subject_bytes; i++) {
						rowarr[i] = tp->bitmat.objfold[i] & other->bitmat.subfold[i];
					}
				} else if (rowarr_jvar == tp->obj) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					limit_bytes = rowarr_size < other->bitmat.subject_bytes ? rowarr_size : other->bitmat.subject_bytes;
					for (unsigned int i = 0; i < limit_bytes; i++) {
						rowarr[i] = rowarr[i] & other->bitmat.subfold[i];
					}
				}
			} else if ((other->bitmat.dimension == PSO_BITMAT || other->bitmat.dimension == SPO_BITMAT)
							&& tp->bitmat.dimension == PSO_BITMAT) {
				if (rowarr == NULL) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					rowarr = (unsigned char *) malloc (tp->bitmat.object_bytes);
					rowarr_size = tp->bitmat.object_bytes;
					assert(other->bitmat.object_bytes == tp->bitmat.object_bytes);
					rowarr_dim = COLUMN;
					rowarr_pos = OBJ_DIMENSION;
					rowarr_jvar = tp->obj;
					for (unsigned int i = 0; i < tp->bitmat.object_bytes; i++) {
						rowarr[i] = tp->bitmat.objfold[i] & other->bitmat.objfold[i];
					}
				} else if (rowarr_jvar == tp->obj) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					limit_bytes = rowarr_size < other->bitmat.object_bytes ? rowarr_size : other->bitmat.object_bytes;
					for (unsigned int i = 0; i < limit_bytes; i++) {
						rowarr[i] = rowarr[i] & other->bitmat.objfold[i];
					}
				}
			}
		} else if (tp->sub < 0 && other->obj == tp->sub) {
			//S-O join1
			if (tp->bitmat.dimension != SPO_BITMAT && tp->bitmat.dimension != POS_BITMAT)
				continue;
			bool rowarr_newly_set = false;
			if (rowarr == NULL) {
				rowarr = (unsigned char *) malloc (tp->bitmat.common_so_bytes);
				rowarr_size = tp->bitmat.common_so_bytes;
				rowarr_pos = SO_DIMENSION;
				rowarr_jvar = tp->sub;
				switch (tp->bitmat.dimension) {
				case (SPO_BITMAT):
					rowarr_dim = ROW;
					break;
				case (POS_BITMAT):
					rowarr_dim = COLUMN;
					break;
				default:
					assert(0);
					break;
				}
				rowarr_newly_set = true;
			} else if (rowarr_jvar == tp->sub) {
				if (rowarr_pos != SO_DIMENSION) {
					assert(rowarr_size >= tp->bitmat.common_so_bytes);
					memset(&rowarr[tp->bitmat.common_so_bytes], 0, rowarr_size - tp->bitmat.common_so_bytes);
					rowarr[tp->bitmat.common_so_bytes-1] &= (0xff << (8-(tp->bitmat.num_comm_so%8)));
					rowarr_pos = SO_DIMENSION;
					rowarr_size = tp->bitmat.common_so_bytes;
				} else {
					assert(rowarr_size == tp->bitmat.common_so_bytes);
				}
			}

			if (other->bitmat.dimension == OPS_BITMAT && tp->bitmat.dimension == SPO_BITMAT) {
				if (rowarr_newly_set) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					for (unsigned int i = 0; i < tp->bitmat.common_so_bytes; i++) {
						rowarr[i] = tp->bitmat.subfold[i] & other->bitmat.subfold[i];
					}
				} else if (rowarr_jvar == tp->sub) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					for (unsigned int i = 0; i < tp->bitmat.common_so_bytes; i++) {
						rowarr[i] = rowarr[i] & other->bitmat.subfold[i];
					}
				}
			} else if ((other->bitmat.dimension == PSO_BITMAT || other->bitmat.dimension == SPO_BITMAT)
						&& tp->bitmat.dimension == SPO_BITMAT) {
				if (rowarr_newly_set) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					for (unsigned int i = 0; i < tp->bitmat.common_so_bytes; i++) {
						rowarr[i] = tp->bitmat.subfold[i] & other->bitmat.objfold[i];
					}
				} else if (rowarr_jvar == tp->sub) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					for (unsigned int i = 0; i < tp->bitmat.common_so_bytes; i++) {
						rowarr[i] = rowarr[i] & other->bitmat.objfold[i];
					}
				}
			} else if (other->bitmat.dimension == OPS_BITMAT && tp->bitmat.dimension == POS_BITMAT) {
				if (rowarr_newly_set) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					for (unsigned int i = 0; i < tp->bitmat.common_so_bytes; i++) {
						rowarr[i] = tp->bitmat.objfold[i] & other->bitmat.subfold[i];
					}
				} else if (rowarr_jvar == tp->sub) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					for (unsigned int i = 0; i < tp->bitmat.common_so_bytes; i++) {
						rowarr[i] = rowarr[i] & other->bitmat.subfold[i];
					}
				}
			} else if ((other->bitmat.dimension == PSO_BITMAT || other->bitmat.dimension == SPO_BITMAT)
							&& tp->bitmat.dimension == POS_BITMAT) {
				if (rowarr_newly_set) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					for (unsigned int i = 0; i < tp->bitmat.common_so_bytes; i++) {
						rowarr[i] = tp->bitmat.objfold[i] & other->bitmat.objfold[i];
					}
				} else if (rowarr_jvar == tp->sub) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					for (unsigned int i = 0; i < tp->bitmat.common_so_bytes; i++) {
						rowarr[i] = rowarr[i] & other->bitmat.objfold[i];
					}
				}
			}
			if (rowarr_jvar == tp->sub) {
				rowarr[bitmat->common_so_bytes-1] &= (0xff << (8-(bitmat->num_comm_so%8)));
			}

		} else if (tp->obj < 0 && other->sub == tp->obj) {
			//S-O join2
			if (tp->bitmat.dimension != OPS_BITMAT && tp->bitmat.dimension != PSO_BITMAT)
				continue;
			bool rowarr_newly_set = false;
			if (rowarr == NULL) {
				rowarr = (unsigned char *) malloc (tp->bitmat.common_so_bytes);
				rowarr_size = tp->bitmat.common_so_bytes;
				rowarr_pos = SO_DIMENSION;
				rowarr_jvar = tp->obj;
				switch (tp->bitmat.dimension) {
				case (OPS_BITMAT):
					rowarr_dim = ROW;
					break;
				case (PSO_BITMAT):
					rowarr_dim = COLUMN;
					break;
				default:
//					cout << "tp->bitmat.dim " << tp->bitmat.dimension << endl;
//					continue;
					assert(0);
					break;
				}
				rowarr_newly_set = true;
			} else if (rowarr_jvar == tp->obj) {
				if (rowarr_pos != SO_DIMENSION) {
					assert(rowarr_size >= tp->bitmat.common_so_bytes);
					memset(&rowarr[tp->bitmat.common_so_bytes], 0, rowarr_size - tp->bitmat.common_so_bytes);
					rowarr[tp->bitmat.common_so_bytes-1] &= (0xff << (8-(tp->bitmat.num_comm_so%8)));
					rowarr_pos = SO_DIMENSION;
					rowarr_size = tp->bitmat.common_so_bytes;
				} else {
					assert(rowarr_size == tp->bitmat.common_so_bytes);
				}
			}

			if (other->bitmat.dimension == SPO_BITMAT && tp->bitmat.dimension == OPS_BITMAT) {
//				rowarr_dim = ROW;
				if (rowarr_newly_set) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					for (unsigned int i = 0; i < tp->bitmat.common_so_bytes; i++) {
						rowarr[i] = tp->bitmat.subfold[i] & other->bitmat.subfold[i];
					}
				} else if (rowarr_jvar == tp->obj) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					for (unsigned int i = 0; i < tp->bitmat.common_so_bytes; i++) {
						rowarr[i] = rowarr[i] & other->bitmat.subfold[i];
					}
				}
			} else if ((other->bitmat.dimension == POS_BITMAT || other->bitmat.dimension == OPS_BITMAT)
						&& tp->bitmat.dimension == OPS_BITMAT) {
//				rowarr_dim = ROW;
				if (rowarr_newly_set) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					for (unsigned int i = 0; i < tp->bitmat.common_so_bytes; i++) {
						rowarr[i] = tp->bitmat.subfold[i] & other->bitmat.objfold[i];
					}
				} else if (rowarr_jvar == tp->obj) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					for (unsigned int i = 0; i < tp->bitmat.common_so_bytes; i++) {
						rowarr[i] = rowarr[i] & other->bitmat.objfold[i];
					}
				}
			} else if (other->bitmat.dimension == SPO_BITMAT && tp->bitmat.dimension == PSO_BITMAT) {
//				rowarr_dim = COLUMN;
				if (rowarr_newly_set) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					for (unsigned int i = 0; i < tp->bitmat.common_so_bytes; i++) {
						rowarr[i] = tp->bitmat.objfold[i] & other->bitmat.subfold[i];
					}
				} else if (rowarr_jvar == tp->obj) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					for (unsigned int i = 0; i < tp->bitmat.common_so_bytes; i++) {
						rowarr[i] = rowarr[i] & other->bitmat.subfold[i];
					}
				}
			} else if ((other->bitmat.dimension == POS_BITMAT || other->bitmat.dimension == OPS_BITMAT)
							&& tp->bitmat.dimension == PSO_BITMAT) {
//				rowarr_dim = COLUMN;
				if (rowarr_newly_set) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					for (unsigned int i = 0; i < tp->bitmat.common_so_bytes; i++) {
						rowarr[i] = tp->bitmat.objfold[i] & other->bitmat.objfold[i];
					}
				} else if (rowarr_jvar == tp->obj) {
//					cout << "------taking bindings from other " << other->toString() << endl;
					for (unsigned int i = 0; i < tp->bitmat.common_so_bytes; i++) {
						rowarr[i] = rowarr[i] & other->bitmat.objfold[i];
					}
				}
			}
			if (rowarr_jvar == tp->obj) {
				rowarr[bitmat->common_so_bytes-1] &= (0xff << (8-(bitmat->num_comm_so%8)));
			}
		}
	}

	///////////
	//DEBUG
	//TODO: remove later
	//////////
//	cout << "--------Wrapper_load_from_dump_file: " << tp->toString() << " bitmat.dim " <<
//		tp->bitmat.dimension << " rowarr is " << (rowarr == NULL ? "NULL " : "NOT NULL ") << " rowarr_size "
//		<< rowarr_size << endl;
//	unsigned int setbits = print_set_bits_in_row(rowarr, rowarr_size);
//	cout << "--------setbits in maskarr/rowarr " << count_bits_in_row(rowarr, rowarr_size) << endl;
	///////////////////////////////////

	unsigned int ret = load_from_dump_file(fname_dump, bmnum, bitmat, false, false, rowarr,
								rowarr_size, rowarr_dim, fpos, fd, true);

//	cout << "--------triples in bitmat " << count_triples_in_bitmat(bitmat) << endl;

	free(rowarr);

	return ret;
}

unsigned int last_set_bit(unsigned char *in, unsigned int size)
{
	if (size == 0)
		return 0;

	unsigned int last_set_bit = 0;

	for (unsigned int i = 0; i < size; i++) {
		if (in[i] == 0xff) {
			last_set_bit = (i+1)*8;
		} else if (in[i] > 0x00) {
			for (unsigned short j = 0; j < 8; j++) {
				if (in[i] & (0x80 >> j)) {
					last_set_bit = (i*8)+j+1;
				}
			}
		}
	}

	return last_set_bit;

}

///////////////////////////////////////////////////////////
unsigned int load_from_dump_file(char *fname_dump, unsigned int bmnum, BitMat *bitmat, bool readtcnt,
			bool readarray, unsigned char *maskarr, unsigned int maskarr_size, int maskarr_dim,
			char *fpos, int fd, bool fold_objdim)
{
//	struct timeval start_time, stop_time;
//	clock_t  t_start, t_end;
//	double curr_time;
//	double st, en;
//	gettimeofday(&start_time, (struct timezone *)0);
//	cout << "Inside load_from_dump_file" << endl;
//	int fd = 0;
	unsigned int size = 0;
//	unsigned int total_size = 0;
//	char *fpos = NULL;

	assert((readtcnt && readarray) || (!readtcnt && !readarray && ((fd == 0) ^ (NULL == fpos))));

#if MMAPFILES
	if (fpos == NULL) {
		fpos = mmap_table[string(fname_dump)];
		fpos += get_offset(fname_dump, bmnum);
	}
#else
	if (fd == 0) {
		fd = open(fname_dump, O_RDONLY | O_LARGEFILE);
		if (fd < 0) {
			printf("*** ERROR opening dump file %s\n", fname_dump);
			assert(0);
		}
		unsigned long offset = get_offset(fname_dump, bmnum);
		///////
		//DEBUG: remove later
		////////
//		cout << "load_from_dump_file: offset is " << offset << endl;
		if (offset > 0) {
			lseek(fd, offset, SEEK_CUR);
		}
	}
#endif

	assert((fd == 0) ^ (NULL == fpos));
//	cout << "load_from_dump_file: offset " << offset << endl;

	if (readtcnt) {
		bitmat->num_triples = 0;
#if MMAPFILES
		memcpy(&bitmat->num_triples, fpos, sizeof(unsigned int));
		fpos += sizeof(unsigned int);
#else
		read(fd, &bitmat->num_triples, sizeof(unsigned int));
#endif
//		total_size += sizeof(unsigned int);
		if (bitmat->num_triples == 0) {
			cout << "load_from_dump_file: 0 results" << endl;
			return bitmat->num_triples;
		}
	}
//	cout << "Num triples1 " << bitmat->num_triples << endl;
	if (readarray) {
		if (bitmat->subfold == NULL) {
			bitmat->subfold = (unsigned char *) malloc (bitmat->subject_bytes * sizeof (unsigned char));
			memset(bitmat->subfold, 0, bitmat->subject_bytes * sizeof (unsigned char));

		}
		if (bitmat->objfold == NULL) {
			bitmat->objfold = (unsigned char *) malloc (bitmat->object_bytes * sizeof (unsigned char));
			memset(bitmat->objfold, 0, bitmat->object_bytes * sizeof (unsigned char));
		}

		if (bitmat->dimension == PSO_BITMAT || bitmat->dimension == POS_BITMAT || comp_folded_arr) {
			//first read size of the compressed array
			unsigned int comp_arr_size = 0;
#if MMAPFILES
			memcpy(&comp_arr_size, fpos, ROW_SIZE_BYTES);
			fpos += ROW_SIZE_BYTES;
#else
			read(fd, &comp_arr_size, ROW_SIZE_BYTES);
#endif
//			total_size += ROW_SIZE_BYTES;
			unsigned char *comp_arr = (unsigned char *) malloc (comp_arr_size * sizeof(unsigned char));
#if MMAPFILES
			memcpy(comp_arr, fpos, comp_arr_size);
			fpos += comp_arr_size;
#else
			read(fd, comp_arr, comp_arr_size);
#endif
			dgap_uncompress(comp_arr, comp_arr_size, bitmat->subfold, bitmat->subject_bytes);
//			total_size += comp_arr_size;

			free(comp_arr);

			if (bitmat->dimension != PSO_BITMAT && bitmat->dimension != POS_BITMAT) {
				comp_arr_size = 0;
#if MMAPFILES
				memcpy(&comp_arr_size, fpos, ROW_SIZE_BYTES);
				fpos += ROW_SIZE_BYTES;
#else
				read(fd, &comp_arr_size, ROW_SIZE_BYTES);
#endif
//				total_size += ROW_SIZE_BYTES;
		
				comp_arr = (unsigned char *) malloc (comp_arr_size * sizeof(unsigned char));
#if MMAPFILES
				memcpy(comp_arr, fpos, comp_arr_size);
				fpos += comp_arr_size;
#else
				read(fd, comp_arr, comp_arr_size);
#endif
				dgap_uncompress(comp_arr, comp_arr_size, bitmat->objfold, bitmat->object_bytes);
	//			total_size += comp_arr_size;

				free(comp_arr);
			}

		} else {
#if MMAPFILES
			memcpy(bitmat->subfold, fpos, bitmat->subject_bytes);
			fpos += bitmat->subject_bytes;
			memcpy(bitmat->objfold, fpos, bitmat->object_bytes);
			fpos += bitmat->object_bytes;
#else
			read(fd, bitmat->subfold, bitmat->subject_bytes);
			read(fd, bitmat->objfold, bitmat->object_bytes);
#endif
//			total_size += bitmat->subject_bytes;
//			total_size += bitmat->object_bytes;
		}
	}
//	cout << "Num bits in subfold " << count_bits_in_row(bitmat->subfold, bitmat->subject_bytes) << endl;
//	cout << "Num bits in objfold " << count_bits_in_row(bitmat->objfold, bitmat->object_bytes) << endl;
//	print_set_bits_in_row(bitmat->objfold, bitmat->object_bytes);
	unsigned int limit_bytes = 0;
	if (maskarr_size == 0) {
		assert(maskarr == NULL);
		limit_bytes = bitmat->subject_bytes;
	}

	bool skipAtLeastOnce = false, skip = false;
	unsigned int last_bit = last_set_bit(maskarr, maskarr_size);
	unsigned int total_set_bits = count_bits_in_row(maskarr, maskarr_size);
	unsigned int subfold_bits = count_bits_in_row(bitmat->subfold, bitmat->subject_bytes);
	/////////
	//DEBUG
	//TODO: remove later
//	cout << "--------- load_from_dump_file: maskarr_size " << maskarr_size << " maskarr_dim " << maskarr_dim << " last_setbit " << last_bit
//		<< " total_set_bits " << total_set_bits << " subfold_bits " << subfold_bits << endl;
	/////////////////////////////
	maskarr_size = last_bit/8 + ((last_bit%8) > 0 ? 1 : 0);
	if (maskarr_dim == ROW) {
		limit_bytes = bitmat->subject_bytes > maskarr_size ? maskarr_size : bitmat->subject_bytes;
	} else {
		limit_bytes = bitmat->subject_bytes;
	}

	unsigned int rownum = 1;
	//////////
	//DEBUG
//	off64_t total_size = 0;

	if (maskarr == NULL || maskarr_dim == COLUMN) {
		for (unsigned int i = 0; i < bitmat->subject_bytes; i++) {
			if (bitmat->subfold[i] == 0x00) {
				rownum += 8;
			} else {
				for (short j = 0; j < 8; j++) {
					if (!(bitmat->subfold[i] & (0x80 >> j))) {
						rownum++;
						continue;
					}
	#if MMAPFILES
					memcpy(&size, fpos, ROW_SIZE_BYTES);
					fpos += ROW_SIZE_BYTES;
	#else
					read(fd, &size, ROW_SIZE_BYTES);
	#endif
					unsigned char *data = (unsigned char *) malloc (size + ROW_SIZE_BYTES);
					memcpy(data, &size, ROW_SIZE_BYTES);
					////////
					//DEBUG
//					total_size += (8+size+ROW_SIZE_BYTES);
//					cout << "total_size " << total_size << endl;
	#if MMAPFILES
					memcpy(data + ROW_SIZE_BYTES, fpos, size);
					fpos += size;
	#else
					read(fd, data + ROW_SIZE_BYTES, size);
	#endif
					struct row r = {rownum, data};
//					if (!vbm_load)
					bitmat->bm.push_back(r);
//					else
//						bitmat->vbm.push_back(r);

					rownum++;
				}
			}
		}

	} else if (maskarr_dim == ROW) {
		if ((total_set_bits <= ((maskarr_size * 8)/SELECTIVITY_THRESHOLD)) && subfold_bits > 10*total_set_bits &&
				(bitmat->dimension == SPO_BITMAT || bitmat->dimension == OPS_BITMAT)) {
//			cout << "--------- load_from_dump_file: NOT loading the traditional way" << endl;
			char dumpfile[1024];
			BitMat bm_tmp;
			unsigned int add_row_dim = 0;
			if (bitmat->dimension == SPO_BITMAT) {
				sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_PSO")].c_str());
				shallow_init_bitmat(&bm_tmp, gnum_preds, gnum_subs, gnum_objs, gnum_comm_so, PSO_BITMAT);
				add_row_dim = PSO_BITMAT;

			} else if (bitmat->dimension == OPS_BITMAT) {
				sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_POS")].c_str());
				shallow_init_bitmat(&bm_tmp, gnum_preds, gnum_objs, gnum_subs, gnum_comm_so, POS_BITMAT);
				add_row_dim = POS_BITMAT;
			}

			rownum = 1;
			for (unsigned int i = 0; i < limit_bytes; i++) {
				if (maskarr[i] == 0x00) {
					rownum += 8;
				} else {
					for (short j = 0; j < 8; j++) {
						if (!(maskarr[i] & (0x80 >> j))) {
							rownum++;
							continue;
						}
						add_row(&bm_tmp, dumpfile, rownum, add_row_dim, bmnum, false);
						assert(bm_tmp.bm.size() == 1);
						list<struct row>::iterator bmit = bm_tmp.bm.begin();
						unsigned int size = 0; memcpy(&size, (*bmit).data, ROW_SIZE_BYTES);
						unsigned char *data = (unsigned char *) malloc (size + ROW_SIZE_BYTES);
						memcpy(data, (*bmit).data, size+ROW_SIZE_BYTES);
						struct row r = {rownum, data};
//							if (!vbm_load)
						bitmat->bm.push_back(r);
//							else
//								bitmat->vbm.push_back(r);
						bm_tmp.reset();

						rownum++;
					}
				}
			}

			free(bitmat->subfold);
			bitmat->subfold = maskarr;
			bitmat->subject_bytes = maskarr_size;

/*
			if (bitmat->dimension == SPO_BITMAT) {
				char dumpfile[1024];
				sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_PSO")].c_str());

				BitMat bm_pso;
				shallow_init_bitmat(&bm_pso, gnum_preds, gnum_subs, gnum_objs, gnum_comm_so, PSO_BITMAT);
				rownum = 1;
				for (unsigned int i = 0; i < limit_bytes; i++) {
					if (maskarr[i] == 0x00) {
						rownum += 8;
					} else {
						for (short j = 0; j < 8; j++) {
							if (!(maskarr[i] & (0x80 >> j))) {
								rownum++;
								continue;
							}
							add_row(&bm_pso, dumpfile, rownum, PSO_BITMAT, bmnum, false);
							assert(bm_pso.bm.size() == 1);
							list<struct row>::iterator bmit = bm_pso.bm.begin();
							unsigned int size = 0; memcpy(&size, (*bmit).data, ROW_SIZE_BYTES);
							unsigned char *data = (unsigned char *) malloc (size + ROW_SIZE_BYTES);
							memcpy(data, (*bmit).data, size+ROW_SIZE_BYTES);
							struct row r = {rownum, data};
//							if (!vbm_load)
							bitmat->bm.push_back(r);
//							else
//								bitmat->vbm.push_back(r);
							bm_pso.reset();

							rownum++;
						}
					}
				}

			} else if (bitmat->dimension == OPS_BITMAT) {
				char dumpfile[1024];
				sprintf(dumpfile, "%s", (char *)config[string("BITMATDUMPFILE_POS")].c_str());

				BitMat bm_pos;
				shallow_init_bitmat(&bm_pos, gnum_preds, gnum_objs, gnum_subs, gnum_comm_so, POS_BITMAT);
				rownum = 1;
				for (unsigned int i = 0; i < limit_bytes; i++) {
					if (maskarr[i] == 0x00) {
						rownum += 8;
					} else {
						for (short j = 0; j < 8; j++) {
							if (!(maskarr[i] & (0x80 >> j))) {
								rownum++;
								continue;
							}
							add_row(&bm_pos, dumpfile, rownum, POS_BITMAT, bmnum, false);
							assert(bm_pos.bm.size() == 1);
							list<struct row>::iterator bmit = bm_pos.bm.begin();
							unsigned int size = 0; memcpy(&size, (*bmit).data, ROW_SIZE_BYTES);
							unsigned char *data = (unsigned char *) malloc (size + ROW_SIZE_BYTES);
							memcpy(data, (*bmit).data, size+ROW_SIZE_BYTES);
							struct row r = {rownum, data};
//							if (!vbm_load)
							bitmat->bm.push_back(r);
//							else
//								bitmat->vbm.push_back(r);
							bm_pos.reset();

							rownum++;
						}
					}
				}

			}
*/
			simple_fold(bitmat, ROW, bitmat->subfold, bitmat->subject_bytes);
			simple_fold(bitmat, COLUMN, bitmat->objfold, bitmat->object_bytes);
			count_triples_in_bitmat(bitmat);

			return bitmat->num_triples;

		}
		for (unsigned int i = 0; i < limit_bytes; i++) {
			if (bitmat->subfold[i] == 0x00) {
				rownum += 8;
			} else {
				for (short j = 0; j < 8; j++) {
					if (!(bitmat->subfold[i] & (0x80 >> j))) {
						rownum++;
						continue;
					}
					skip = false;
	#if MMAPFILES
					memcpy(&size, fpos, ROW_SIZE_BYTES);
					fpos += ROW_SIZE_BYTES;
	#else
					read(fd, &size, ROW_SIZE_BYTES);
	#endif
					if ((maskarr[i] & (0x80 >> j)) == 0x00) {
						skip = true;
//						skip_cnt++;
						bitmat->subfold[i] &= ~(0x80 >> j);
						skipAtLeastOnce = true;
					}
					unsigned char *data = NULL;
					if (!skip) {
						data = (unsigned char *) malloc (size + ROW_SIZE_BYTES);
						memcpy(data, &size, ROW_SIZE_BYTES);
					}
	#if MMAPFILES
					if (!skip) {
						memcpy(data + ROW_SIZE_BYTES, fpos, size);
					}
					fpos += size;
	#else
					if (!skip) {
						read(fd, data + ROW_SIZE_BYTES, size);
					} else {
						lseek(fd, size, SEEK_CUR);
					}
	#endif
					if (!skip) {
						struct row r = {rownum, data};
//						if (!vbm_load)
						bitmat->bm.push_back(r);
//						else
//							bitmat->vbm.push_back(r);
					}
					rownum++;
				}
			}
		}
		free(bitmat->subfold);
		bitmat->subfold = maskarr;
		bitmat->subject_bytes = maskarr_size;

//		cout << "load_from_dump_file: skip_cnt " << skip_cnt << endl;
	}

	if (maskarr_dim == COLUMN) {
		//modified simple_unfold dynamically updates triple count
		//in bitmat while unfolding on COLUMN dim.
//		cout << "load_from_dump_file: maskarr_dim COLUMN" << endl;
		simple_unfold(bitmat, maskarr, maskarr_size, COLUMN, 3);
		simple_fold(bitmat, ROW, bitmat->subfold, bitmat->subject_bytes);
//		cout << "bitmat->num_triples " << bitmat->num_triples << endl;
	}

//	gettimeofday(&stop_time, (struct timezone *)0);
//	st = start_time.tv_sec + (start_time.tv_usec/MICROSEC);
//	en = stop_time.tv_sec + (stop_time.tv_usec/MICROSEC);
//	curr_time = en-st;
//
//	printf("Time for load_from_dump_file(gettimeofday)1: %f\n", curr_time);

	if (skipAtLeastOnce) {
//		for (unsigned int i = 0; i < limit_bytes; i++) {
//			bitmat->subfold[i] = bitmat->subfold[i] & maskarr[i];
//		}
		memset(&bitmat->subfold[limit_bytes], 0, bitmat->subject_bytes - limit_bytes);
		if (fold_objdim) {
			simple_fold(bitmat, COLUMN, bitmat->objfold, bitmat->object_bytes);
		}
		count_triples_in_bitmat(bitmat);
	}
//	printf("Total size loaded from file %u\n", total_size);

#if MMAPFILES
#else
	close(fd);
#endif

//	cout << "Exiting load_from_dump_file" << endl;
//	gettimeofday(&stop_time, (struct timezone *)0);
//	st = start_time.tv_sec + (start_time.tv_usec/MICROSEC);
//	en = stop_time.tv_sec + (stop_time.tv_usec/MICROSEC);
//	curr_time = en-st;
//
//	printf("Time for load_from_dump_file(gettimeofday): %f\n", curr_time);

//	cout << "bitmat->num_triples " << bitmat->num_triples << endl;

	return bitmat->num_triples;

//	assert(bitmat->num_triples == testtrip);
//	cout << "Num triples " << bitmat->num_triples << endl;
//	unsigned char *test = (unsigned char *) malloc (10);
//	test[0] = 0xff;
//	test[1] = 0x00;
//	test[2] = 0xff;
//	test[3] = 0x00;
//	test[4] = 0xff;
//	test[5] = 0xff;
//	cout << "Num objects " << count_bits_in_row(test, 10) << endl;
//	print_set_bits_in_row(test, 10);


//	vector<struct triple> triplist;
//	list_enctrips_bitmat_new(bitmat, 44, triplist);
//	for (vector<struct triple>::iterator itr = triplist.begin(); itr != triplist.end(); itr++) {
//		cout << (*itr).sub << ":"<< (*itr).pred << ":" << (*itr).obj << endl;
//	}

//	cout << "222 Num objects here " << count_bits_in_row(bitmat->objfold, bitmat->object_bytes) << " Num subjects here " << count_bits_in_row(bitmat->subfold, bitmat->subject_bytes) << endl;
}

////////////////////////////////////
bool filter_and_load_bitmat(BitMat *bitmat, int fd, char *fpos, unsigned char *and_array,
							unsigned int and_array_size)
{
	assert((fd == 0) ^ (NULL == fpos));

	unsigned int size = 0;
	unsigned int total_size = 0;
	unsigned int i = 0;

//	cout << "Inside filter_and_load_bitmat" << endl;

	if (bitmat->subfold == NULL) {
		bitmat->subfold = (unsigned char *) malloc (bitmat->subject_bytes * sizeof (unsigned char));
		memset(bitmat->subfold, 0, bitmat->subject_bytes * sizeof (unsigned char));

	}
	if (bitmat->objfold == NULL) {
		bitmat->objfold = (unsigned char *) malloc (bitmat->object_bytes * sizeof (unsigned char));
		memset(bitmat->objfold, 0, bitmat->object_bytes * sizeof (unsigned char));
	}

	if (bitmat->dimension == PSO_BITMAT || bitmat->dimension == POS_BITMAT || comp_folded_arr) {
		//first read size of the compressed array
		unsigned int comp_arr_size = 0;
#if MMAPFILES
		memcpy(&comp_arr_size, fpos, ROW_SIZE_BYTES);
		fpos += ROW_SIZE_BYTES;
#else
		read(fd, &comp_arr_size, ROW_SIZE_BYTES);
#endif
//		total_size += ROW_SIZE_BYTES;
		unsigned char *comp_arr = (unsigned char *) malloc (comp_arr_size * sizeof(unsigned char));
#if MMAPFILES
		memcpy(comp_arr, fpos, comp_arr_size);
#else
		read(fd, comp_arr, comp_arr_size);
#endif
		dgap_uncompress(comp_arr, comp_arr_size, bitmat->subfold, bitmat->subject_bytes);
//		total_size += comp_arr_size;

		free(comp_arr);

		if (bitmat->dimension != PSO_BITMAT && bitmat->dimension != POS_BITMAT) {
			comp_arr_size = 0;
#if MMAPFILES
			memcpy(&comp_arr_size, fpos, ROW_SIZE_BYTES);
			fpos += (ROW_SIZE_BYTES + comp_arr_size);
#else
			read(fd, &comp_arr_size, ROW_SIZE_BYTES);
			lseek(fd, comp_arr_size, SEEK_CUR);
#endif
	//		total_size += ROW_SIZE_BYTES;

			//No need of reading object array as that's going to
			//change after "filtering"
		}
//		total_size += comp_arr_size;

	} else {
#if MMAPFILES
		memcpy(bitmat->subfold, fpos, bitmat->subject_bytes);
		fpos += (bitmat->subject_bytes + bitmat->object_bytes);
#else
		read(fd, bitmat->subfold, bitmat->subject_bytes);
		lseek(fd, bitmat->object_bytes, SEEK_CUR);
#endif
//		total_size += bitmat->subject_bytes;
//		total_size += bitmat->object_bytes;
	}

//	unsigned char *andres = NULL;
//	unsigned int andres_size = 0;

	unsigned int rownum = 1;

//	cumulative_dgap(and_array, and_array_size, and_array);
//	andres = (unsigned char *) malloc (GAP_SIZE_BYTES * bitmat->num_objs + ROW_SIZE_BYTES);

	for (unsigned int i = 0; i < bitmat->subject_bytes; i++) {
		if (bitmat->subfold[i] == 0x00) {
			rownum += 8;
		} else {
			for (short j = 0; j < 8; j++) {
				if (bitmat->subfold[i] & (0x80 >> j)) {
#if MMAPFILES
					memcpy(&size, fpos, ROW_SIZE_BYTES);
					fpos += ROW_SIZE_BYTES;
#else
					read(fd, &size, ROW_SIZE_BYTES);
#endif
					unsigned char *data = (unsigned char *) malloc (size + ROW_SIZE_BYTES);
					memcpy(data, &size, ROW_SIZE_BYTES);
#if MMAPFILES
					memcpy(data + ROW_SIZE_BYTES, fpos, size);
					fpos += size;
#else
					read(fd, data + ROW_SIZE_BYTES, size);
#endif

//					cumulative_dgap(&data[ROW_SIZE_BYTES], size, &data[ROW_SIZE_BYTES]);
//					andres_size = dgap_AND(data + ROW_SIZE_BYTES, size, and_array, and_array_size, &andres[ROW_SIZE_BYTES]);
//					de_cumulative_dgap(&andres[ROW_SIZE_BYTES], andres_size, &andres[ROW_SIZE_BYTES]);

//					if ((andres_size == 1 + GAP_SIZE_BYTES) && !andres[ROW_SIZE_BYTES]) {
//						//AND result is all 0s
//						free(data);
//						//Unset bit in subfold
//						bitmat->subfold[i/8] &= ((0x80 >> (i%8)) ^ 0xff);
//						continue;
//					}
//
//					memcpy(andres, &andres_size, ROW_SIZE_BYTES);
//					free(data);
//					data = (unsigned char *) malloc (andres_size + ROW_SIZE_BYTES);
//					memcpy(data, andres, andres_size + ROW_SIZE_BYTES);
					//Populate objfold array too
					struct row r = {rownum, data};
					bitmat->bm.push_back(r);
//					dgap_uncompress(data + ROW_SIZE_BYTES, andres_size, bitmat->objfold, bitmat->object_bytes);
				}
				rownum++;
			}

		}
	}

	//TODO: Optimization required if this method is going to be heavily used.. right now ignoring
	simple_unfold(bitmat, and_array, and_array_size, COLUMN, 3);
	simple_fold(bitmat, ROW, bitmat->subfold, bitmat->subject_bytes);
	simple_fold(bitmat, COLUMN, bitmat->objfold, bitmat->object_bytes);

//	printf("Total size loaded from file %u\n", total_size);

#if MMAPFILES
#else
	close(fd);
#endif

	if (bitmat->num_triples != 0)
		return true;

	return false;

}

////////////////////////////////////////////////////////////
void clear_rows(BitMat *bitmat, bool clearbmrows, bool clearfoldarr, bool optimize)
{
	if (clearbmrows) {
		if (bitmat->bm.size() > 0) {
			for (std::list<struct row>::iterator it = bitmat->bm.begin(); it != bitmat->bm.end(); ){
				free((*it).data);
				it = bitmat->bm.erase(it);
			}
		}

	}

	if (clearfoldarr) {

		//This was probably done this way for optimizing the load
		//operation, but this is clearly wrong if clear_rows
		//is used independently
		if (optimize) {
			if (bitmat->dimension != PSO_BITMAT && bitmat->dimension != POS_BITMAT) {
				if (comp_folded_arr == 0 && bitmat->subfold != NULL)
					memset(bitmat->subfold, 0, bitmat->subject_bytes);
				if (bitmat->objfold != NULL)
					memset(bitmat->objfold, 0, bitmat->object_bytes);
			}
		} else {

			if (bitmat->subfold != NULL)
				memset(bitmat->subfold, 0, bitmat->subject_bytes);
			if (bitmat->objfold != NULL)
				memset(bitmat->objfold, 0, bitmat->object_bytes);
		}

	}

}
///////////////////////////////////////////////////////////////////////
//TODO: make use of the fact that at any load pt
//if any bitmat is compl empty the result is empty
//since it's a join

bool load_one_row(BitMat *bitmat, char *fname, unsigned int rownum, unsigned int bmnum, bool load_objfold)
{
	int fd = 0;
	unsigned int total_size = 0, size = 0;
	char *fpos = NULL;

	unsigned long offset = get_offset(fname, bmnum);

#if MMAPFILES
	fpos = mmap_table[string(fname)];
#else
	fd = open(fname, O_RDONLY | O_LARGEFILE);
	if (fd < 0) {
		cout << "*** ERROR opening dump file " << fname << endl;
		assert(0);
	}

	//Move over number of triples
	lseek(fd, offset + sizeof(unsigned int), SEEK_CUR);
	total_size += sizeof(unsigned int);
#endif

	assert((fpos == NULL) ^ (fd == 0));

	if (bitmat->subfold == NULL) {
		bitmat->subfold = (unsigned char *) malloc (bitmat->subject_bytes);
		memset(bitmat->subfold, 0, bitmat->subject_bytes);
	}
	if (bitmat->objfold == NULL && load_objfold) {
		bitmat->objfold = (unsigned char *) malloc (bitmat->object_bytes);
		memset(bitmat->objfold, 0, bitmat->object_bytes);
	}

	//TODO: make changes here because now you no more
	//store compressed objfold for PSO and POS bitmats
	if (bitmat->dimension == PSO_BITMAT || bitmat->dimension == POS_BITMAT || comp_folded_arr) {
		unsigned int comp_arr_size = 0;
#if MMAPFILES
		//sizeof(unsigned int) is to walk over the number of triples bytes
		memcpy(&comp_arr_size, &fpos[offset + sizeof(unsigned int)], ROW_SIZE_BYTES);
		fpos += offset + sizeof(unsigned int) + ROW_SIZE_BYTES;
#else
		read(fd, &comp_arr_size, ROW_SIZE_BYTES);
#endif

		unsigned char *comp_arr =  (unsigned char *) malloc (comp_arr_size);

#if MMAPFILES
		memcpy(comp_arr, fpos, comp_arr_size);
		fpos += comp_arr_size;
#else
		read(fd, comp_arr, comp_arr_size);
#endif
		total_size += ROW_SIZE_BYTES + comp_arr_size;

		dgap_uncompress(comp_arr, comp_arr_size, bitmat->subfold, bitmat->subject_bytes);

		free(comp_arr);

		//Just move over objfold array as you don't need it
		//TODO:
		if (bitmat->dimension != PSO_BITMAT && bitmat->dimension != POS_BITMAT) {
			comp_arr_size = 0;
#if MMAPFILES
			memcpy(&comp_arr_size, fpos, ROW_SIZE_BYTES);
			fpos += (ROW_SIZE_BYTES + comp_arr_size);
#else
			read(fd, &comp_arr_size, ROW_SIZE_BYTES);
			lseek(fd, comp_arr_size, SEEK_CUR);
#endif
			total_size += ROW_SIZE_BYTES + comp_arr_size;
		}

	} else {
#if MMAPFILES
		memcpy(bitmat->subfold, fpos, bitmat->subject_bytes);
		fpos += (bitmat->subject_bytes + bitmat->object_bytes);
#else
		read(fd, bitmat->subfold, bitmat->subject_bytes);
		lseek(fd, bitmat->object_bytes, SEEK_CUR);
#endif

		total_size += (bitmat->subject_bytes + bitmat->object_bytes);
	}

	if (bitmat->subfold[(rownum-1)/8] & (0x80 >> ((rownum-1)%8)) ) {
		//this row exists
		for (unsigned int i = 0; i < rownum - 1; i++) {
			if (bitmat->subfold[i/8] & (0x80 >> (i%8)) ) {
				//this row exists
#if MMAPFILES
				memcpy(&size, fpos, ROW_SIZE_BYTES);
				fpos += (ROW_SIZE_BYTES+size);
#else
				read(fd, &size, ROW_SIZE_BYTES);
				lseek(fd, size, SEEK_CUR);
#endif
				total_size += (size + ROW_SIZE_BYTES);
			}
		}
#if MMAPFILES
		memcpy(&size, fpos, ROW_SIZE_BYTES);
		fpos += ROW_SIZE_BYTES;
#else
		read(fd, &size, ROW_SIZE_BYTES);
#endif

//		if (bitmat->bm == NULL) {
			//just load one row
//			bitmat->bm = (unsigned char **) malloc (sizeof(unsigned char *));
//		}
		unsigned char *data = (unsigned char *) malloc (size + ROW_SIZE_BYTES);
		memcpy(data, &size, ROW_SIZE_BYTES);
#if MMAPFILES
		memcpy(data + ROW_SIZE_BYTES, fpos, size);
		fpos += size;
#else
		read(fd, data + ROW_SIZE_BYTES, size);
#endif
		struct row r = {rownum, data};
		bitmat->bm.push_back(r);
		total_size += (size + ROW_SIZE_BYTES);
		//TODO: make subfold and objfold consistent to this row
		memset(bitmat->subfold, 0, bitmat->subject_bytes);
		bitmat->subfold[(rownum-1)/8] |= (0x80 >> ((rownum-1)%8));
		//Populating objfold
		if (load_objfold) dgap_uncompress(data + ROW_SIZE_BYTES, size, bitmat->objfold, bitmat->object_bytes);
		bitmat->single_row = true;
#if MMAPFILES
#else
		close(fd);
#endif
		return true;
	}

#if MMAPFILES
#else
		close(fd);
#endif

	//since you came here, it means that the
	//row doesn't exist
	return false;

}

///////////////////////////////////////////////////////////
bool add_row(BitMat *bitmat, char *fname, unsigned int bmnum, unsigned int readdim, unsigned int rownum, bool load_objfold)
{

//	cout << "Inside add_row" << endl;

//	cout << "Advanced file pointer" << endl;

	//TODO: this will go away later
	if (readdim == bitmat->dimension) {

//		cout << "Calling load_one_row" << endl;
		return load_one_row(bitmat, fname, rownum, bmnum, load_objfold);
	} else {
		assert(0);
		return false;
	}
/*
	size = 0;

	if (tmpsubf[(rownum-1)/8] & (0x80 >> ((rownum-1)%8)) ) {
		//"bmnum" is the predicate num in the original SPO BM
		if (bitmat->bm == NULL) {
			init_bitmat_rows(bitmat, true, true);
		}
		bitmat->subfold[(bmnum-1)/8] |= 0x80 >> ((bmnum-1)%8);

		for (unsigned int i = 0; i < rownum - 1; i++) {
			if (tmpsubf[i/8] & (0x80 >> (i%8)) ) {
				//this row exists
				read(fd, &size, ROW_SIZE_BYTES);
				lseek(fd, size, SEEK_CUR);
				total_size += (size + ROW_SIZE_BYTES);
			}
		}
		read(fd, &size, ROW_SIZE_BYTES);
		bitmat->bm[bmnum-1] = (unsigned char *) malloc (size + ROW_SIZE_BYTES);
		memcpy(bitmat->bm[bmnum-1], &size, ROW_SIZE_BYTES);
		read(fd, bitmat->bm[bmnum-1] + ROW_SIZE_BYTES, size);
		total_size += (size + ROW_SIZE_BYTES);
		return true;
	}
	return false;
*/
}
/////////////////////////////////////////////////////
unsigned int get_mask_array(unsigned char *and_array, unsigned int setbit)
{
	unsigned int and_array_size = setbit/8 + (setbit%8 > 0 ? 1:0);
	and_array = (unsigned char *) malloc (and_array_size);
	memset(and_array, 0, and_array_size);
	and_array[(setbit-1)/8] |= (0x80 >> ((setbit-1)%8));

	return and_array_size;
}

//////////////////////////////////////////////////////
unsigned int get_and_array(BitMat *bitmat, unsigned char *and_array, unsigned int bit)
{
	unsigned int t = 1;
	unsigned int and_array_size = 1;
	unsigned later_0 = (bitmat->object_bytes << 3) - bit;
	unsigned ini_0 = bit - 1;

	if (bit == 1) {
		and_array[0] = 0x01;
		memcpy(&and_array[1], &t, GAP_SIZE_BYTES);
		and_array_size += GAP_SIZE_BYTES;
	} else {
		and_array[0] = 0x00;
		memcpy(&and_array[1], &ini_0, GAP_SIZE_BYTES);
		memcpy(&and_array[GAP_SIZE_BYTES+1], &t, GAP_SIZE_BYTES);
		and_array_size += 2*GAP_SIZE_BYTES;
	}
	if (later_0 > 0) {
		memcpy(&and_array[and_array_size], &later_0, GAP_SIZE_BYTES);
		and_array_size += GAP_SIZE_BYTES;
	}

	return and_array_size;

}
////////////////////////////////
void shallow_init_bitmat(BitMat *bitmat, unsigned int snum, unsigned int pnum, unsigned int onum, unsigned int commsonum, int dimension)
{
	bitmat->bm.clear();
	bitmat->num_subs = snum;
	bitmat->num_preds = pnum;
	bitmat->num_objs = onum;
	bitmat->num_comm_so = commsonum;

//	row_size_bytes = bitmat->row_size_bytes;
//	gap_size_bytes = bitmat->gap_size_bytes;
	bitmat->dimension = dimension;

	bitmat->subject_bytes = (snum%8>0 ? snum/8+1 : snum/8);
	bitmat->predicate_bytes = (pnum%8>0 ? pnum/8+1 : pnum/8);
	bitmat->object_bytes = (onum%8>0 ? onum/8+1 : onum/8);
	bitmat->common_so_bytes = (commsonum%8>0 ? commsonum/8+1 : commsonum/8);

	bitmat->subfold = NULL;
	bitmat->objfold = NULL;
//	bitmat->subfold_prev = NULL;
//	bitmat->objfold_prev = NULL;
	bitmat->single_row = false;
	bitmat->num_triples = 0;
}

unsigned long count_size_of_bitmat(BitMat *bitmat)
{
	unsigned long size = 0;

	if (bitmat->bm.size() > 0) {
		for (std::list<struct row>::iterator it = bitmat->bm.begin(); it != bitmat->bm.end(); it++) {
			unsigned int rowsize = 0;
			memcpy(&rowsize, (*it).data, ROW_SIZE_BYTES);
			size += rowsize + ROW_SIZE_BYTES + sizeof((*it).rowid);
		}
	} else if (bitmat->vbm.size() > 0) {
		for (vector<struct row>::iterator it = bitmat->vbm.begin(); it != bitmat->vbm.end(); it++) {
			unsigned int rowsize = 0;
			memcpy(&rowsize, (*it).data, ROW_SIZE_BYTES);
			size += rowsize + ROW_SIZE_BYTES + sizeof((*it).rowid);
		}

	}
	if (bitmat->subfold != NULL) {
		size += sizeof(bitmat->subject_bytes);
	}
//	if (bitmat->subfold_prev != NULL) {
//		size += sizeof(bitmat->subject_bytes);
//	}
	if (bitmat->objfold != NULL) {
		size += sizeof(bitmat->object_bytes);
	}
//	if (bitmat->objfold_prev != NULL) {
//		size += sizeof(bitmat->object_bytes);
//	}

	return size;
}

//////////////////////////////////////////
unsigned long get_offset(char *dumpfile, unsigned int bmnum)
{
	//Get the offset
	char tablefile[1024];
	sprintf(tablefile, "%s_table", dumpfile);
	unsigned long offset = 0;
#if MMAPFILES
	char *mmapfile = mmap_table[string(tablefile)];
	memcpy(&offset, &mmapfile[(bmnum-1)*table_col_bytes], table_col_bytes);
#else
	int fd = open(tablefile, O_RDONLY);
	if (fd < 0) {
		cout << "*** ERROR opening " << tablefile << endl;
		assert (0);
	}

	lseek(fd, (bmnum-1)*table_col_bytes, SEEK_CUR);
	unsigned char tablerow[table_col_bytes];
	read(fd, tablerow, table_col_bytes);
	memcpy(&offset, tablerow, table_col_bytes);
	close(fd);
#endif
	return offset;

}

bool mmap_all_files()
{
	int fd = 0; off64_t size = 0; char *fstream; char tablefile[1024];

	fd = open((char *)config[string("BITMATDUMPFILE_SPO")].c_str(), O_RDONLY);
	assert(fd >= 0);
	size = lseek64(fd, 0, SEEK_END);
	vectfd[0] = make_pair(fd, size);
	fstream = (char *) mmap64(0, size, PROT_READ, MAP_PRIVATE, fd, 0);
	assert(fstream != (void *)-1);
	mmap_table[config[string("BITMATDUMPFILE_SPO")]] = fstream;
	sprintf(tablefile, "%s_table", (char *)config[string("BITMATDUMPFILE_SPO")].c_str());
	fd = open(tablefile, O_RDONLY);
	assert(fd >= 0);
	size = lseek64(fd, 0, SEEK_END);
	vectfd[1] = make_pair(fd, size);
	fstream = (char *) mmap64(0, size, PROT_READ, MAP_PRIVATE, fd, 0);
	assert(fstream != (void *)-1);
	mmap_table[string(tablefile)] = fstream;

	fd = open((char *)config[string("BITMATDUMPFILE_OPS")].c_str(), O_RDONLY);
	assert(fd >= 0);
	size = lseek64(fd, 0, SEEK_END);
	vectfd[2] = make_pair(fd, size);
	fstream = (char *) mmap64(0, size, PROT_READ, MAP_PRIVATE, fd, 0);
	assert(fstream != (void *)-1);
	mmap_table[config[string("BITMATDUMPFILE_OPS")]] = fstream;
	sprintf(tablefile, "%s_table", (char *)config[string("BITMATDUMPFILE_OPS")].c_str());
	fd = open(tablefile, O_RDONLY);
	assert(fd >= 0);
	size = lseek64(fd, 0, SEEK_END);
	vectfd[3] = make_pair(fd, size);
	fstream = (char *) mmap64(0, size, PROT_READ, MAP_PRIVATE, fd, 0);
	assert(fstream != (void *)-1);
	mmap_table[string(tablefile)] = fstream;

	fd = open((char *)config[string("BITMATDUMPFILE_PSO")].c_str(), O_RDONLY);
	assert(fd >= 0);
	size = lseek64(fd, 0, SEEK_END);
	vectfd[4] = make_pair(fd, size);
	fstream = (char *) mmap64(0, size, PROT_READ, MAP_PRIVATE, fd, 0);
	assert(fstream != (void *)-1);
	mmap_table[config[string("BITMATDUMPFILE_PSO")]] = fstream;
	sprintf(tablefile, "%s_table", (char *)config[string("BITMATDUMPFILE_PSO")].c_str());
	fd = open(tablefile, O_RDONLY);
	assert(fd >= 0);
	size = lseek64(fd, 0, SEEK_END);
	vectfd[5] = make_pair(fd, size);
	fstream = (char *) mmap64(0, size, PROT_READ, MAP_PRIVATE, fd, 0);
	assert(fstream != (void *)-1);
	mmap_table[string(tablefile)] = fstream;


	fd = open((char *)config[string("BITMATDUMPFILE_POS")].c_str(), O_RDONLY);
	assert(fd >= 0);
	size = lseek64(fd, 0, SEEK_END);
	vectfd[6] = make_pair(fd, size);
	fstream = (char *) mmap64(0, size, PROT_READ, MAP_PRIVATE, fd, 0);
	assert(fstream != (void *)-1);
	mmap_table[config[string("BITMATDUMPFILE_POS")]] = fstream;
	sprintf(tablefile, "%s_table", (char *)config[string("BITMATDUMPFILE_POS")].c_str());
	fd = open(tablefile, O_RDONLY);
	assert(fd >= 0);
	size = lseek64(fd, 0, SEEK_END);
	vectfd[7] = make_pair(fd, size);
	fstream = (char *) mmap64(0, size, PROT_READ, MAP_PRIVATE, fd, 0);
	assert(fstream != (void *)-1);
	mmap_table[string(tablefile)] = fstream;

	return true;
}

bool munmap_all_files()
{

	char tablefile[1024];

	munmap(mmap_table[config[string("BITMATDUMPFILE_SPO")]], vectfd[0].second);
	close(vectfd[0].first);
	sprintf(tablefile, "%s_table", (char *)config[string("BITMATDUMPFILE_SPO")].c_str());
	munmap(mmap_table[string(tablefile)], vectfd[1].second);
	close(vectfd[1].first);

	munmap(mmap_table[config[string("BITMATDUMPFILE_OPS")]], vectfd[2].second);
	close(vectfd[2].first);
	sprintf(tablefile, "%s_table", (char *)config[string("BITMATDUMPFILE_OPS")].c_str());
	munmap(mmap_table[string(tablefile)], vectfd[3].second);
	close(vectfd[3].first);

	munmap(mmap_table[config[string("BITMATDUMPFILE_PSO")]], vectfd[4].second);
	close(vectfd[4].first);
	sprintf(tablefile, "%s_table", (char *)config[string("BITMATDUMPFILE_PSO")].c_str());
	munmap(mmap_table[string(tablefile)], vectfd[5].second);
	close(vectfd[5].first);

	munmap(mmap_table[config[string("BITMATDUMPFILE_POS")]], vectfd[6].second);
	close(vectfd[6].first);
	sprintf(tablefile, "%s_table", (char *)config[string("BITMATDUMPFILE_POS")].c_str());
	munmap(mmap_table[string(tablefile)], vectfd[7].second);
	close(vectfd[7].first);

	return true;
}

void get_all_triples_in_query(char *file)
{
	vector<twople> twoples;
	FILE *ftmp = NULL;
	if (file != NULL)
		ftmp = fopen(file, "w");

	for (int i = 0; i < graph_tp_nodes; i++) {
//		if (!graph[MAX_JVARS_IN_QUERY + i].isNeeded)
//			continue;

		TP *tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;

		switch (tp->bitmat.dimension) {
			case SPO_BITMAT:
				if (ftmp) {
//					fprintf(ftmp, "----------begin [%d %d %d] %d -------------------------\n", tp->sub, tp->pred, tp->obj, tp->bitmat.dimension);
				} else
					cout << "----------begin [" << tp->sub << " " << tp->pred << " " << tp->obj << "]-------------------------" << endl;
				list_enctrips_bitmat_new(&tp->bitmat, tp->pred, twoples, ftmp);
				for (vector<twople>::iterator it = twoples.begin(); it != twoples.end(); it++) {
					cout << (*it).sub << ":" << tp->pred << ":" << (*it).obj << endl;
				}
				if (ftmp) {
//					fprintf(ftmp, "----------end [%d %d %d] %d -------------------------\n", tp->sub, tp->pred, tp->obj, tp->bitmat.dimension);
				} else 
					cout << "----------end [" << tp->sub << " " << tp->pred << " " << tp->obj << "]-------------------------" << endl;
				break;

			case OPS_BITMAT:
				if (ftmp) {
//					fprintf(ftmp, "----------begin [%d %d %d] %d -----------------\n", tp->sub, tp->pred, tp->obj, tp->bitmat.dimension);
				} else
					cout << "----------begin [" << tp->sub << " " << tp->pred << " " << tp->obj << "]-------------------------" << endl;
				list_enctrips_bitmat_new(&tp->bitmat, tp->pred, twoples, ftmp);
				for (vector<twople>::iterator it = twoples.begin(); it != twoples.end(); it++) {
					cout << (*it).obj << ":" << tp->pred << ":" << (*it).sub << endl;
				}
				if (ftmp) {
//					fprintf(ftmp, "----------end [%d %d %d] %d -------------------\n", tp->sub, tp->pred, tp->obj, tp->bitmat.dimension);
				} else 
					cout << "----------end [" << tp->sub << " " << tp->pred << " " << tp->obj << "]-------------------------" << endl;
				break;

			case PSO_BITMAT:
				if (ftmp) {
//					fprintf(ftmp, "----------begin [%d %d %d] %d -----------------\n", tp->sub, tp->pred, tp->obj, tp->bitmat.dimension);
				} else
					cout << "----------begin [" << tp->sub << " " << tp->pred << " " << tp->obj << "]-------------------------" << endl;
				list_enctrips_bitmat_new(&tp->bitmat, tp->sub, twoples, ftmp);
				for (vector<twople>::iterator it = twoples.begin(); it != twoples.end(); it++) {
					cout << tp->sub << ":" << (*it).sub << ":" << (*it).obj << endl;
				}
				if (ftmp) {
//					fprintf(ftmp, "----------end [%d %d %d] %d ------------------\n", tp->sub, tp->pred, tp->obj, tp->bitmat.dimension);
				} else 
					cout << "----------end [" << tp->sub << " " << tp->pred << " " << tp->obj << "]-------------------------" << endl;
				break;

			case POS_BITMAT:
				if (ftmp) {
//					fprintf(ftmp, "----------begin [%d %d %d] %d -----------------\n", tp->sub, tp->pred, tp->obj, tp->bitmat.dimension);
				} else
					cout << "----------begin [" << tp->sub << " " << tp->pred << " " << tp->obj << "]-------------------------" << endl;
				list_enctrips_bitmat_new(&tp->bitmat, tp->obj, twoples, ftmp);
				for (vector<twople>::iterator it = twoples.begin(); it != twoples.end(); it++) {
					cout << (*it).obj << ":" << (*it).sub << ":" << tp->obj << endl;
				}
				if (ftmp) {
//					fprintf(ftmp, "----------end [%d %d %d] %d -------------------\n", tp->sub, tp->pred, tp->obj, tp->bitmat.dimension);
				} else 
					cout << "----------end [" << tp->sub << " " << tp->pred << " " << tp->obj << "]-------------------------" << endl;
				break;

			default:
				break;

		}

	}
	if (ftmp != NULL)
		fclose(ftmp);

}
void get_all_triples_in_query2(char *file)
{
	FILE *ftmp = NULL;
	if (file != NULL)
		ftmp = fopen(file, "w");

	vector<twople> twoples;
	for (int i = 0; i < graph_tp_nodes; i++) {
//		if (!graph[MAX_JVARS_IN_QUERY + i].isNeeded)
//			continue;
		TP *tp = (TP *)graph[MAX_JVARS_IN_QUERY + i].data;

		switch (tp->bitmat.dimension) {
			case SPO_BITMAT:
				if (ftmp)
					fprintf(ftmp, "----------begin [%d %d %d] %d ----------------\n", tp->sub, tp->pred, tp->obj, tp->bitmat.dimension);
				else
					cout << "----------begin [" << tp->sub << " " << tp->pred << " " << tp->obj << "]-------------------------" << endl;

				list_enctrips_bitmat2(&tp->bitmat, tp->pred, twoples, NULL);
				for (vector<twople>::iterator it = twoples.begin(); it != twoples.end(); it++) {
					cout << (*it).sub << ":" << tp->pred << ":" << (*it).obj << endl;
				}
				if (ftmp)
					fprintf(ftmp, "----------end [%d %d %d] %d ------------------\n", tp->sub, tp->pred, tp->obj, tp->bitmat.dimension);
				else
					cout << "----------end [" << tp->sub << " " << tp->pred << " " << tp->obj << "]-------------------------" << endl;

				break;

			case OPS_BITMAT:
				if (ftmp)
					fprintf(ftmp, "----------begin [%d %d %d] %d ----------------\n", tp->sub, tp->pred, tp->obj, tp->bitmat.dimension);
				else
					cout << "----------begin [" << tp->sub << " " << tp->pred << " " << tp->obj << "]-------------------------" << endl;

				list_enctrips_bitmat2(&tp->bitmat, tp->pred, twoples, NULL);
				for (vector<twople>::iterator it = twoples.begin(); it != twoples.end(); it++) {
					cout << (*it).obj << ":" << tp->pred << ":" << (*it).sub << endl;
				}
				if (ftmp)
					fprintf(ftmp, "----------end [%d %d %d] %d ------------------\n", tp->sub, tp->pred, tp->obj, tp->bitmat.dimension);
				else
					cout << "----------end [" << tp->sub << " " << tp->pred << " " << tp->obj << "]-------------------------" << endl;

				break;

			case PSO_BITMAT:
				if (ftmp)
					fprintf(ftmp, "----------begin [%d %d %d] %d -----------------\n", tp->sub, tp->pred, tp->obj, tp->bitmat.dimension);
				else
					cout << "----------begin [" << tp->sub << " " << tp->pred << " " << tp->obj << "]-------------------------" << endl;

				list_enctrips_bitmat2(&tp->bitmat, tp->sub, twoples, NULL);
				for (vector<twople>::iterator it = twoples.begin(); it != twoples.end(); it++) {
					cout << tp->sub << ":" << (*it).sub << ":" << (*it).obj << endl;
				}
				if (ftmp)
					fprintf(ftmp, "----------end [%d %d %d] %d ------------------\n", tp->sub, tp->pred, tp->obj, tp->bitmat.dimension);
				else
					cout << "----------end [" << tp->sub << " " << tp->pred << " " << tp->obj << "]-------------------------" << endl;

				break;

			case POS_BITMAT:
				if (ftmp)
					fprintf(ftmp, "----------begin [%d %d %d] %d -----------------\n", tp->sub, tp->pred, tp->obj, tp->bitmat.dimension);
				else
					cout << "----------begin [" << tp->sub << " " << tp->pred << " " << tp->obj << "]-------------------------" << endl;

				list_enctrips_bitmat2(&tp->bitmat, tp->obj, twoples, NULL);
				for (vector<twople>::iterator it = twoples.begin(); it != twoples.end(); it++) {
					cout << (*it).obj << ":" << (*it).sub << ":" << tp->obj << endl;
				}
				if (ftmp)
					fprintf(ftmp, "----------end [%d %d %d] %d -------------------\n", tp->sub, tp->pred, tp->obj, tp->bitmat.dimension);
				else
					cout << "----------end [" << tp->sub << " " << tp->pred << " " << tp->obj << "]-------------------------" << endl;

				break;

			default:
				break;

		}

	}

	if (ftmp != NULL)
		fclose(ftmp);
}

void print_stats(char *fname_dump, unsigned int numbms, bool compfold, unsigned int numsubs, unsigned int numobjs)
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
	unsigned int object_bytes = (numobjs%8>0 ? numobjs/8+1 : numobjs/8);

	unsigned char subfold[subject_bytes];
	unsigned char objfold[object_bytes];

	memset(subfold, 0, subject_bytes);
	memset(objfold, 0, object_bytes);

	cout << "Subject_bytes " << subject_bytes << endl;
	cout << "Object_bytes " << object_bytes << endl;

	cout << fname_dump << " " << tablefile << endl;

	unsigned int pred_subs = 0, pred_objs = 0;
	unsigned long prev_offset = 0, size = 0;

	for (unsigned int i = 0; i < numbms; i++) {
//		offset = get_offset(fname_dump, i+1);
//		cout << "Offset is " << offset << " ";
		read(fd_table, &offset, table_col_bytes);
		size = offset - prev_offset;

		if (pred_subs != 0 && pred_objs != 0 && size != 0) {
			cout << i << " #Subjects " << pred_subs << " #Objects " << pred_objs << " #Size " << size << " #Triples " << numtriples << endl;
		}

		assert (lseek(fd, offset, SEEK_SET) != -1);
		//Moving over the numtriples field
		read(fd, &numtriples, sizeof(unsigned int));
//		cout << "***#triples " << numtriples << " ";
//		cout << "***Counting rows for bmnum " << i + 1 << " -- ";
		if (compfold) {
			unsigned int comp_arr_size = 0;
			read(fd, &comp_arr_size, ROW_SIZE_BYTES);
			unsigned char *comp_arr = (unsigned char *) malloc (comp_arr_size);
			dgap_uncompress(comp_arr, comp_arr_size, subfold, subject_bytes);
		} else {
			read(fd, subfold, subject_bytes);
			read(fd, objfold, object_bytes);
		}

		pred_subs = count_bits_in_row(subfold, subject_bytes);
		pred_objs = count_bits_in_row(objfold, object_bytes);

//		cout << i+1 << " #Subjects " << pred_subs << " #Objects " << pred_objs << " #Size " << size << " #Triples " << numtriples << endl;

		prev_offset = offset;
		offset = 0;
		//Now count set bits in subfold
//		cout << "#Subjects " << count_bits_in_row(subfold, subject_bytes) << " #Objects " << count_bits_in_row(objfold, object_bytes) << endl;
	}

	offset = lseek(fd, 0L, SEEK_END);
	size = offset - prev_offset;

	cout << numbms << " #Subjects " << pred_subs << " #Objects " << pred_objs << " #Size " << size << " #Triples " << numtriples << endl;
}

unsigned int count_intersect(unsigned int bm1, unsigned int bm2)
{
	BitMat bitmat_spo1;
	init_bitmat(&bitmat_spo1, gnum_subs, gnum_preds, gnum_objs, gnum_comm_so, SPO_BITMAT);
	BitMat bitmat_spo2;
	init_bitmat(&bitmat_spo2, gnum_subs, gnum_preds, gnum_objs, gnum_comm_so, SPO_BITMAT);
	char *fpos1 = NULL; char *fpos2 = NULL;
	unsigned long offset1 = get_offset((char *)config[string("BITMATDUMPFILE_SPO")].c_str(), bm1);
	unsigned long offset2 = get_offset((char *)config[string("BITMATDUMPFILE_SPO")].c_str(), bm2);

#if MMAPFILES
	fpos1 = mmap_table[string((char *)config[string("BITMATDUMPFILE_SPO")].c_str())];
	fpos2 = fpos1;
	fpos1 += offset1;
	fpos2 += offset2;
#endif

#if MMAPFILES
	memcpy(&bitmat_spo1.num_triples, fpos1, sizeof(unsigned int));
	fpos1 += sizeof(unsigned int);
	memcpy(&bitmat_spo2.num_triples, fpos2, sizeof(unsigned int));
	fpos2 += sizeof(unsigned int);
#endif

#if MMAPFILES
	memcpy(bitmat_spo1.subfold, fpos1, bitmat_spo1.subject_bytes);
	fpos1 += bitmat_spo1.subject_bytes;
	memcpy(bitmat_spo1.objfold, fpos1, bitmat_spo1.object_bytes);
	fpos1 += bitmat_spo1.object_bytes;

	memcpy(bitmat_spo2.subfold, fpos2, bitmat_spo2.subject_bytes);
	fpos2 += bitmat_spo2.subject_bytes;
	memcpy(bitmat_spo2.objfold, fpos2, bitmat_spo2.object_bytes);
	fpos2 += bitmat_spo2.object_bytes;
#endif

	unsigned char res[bitmat_spo1.common_so_bytes];

	for (unsigned int i=0; i < bitmat_spo1.common_so_bytes; i++) {
		res[i] = bitmat_spo1.objfold[i] & bitmat_spo2.subfold[i];
	}

	res[bitmat_spo1.common_so_bytes-1] &= (0xff << (8-(bitmat_spo1.num_comm_so%8)));

	return count_bits_in_row(res, bitmat_spo1.common_so_bytes);
}

unsigned int count_intersect_ss(unsigned int bm1, unsigned int bm2)
{
	BitMat bitmat_spo1;
	init_bitmat(&bitmat_spo1, gnum_subs, gnum_preds, gnum_objs, gnum_comm_so, SPO_BITMAT);
	BitMat bitmat_spo2;
	init_bitmat(&bitmat_spo2, gnum_subs, gnum_preds, gnum_objs, gnum_comm_so, SPO_BITMAT);
	char *fpos1 = NULL; char *fpos2 = NULL;
	unsigned long offset1 = get_offset((char *)config[string("BITMATDUMPFILE_SPO")].c_str(), bm1);
	unsigned long offset2 = get_offset((char *)config[string("BITMATDUMPFILE_SPO")].c_str(), bm2);

#if MMAPFILES
	fpos1 = mmap_table[string((char *)config[string("BITMATDUMPFILE_SPO")].c_str())];
	fpos2 = fpos1;
	fpos1 += offset1;
	fpos2 += offset2;
#endif

#if MMAPFILES
	memcpy(&bitmat_spo1.num_triples, fpos1, sizeof(unsigned int));
	fpos1 += sizeof(unsigned int);
	memcpy(&bitmat_spo2.num_triples, fpos2, sizeof(unsigned int));
	fpos2 += sizeof(unsigned int);
#endif

#if MMAPFILES
	memcpy(bitmat_spo1.subfold, fpos1, bitmat_spo1.subject_bytes);
	memcpy(bitmat_spo2.subfold, fpos2, bitmat_spo2.subject_bytes);
#endif

	unsigned char res[bitmat_spo1.subject_bytes];

	for (unsigned int i=0; i < bitmat_spo1.subject_bytes; i++) {
		res[i] = bitmat_spo1.subfold[i] & bitmat_spo2.subfold[i];
	}

	return count_bits_in_row(res, bitmat_spo1.subject_bytes);
}

unsigned int count_intersect_oo(unsigned int bm1, unsigned int bm2)
{
	BitMat bitmat_spo1;
	init_bitmat(&bitmat_spo1, gnum_subs, gnum_preds, gnum_objs, gnum_comm_so, SPO_BITMAT);
	BitMat bitmat_spo2;
	init_bitmat(&bitmat_spo2, gnum_subs, gnum_preds, gnum_objs, gnum_comm_so, SPO_BITMAT);
	char *fpos1 = NULL; char *fpos2 = NULL;
	unsigned long offset1 = get_offset((char *)config[string("BITMATDUMPFILE_SPO")].c_str(), bm1);
	unsigned long offset2 = get_offset((char *)config[string("BITMATDUMPFILE_SPO")].c_str(), bm2);

#if MMAPFILES
	fpos1 = mmap_table[string((char *)config[string("BITMATDUMPFILE_SPO")].c_str())];
	fpos2 = fpos1;
	fpos1 += offset1;
	fpos2 += offset2;
#endif

#if MMAPFILES
	memcpy(&bitmat_spo1.num_triples, fpos1, sizeof(unsigned int));
	fpos1 += sizeof(unsigned int);
	memcpy(&bitmat_spo2.num_triples, fpos2, sizeof(unsigned int));
	fpos2 += sizeof(unsigned int);
#endif

#if MMAPFILES
//	memcpy(bitmat_spo1.subfold, fpos1, bitmat_spo1.subject_bytes);
	fpos1 += bitmat_spo1.subject_bytes;
	memcpy(bitmat_spo1.objfold, fpos1, bitmat_spo1.object_bytes);
	fpos1 += bitmat_spo1.object_bytes;

//	memcpy(bitmat_spo2.subfold, fpos2, bitmat_spo2.subject_bytes);
	fpos2 += bitmat_spo2.subject_bytes;
	memcpy(bitmat_spo2.objfold, fpos2, bitmat_spo2.object_bytes);
	fpos2 += bitmat_spo2.object_bytes;
#endif

	unsigned char res[bitmat_spo1.object_bytes];

	for (unsigned int i=0; i < bitmat_spo1.object_bytes; i++) {
		res[i] = bitmat_spo1.objfold[i] & bitmat_spo2.objfold[i];
	}

	return count_bits_in_row(res, bitmat_spo1.object_bytes);
}
