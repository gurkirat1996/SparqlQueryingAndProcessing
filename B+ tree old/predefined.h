#ifndef PREDEFINED_H
#define PREDEFINED_H

#include <bits/stdc++.h>
using namespace std;

#define ull unsigned long long
#define puu pair<ull,ull>
#define X first
#define Y second

namespace bpt {

/* predefined B+ info */
#define BP_ORDER 24

/* key/value type */
typedef ull value_t;
struct key_t {
    puu k ;
    key_t(ull x=0,ull y=0)
    {
        k.X = x;
        k.Y = y;
    }
};

inline int keycmp(const key_t &a, const key_t &b) {
	puu p=a.k, q=b.k;
		  if(p.X!=q.X)return (p.X<q.X)?-1:1;
		  if(p.Y==q.Y)return 0;
		  return (p.Y<q.Y)?-1:1;
//    int x = strlen(a.k) - strlen(b.k);
//    return x == 0 ? strcmp(a.k, b.k) : x;
}

#define OPERATOR_KEYCMP(type) \
    bool operator< (const key_t &l, const type &r) {\
        return (l.k< r.key.k);\
    }\
    bool operator< (const type &l, const key_t &r) {\
        return (l.key.k< r.k);\
    }\
    bool operator== (const key_t &l, const type &r) {\
        return (l.k== r.key.k);\
    }\
    bool operator== (const type &l, const key_t &r) {\
        return (l.key.k== r.k);\
    }

}

#endif /* end of PREDEFINED_H */

