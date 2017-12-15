
#include <bits/stdc++.h>
using namespace std;
typedef long long ll;
typedef pair<ll, ll> pii;
typedef pair<ll, ll> pll;
typedef vector<ll> vii;
typedef vector<ll> vll;

#define dbl long double
#define X first
#define Y second
#define rep(i,n) for(ll i=0; i<(n); i++)
#define repp(i,ed,b) for(ll i=ed; i<=(b); i++)
#define repp2(i,b,ed) for(ll i=b; i>=(ed); i--)
#define fill(ed,val) memset(ed, (val), sizeof(ed))
#define mp make_pair
#define pb push_back
#define sz(x) (ll)x.size()
#define all(c) (c).begin(), (c).end()
#define uni(c) c.resize(distance(c.begin(), unique(all(c))))
#define io ios_base::sync_with_stdio(false);cin.tie(NULL);
#define sc(n) scanf("%lld",&n)

ll mod=1e9+7, mod1=1e9+9;
const ll N=1e5, MAX=6e9;
dbl eps=1e-12;
string s1;
int main() {
    io;
    freopen("data.txt", "w",stdout);
    string ch="1234567890abcdefghijklmnopqrstuvwxyz";
    int len =sz(ch);
    ll cur=0;
    ll max_str=50;
    while(cur<MAX){
        int le=rand()%max_str+1;
        s1="";
        rep(i,le){
            s1+=ch[(rand()%len)];
        }
        if(cur%1000000==0)cerr<<cur<<endl;
        cout<<s1<<endl;
        cur+=le+1;  //for \n
    }


    return 0;
}
