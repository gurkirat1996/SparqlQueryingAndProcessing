int cn=97;	//used for levels

map<string, int> join;
void dfs_join(PatternGroup& group){
	for (auto i: group.patterns){
		for(auto j: {i.subject, i.object, i.predicate})
			if(j.type == Element::Variable)join[j.id]++;
	}
	for (auto i: group.filters);
	for (auto i: group.unions)dfs_join(i);
	for (auto i: group.optional)dfs_join(i);
}
map<int, int> val;
int neg=-1, neg2=MAX_JVARS-1; //to assign join/non-join values
int assign(int id){//return value for variables
	if(val.count(id))return val[id];
	if(join[id]>1)return val[id]=neg2--;
	else return val[id]=neg--;
}
///ISSUES: (1) id not negative. (2) freely access each other's elements
void dfs2(PatternGroup& group,string strlevel){

	for (auto i: group.patterns){;
		/*insert pattern into tp, with level=str*/

		




		TP *tp = new TP;
		auto tmp=i.subject;
		if(tmp.type==Element::Variable)tp->sub=assign(tmp.id);
		else tp->sub = ;//query b+ tree for id;

		tmp=i.object;
		if(tmp.type==Element::Variable)tp->obj=assign(tmp.id);
		else tp->obj = ;//query b+ tree for id;

		tmp=i.predicate;
		if(tmp.type==Element::Variable)tp->pred=assign(tmp.id);
		else tp->pred = ;//query b+ tree for id;

		// tp->sub = spos;
		// tp->pred = ppos;
		// tp->obj = opos;
		tp->nodenum = tplist_size + MAX_JVARS_IN_QUERY;
		tp->bitmat.bm.clear();
		tp->bitmat2.bm.clear();
		tp->unfolded = 0;
		tp->rollback = false;

		tp->strlevel = strlevel;

		tp->numpass = 0;
		tp->numjvars = 0;
		tp->numvars = 0;
		/
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

		int tm=0;	//to check if loop is for sub/obj/pred
		int _id;	//to keep calue of tp.sub/obj/pred
		for(auto j: {i.subject, i.object, i.predicate}){
			bool found = false;
			if(tm==0)_id=tp.sub;
			else if(tm==1) id=tp.obj;
			else if(tm==2) id=tp.pred;
			tm++;

			if(_id<0){
				for (int i = 0; i < varlist_size; i++) {
						jvar = (JVAR *)graph[i].data;
						if (jvar->nodenum == _id) {
							found = true;
							break;
						}
					}
				if (!found) {
					jvar = new JVAR;
					jvar->nodenum = _id;

					if (_id> MAX_JVARS) {
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
		}
	}
	for (auto i: group.filters){;/* what to do here? */}
	for (auto i: group.unions){
		for(auto j:i){
			;//not sure what to do here, ignore?
		}
	}
	int len=str.size():
	stirng strlevel2=strlevel;
	for (auto i: group.optional){
		dfs2(i, strlevel2);
		strlevel2=strlevel;//to ensure proper prefix condition
		strlevel2 += cn++;
	}
	return ;
}