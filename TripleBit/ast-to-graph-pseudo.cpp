int cn=97;	//used for levels

///ISSUES: (1) id not negative. (2) freely access each other's elements
void dfs2(PatternGroup& group, deque<struct level> lstack){

	for (auto i: group.patterns){;
		/*insert pattern into tp, with level=str*/
		TP *tp = new TP;
		// tp->sub = spos;
		// tp->pred = ppos;
		// tp->obj = opos;
		tp->nodenum = tplist_size + MAX_JVARS_IN_QUERY;
		tp->bitmat.bm.clear();
		tp->bitmat2.bm.clear();
		tp->unfolded = 0;
		tp->rollback = false;

		tp->level = lstack;

		tp->numpass = 0;
		tp->numjvars = 0;
		tp->numvars = 0;
		/*	
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
		*/

		graph[MAX_JVARS_IN_QUERY + tplist_size].type = TP_NODE;
		graph[MAX_JVARS_IN_QUERY + tplist_size].data = tp;
		tp->gtp = &graph[MAX_JVARS_IN_QUERY + tplist_size];
		graph[MAX_JVARS_IN_QUERY + tplist_size].nextTP = NULL;
		graph[MAX_JVARS_IN_QUERY + tplist_size].numTPs = 0;
		graph[MAX_JVARS_IN_QUERY + tplist_size].nextadjvar = NULL;
		graph[MAX_JVARS_IN_QUERY + tplist_size].numadjvars = 0;
		graph[MAX_JVARS_IN_QUERY + tplist_size].isNeeded = false;

		tplist_size++;

			///NOTE: here jval > 0
		for(auto j: {i.subject, i.object, i.predicate}){
			bool found = false;
			if(j.type == Element::Variable){
				for (int i = 0; i < varlist_size; i++) {
						jvar = (JVAR *)graph[i].data;
						if (jvar->nodenum == j.id) {
							found = true;
							break;
						}
					}
				if (!found) {
					jvar = new JVAR;
					jvar->nodenum = j.id;

					if (j.id > MAX_JVARS) {
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
	deque<struct level> l2=lstack;
	for (auto i: group.optional){
		dfs2(i, l2);
		l2=lstack;//to ensure proper prefix condition
		l.push_back({cn++, '$'});
	}
	return ;
}