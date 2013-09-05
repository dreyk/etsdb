%% Author: gunin
%% Created: Sep 5, 2013
%% Description: TODO: Add description to etsdb_mscan_fsm
-module(etsdb_mscan_fsm).

-behaviour(gen_fsm).

-export([start_link/5]).


-export([init/1, prepare/2,execute/2,wait_result/2, handle_event/3,
	 handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-record(results,{num_ok=0,num_fail=0,ok_quorum=0,fail_quorum=0,indexes=[]}).
-record(state, {caller,preflist,it,getquery,timeout,bucket,req_ref,vnode_results,data}).

-include("etsdb_request.hrl").

start_link(Caller,Bucket,It,Query,Timeout) ->
    gen_fsm:start_link(?MODULE, [Caller,Bucket,It,Query,Timeout], []).

init([Caller,Bucket,It,Query,Timeout]) ->
    {ok,prepare, #state{caller=Caller,it=It,getquery=Query,bucket=Bucket,timeout=Timeout},0}.


prepare(timeout, #state{caller=Caller,it=It,bucket=Bucket}=StateData) ->
	case pscan(Bucket, It,[],[],[]) of
		{error,Error}->
			reply_to_caller(Caller,{error,Error}),
			{stop,normal,StateData};
		{Results,PrefList}->
			{next_state,execute,StateData#state{preflist=PrefList,data=[],vnode_results=Results},0}
	end.
pscan(_Bucket,emty,Result,AllPrefLists,_Included)->
	{Result,lists:usort(AllPrefLists)};
pscan(Bucket,#scan_it{partition=Partition}=It,Result,AllPrefLists,Included)->
	PartitionIdx = crypto:hash(sha,Partition),
	ReadCount = Bucket:r_val(),
	case preflist(PartitionIdx,ReadCount) of
		{error,Error}->
			{error,Error};
		[Vnode|_]=Preflist when length(Preflist)==ReadCount->
			case lists:member(Vnode,Included) of
				true->
					pscan(Bucket,Bucket:scan_partiotions(It),Result,AllPrefLists,Included);
				_->
					NumOk = Bucket:r_quorum(),
					NumFail = ReadCount-NumOk+1,
					PrefIndex = [Index||{Index,_}<-Preflist],
					pscan(Bucket,Bucket:scan_partiotions(It),[#results{ok_quorum=NumOk,fail_quorum=NumFail,indexes=PrefIndex}|Result],AllPrefLists++Preflist,
						  [Vnode|Included])
			end;
		_Preflist->
			{error,insufficient_vnodes}
	end.

execute(timeout, #state{preflist=Preflist,getquery=Query,bucket=Bucket,timeout=Timeout}=StateData) ->
	Ref = make_ref(),
	etsdb_vnode:get_query(Ref,Preflist,Bucket,Query),
	{next_state,wait_result, StateData#state{preflist=Preflist,req_ref=Ref},Timeout}.


wait_result({r,Index,ReqID,Res},#state{caller=Caller,vnode_results=Results,req_ref=ReqID,bucket=Bucket,timeout=Timeout,data=Data}=StateData) ->
	case results(Index, Res, Bucket,Results,Data,[]) of
		{error,Error}->
			reply_to_caller(Caller,{error,Error}),
			{stop,normal,StateData};
		{Data1,[]}->
			reply_to_caller(Caller,{ok,Data1}),
			{stop,normal,StateData};
		{Data1,Results1}->
			{next_state,wait_result, StateData#state{vnode_results=Results1,data=Data1},Timeout}
	end;

wait_result(timeout,#state{caller=Caller}=StateData) ->
	reply_to_caller(Caller,{error,timeout}),
    {stop,normal,StateData}.


handle_event(_Event, StateName, StateData) ->
    {next_state, StateName, StateData}.

handle_sync_event(_Event, _From, StateName, StateData) ->
    Reply = ok,
    {reply, Reply, StateName, StateData}.

handle_info(_Info, StateName, StateData) ->
    {next_state, StateName, StateData}.


terminate(_Reason, _StateName, _StatData) ->
    ok.

code_change(_OldVsn, StateName, StateData, _Extra) ->
    {ok, StateName, StateData}.

reply_to_caller({raw,Ref,To},Reply)->
	To ! {Ref,Reply}.

preflist(Partition,WVal)->
	etsdb_apl:get_apl(Partition,WVal).


results(_Index,_Res,_Bucket,[],Data,Acc)->
	{Data,Acc};
results(Index,Res,Bucket,[#results{indexes=Indexes}=Result|Results],Data,Acc)->
	case lists:member(Index,Indexes) of
		true->
			case add_result(Index,Res,Bucket,Result,Data,Acc) of
				{error,_}=Error->
					Error;
				{Data1,Acc1}->
					results(Index,ok,Bucket,Results,Data1,Acc1)
			end;
		_->
			results(Index,Res,Bucket,Results,Data,Acc)
	end.
add_result(Index,{ok,L},Bucket,Result,Data,Acc)->			
	L1 = Bucket:unserialize_result(L),
	Data1 = Bucket:join_scan(L1,Data),
	add_result(Index,ok,Bucket,Result,Data1,Acc);
add_result(_Index,ok,_Bucket,#results{num_ok=Count,ok_quorum=Quorum}=Result,Data,Acc)->		
	Count1 = Count+1,
	if
		Count1==Quorum->
			{Data,Acc};
		true->
			{Data,[Result#results{num_ok=Count1}|Acc]}
	end;
add_result(_Index,error,_Bucket,#results{num_fail=Count,fail_quorum=Quorum}=Result,Data,Acc)->
	Count1 = Count+1,
	if
		Count1==Quorum->
			{error,fail};
		true->
			{Data,[Result#results{num_fail=Count1}|Acc]}
	end;
add_result(Index,Res,Bucket,Result,Data,Acc)->
	lager:error("Cant' get data from ~p reason is ~p",[Index,Res]),
	add_result(Index,error,Bucket,Result,Data,Acc).
