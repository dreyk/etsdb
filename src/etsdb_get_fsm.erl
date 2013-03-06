%% -------------------------------------------------------------------
%%
%%
%% Copyright (c) Dreyk.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
-module(etsdb_get_fsm).

-behaviour(gen_fsm).

-export([start_link/5]).


-export([init/1, execute/2,wait_result/2,prepare/2, handle_event/3,
	 handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-record(state, {caller,preflist,partition,getquery,data,timeout,bucket,results,req_ref}).

start_link(Caller,Partition,Bucket,Query,Timeout) ->
    gen_fsm:start_link(?MODULE, [Caller,Partition,Bucket,Query,Timeout], []).

init([Caller,Partition,Bucket,Query,Timeout]) ->
    {ok,prepare, #state{caller=Caller,partition=Partition,getquery=Query,data=[],bucket=Bucket,timeout=Timeout},0}.


prepare(timeout, #state{caller=Caller,partition=Partition,bucket=Bucket}=StateData) ->
	case preflist(Partition,Bucket:r_val()) of
		{error,Error}->
			reply_to_caller(Caller,{error,Error}),
			{stop,normal,StateData};
		Preflist->
			{next_state,execute,StateData#state{preflist=Preflist},0}
	end.
execute(timeout, #state{preflist=Preflist,getquery=Query,bucket=Bucket,timeout=Timeout}=StateData) ->
	Ref = make_ref(),
	etsdb_vnode:get_query(Ref,Preflist,Bucket,Query),
    {next_state,wait_result, StateData#state{results=Bucket:quorum(),req_ref=Ref},Timeout}.


wait_result({r,_Index,ReqID,Data},#state{caller=Caller,results=1,req_ref=ReqID,data=Acc}=StateData) ->
	Acc1 = case Data of
			   {ok,L}->
				   join_data(L, Acc);
			   _->
				   Acc
		   end,
	reply_to_caller(Caller,{ok,Acc1}),
    {stop,normal,StateData};
wait_result({r,_Index,ReqID,Data},#state{results=Count,timeout=Timeout,req_ref=ReqID,data=Acc}=StateData) ->
	Acc1 = case Data of
			   {ok,L}->
				   join_data(L, Acc);
			   _->
				   Acc
		   end,
    {next_state,wait_result, StateData#state{results=Count-1,data=Acc1},Timeout};
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


join_data(D,Acc) when is_list(D)->
	Acc++D;
join_data(_,Acc)->
	Acc.
