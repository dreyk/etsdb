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

%% @doc etsdb riak_core_vnode implementation
-module(etsdb_vnode).
-author('Alex G. <gunin@mail.mipt.ru>').

-export([start_vnode/1,
		 init/1,
		 handle_command/3,
		 handle_handoff_command/3,
		 handle_handoff_data/2,
		 handoff_cancelled/1,
		 handle_info/2,
		 handoff_finished/2,
		 handoff_starting/2,
		 encode_handoff_item/2,
		 terminate/2,
		 delete/1,
		 handle_coverage/4,
		 is_empty/1,
		 handle_exit/3,
		 put_internal/3,
		 put_external/4,
		 get_query/4]).

-behaviour(riak_core_vnode).

-include("etsdb_request.hrl").

%%vnode state
-record(state,{delete_mod,vnode_index,backend,backend_ref}).

%%Start vnode
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, etsdb_vnode).


put_internal(ReqID,Preflist,Data)->
	riak_core_vnode_master:command(Preflist,#etsdb_innerstore_req_v1{value=Data,req_id=ReqID},{fsm,undefined,self()},etsdb_vnode_master).

put_external(ReqID,Preflist,Bucket,Data)->
	riak_core_vnode_master:command(Preflist,#etsdb_store_req_v1{value=Data,req_id=ReqID,bucket=Bucket},{fsm,undefined,self()},etsdb_vnode_master).

get_query(ReqID,Preflist,Bucket,Query)->
	riak_core_vnode_master:command(Preflist,#etsdb_get_query_req_v1{get_query=Query,req_id=ReqID,bucket=Bucket},{fsm,undefined,self()},etsdb_vnode_master).

%%Init Callback.
init([Index]) ->
    DeleteMode = app_helper:get_env(etsdb, delete_mode, 3000),
	%%{BackEndModule,BackEndProps} = app_helper:get_env(etsdb, backend,{etsdb_ets_backend,[]}),
	{BackEndModule,BackEndProps} = app_helper:get_env(etsdb, backend,{etsdb_leveldb_backend,[{data_root,"./data/leveldb"}]}),
	%%Start storage backend
	case BackEndModule:init(Index,BackEndProps) of
		{ok,Ref}->
			RBuckets = app_helper:get_env(etsdb,registered_bucket),
			start_clear_buckets(RBuckets),
    		{ok,#state{vnode_index=Index,delete_mod=DeleteMode,backend=BackEndModule,backend_ref=Ref},[{pool,etsdb_vnode_worker, 10, []}]};
		{error,Else}->
			{error,Else}
	end.

start_clear_buckets([B|Tail])->
	lager:debug("start timer for ~p",[B]),
	riak_core_vnode:send_command_after(clear_period(B),{clear_db,B}),
	start_clear_buckets(Tail);
start_clear_buckets([])->
	ok.

handle_command({remove_expired,_,_}, _Sender,
			   #state{vnode_index=undefined}=State)->
	{noreply,State};
handle_command({remove_expired,Bucket,{expired_records,{0,_Records}}}, _Sender,
			   #state{vnode_index=Index}=State)->
	lager:info("nothing to delete from ~p on ~p",[Bucket,Index]),
	riak_core_vnode:send_command_after(clear_period(Bucket),{clear_db,Bucket}),
	{noreply,State};
handle_command({remove_expired,Bucket,{expired_records,{Count,Records}}}, _Sender,
			   #state{backend=BackEndModule,backend_ref=BackEndRef,vnode_index=Index}=State)->
	lager:info("prepare delete ~p records on ~p",[Count,Index]),
	ToDelete = lists:usort(Records),
	case BackEndModule:delete(Bucket,ToDelete,BackEndRef) of
		{ok,NewBackEndRef}->
			ok;
		{error,Reason,NewBackEndRef}->
			lager:error("Can't delete old records ~p on ~p",[Reason,Index])
	end,
	case Count of
		{continue,_}->
			riak_core_vnode:send_command_after(1000,{clear_db,Bucket});
		_->
			riak_core_vnode:send_command_after(clear_period(Bucket),{clear_db,Bucket})
	end,
	{noreply,State#state{backend_ref=NewBackEndRef}};
handle_command({remove_expired,Bucket,Error}, _Sender,#state{vnode_index=Index}=State)->
	lager:error("Find expired task failed ~p on ~p",[Error,Index]),
	riak_core_vnode:send_command_after(clear_period(Bucket),{clear_db,Bucket}),
	{noreply, State};

handle_command({clear_db,_}, _Sender,
			   #state{vnode_index=undefined}=State)->
	{noreply, State};
handle_command({clear_db,Bucket}, Sender,
			   #state{backend=BackEndModule,backend_ref=BackEndRef,vnode_index=Index}=State)->
	Me = self(),
	case BackEndModule:find_expired(Bucket,BackEndRef) of
		{async, AsyncWork} ->
			Fun = fun()->
						  riak_core_vnode:send_command(Me,{remove_expired,Bucket,AsyncWork()}) end,
			{async,{clear_db,Fun},Sender, State};
		Else->
			lager:error("Can't create clear db task ~p on ~p",[Else,Index]),
			{noreply, State}
	end;

%%Receive command to store data in user format.
handle_command(?ETSDB_STORE_REQ{bucket=Bucket,value=Value,req_id=ReqID}, Sender,
			   #state{backend=BackEndModule,backend_ref=BackEndRef,vnode_index=Index}=State)->
	case BackEndModule:save(Bucket,Value,BackEndRef) of
		{Result,NewBackEndRef}->
			riak_core_vnode:reply(Sender, {w,Index,ReqID,Result});
		{error,Reason,NewBackEndRef}->
			riak_core_vnode:reply(Sender, {w,Index,ReqID,{error,Reason}})
	end,
	{noreply,State#state{backend_ref=NewBackEndRef}};

handle_command(?ETSDB_GET_QUERY_REQ{bucket=Bucket,get_query=Query,req_id=ReqID}, Sender,
			   #state{backend=BackEndModule,backend_ref=BackEndRef,vnode_index=Index}=State)->
	 case do_get_qyery(BackEndModule,BackEndRef,Bucket,Query) of
        {async, AsyncWork} ->
			Fun =
				fun()->
						{r,Index,ReqID,AsyncWork()} end,
            {async, {invoke,Fun},Sender, State};
        Result->
			riak_core_vnode:reply(Sender, {r,Index,ReqID,Result}),
            {noreply, State}
    end.
	

handle_info(timeout,State)->
    lager:debug("receive timeout ~p",[State]),
    {ok,State};

handle_info(Info,State)->
    lager:debug("receive info ~p",[{Info,State}]),
    {ok,State}.

handle_handoff_command(Message, _Sender, State) ->
    lager:debug("receive handoff ~p",[Message]),
    {noreply, State}.

handoff_starting(TargetNode, State) ->
	lager:debug("handof stating ~p",[TargetNode]),
    {true, State}.

handoff_cancelled(State) ->
	lager:debug("handof canceled ~p",[State]),
    {ok, State}.

handoff_finished(TargetNode, State) ->
	lager:debug("handof finished ~p",[TargetNode]),
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(ObjectName,ObjectValue) ->
    term_to_binary({ObjectName,ObjectValue}).

delete(#state{backend=BackEndModule,backend_ref=BackEndRef}=State)->
	case BackEndModule:drop(BackEndRef) of
        {ok,NewBackEndRef} ->
            ok;
        {error, Reason, NewBackEndRef} ->
            lager:error("Failed to drop ~p. Reason: ~p~n", [BackEndModule, Reason]),
            ok
    end,
    {ok, State#state{backend_ref=NewBackEndRef,vnode_index=undefined}}.


handle_coverage(_Request, _KeySpaces, _Sender, ModState)->
   {noreply,ModState}.

is_empty(State)->
    {true,State}.

handle_exit(_Pid,_Reason,State)->
	{noreply,State}.
%%------------------------------------------
%% Terminate vnode process.
%% Try terminate all child(user process) like supervisor
%%------------------------------------------
terminate(Reason,#state{backend=BackEndModule,backend_ref=BackEndRef}=State)->
	lager:info("etsdb vnode terminated in state ~p reason ~p",[State,Reason]),
	BackEndModule:stop(BackEndRef),
	ok;
terminate(Reason,State)->
	lager:info("etsdb vnode terminated in state ~p reason ~p",[State,Reason]),
    ok.

%% ===================================================================
%% Internal functions
%% ===================================================================

do_get_qyery(BackEndModule,BackEndRef,Bucket,{scan,From,To})->
	BackEndModule:scan(Bucket,From,To,[],BackEndRef);
do_get_qyery(BackEndModule,BackEndRef,Bucket,{scan,From,To,Acc})->
	BackEndModule:scan(Bucket,From,To,Acc,BackEndRef);
do_get_qyery(_BackEndModule,BackEndRef,_Bucket,_Query)->
	{{error,bad_query},BackEndRef}.

clear_period(Bucket)->
	I = Bucket:clear_period(),
	I+etsdb_util:random_int(I).