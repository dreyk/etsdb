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

-module(etsdb_dump_fsm).
-author("dreyk").

-behaviour(gen_fsm).

-export([start_link/5]).


-export([init/1, execute/2,wait_result/2,prepare/2, handle_event/3,
    handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-record(state, {result_hadler,preflist,param,dump_name,timeout,bucket,results,rcount,req_ref}).

start_link(ResultHandler,Bucket,DumpName,Param,Timeout) ->
    gen_fsm:start_link(?MODULE, [ResultHandler,Bucket,DumpName,Param,Timeout], []).

init([ResultHandler,Bucket,DumpName,Param,Timeout]) ->
    {ok,prepare, #state{result_hadler = ResultHandler,param = Param,bucket=Bucket,timeout=Timeout,dump_name = DumpName},0}.


prepare(timeout, #state{bucket = Bucket}=StateData) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    AllOwners = riak_core_ring:all_owners(Ring),
    UpNodes = ordsets:from_list(riak_core_node_watcher:nodes(etsdb)),
    {PrefList,Res}=lists:foldl(fun({I,N},{PL,RAcc})->
        case ordsets:is_element(N,UpNodes) of
            true->
                {[{I,N}|PL],RAcc};
            _->
                {PL,[{{I,N},down}|RAcc]}
        end end,{[],[]},AllOwners),
    lager:info("prepare dump ~p",[{Bucket,PrefList}]),
    {next_state,execute,StateData#state{preflist=PrefList,results=Res,rcount = length(PrefList)},0}.

execute(timeout, #state{preflist=Preflist,dump_name = File,param = Param,bucket=Bucket,timeout=Timeout}=StateData) ->
    Ref = make_ref(),
    etsdb_vnode:dump_to(Ref,Preflist,Bucket,File,Param),
    {next_state,wait_result, StateData#state{req_ref=Ref},Timeout}.


wait_result({r,VNode,ReqID,InvokeRes},#state{result_hadler = ResaultHandler,results=Results,req_ref=ReqID,timeout=Timeout,rcount = C1}=StateData) ->
    C2 = C1-1,
    if
        C2==0->
            reply_to_caller(ResaultHandler,[{VNode,InvokeRes}|Results]),
            {stop,normal,StateData};
        true->
            {next_state,wait_result, StateData#state{results=[{VNode,InvokeRes}|Results],rcount = C2},Timeout}
    end;
wait_result(timeout,#state{result_hadler = ResaultHandler}=StateData) ->
    reply_to_caller(ResaultHandler,{error,timeout}),
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

reply_to_caller(Fun,Reply) when is_function(Fun)->
    Fun(Reply);
reply_to_caller({raw,Ref,To},Reply)->
    To ! {Ref,Reply}.
