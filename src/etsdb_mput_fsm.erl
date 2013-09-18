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

-module(etsdb_mput_fsm).

-behaviour(gen_fsm).

-export([start_link/4]).


-export([init/1, execute/2,wait_result/2,prepare/2, handle_event/3,
     handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-record(results,{num_ok=0,num_fail=0,ok_quorum=0,fail_quorum=0,indexes}).
-record(state, {caller,data,timeout,bucket,results,req_ref}).

start_link(Caller,Bucket,Data,Timeout) ->
    gen_fsm:start_link(?MODULE, [Caller,Bucket,Data,Timeout], []).

init([Caller,Bucket,Data,Timeout]) ->
    {ok,prepare, #state{caller=Caller,bucket=Bucket,timeout=Timeout,data=Data},0}.

pwrite(_Bucket,_Ring,_UpNodes,[],Results,ToSave)->
    {ToSave,Results};
pwrite(Bucket,Ring,UpNodes,[{Partition,Data}|Datas],Results,ToSave)->
    WriteCount = Bucket:w_val(),
    case preflist(Partition,WriteCount) of
        {error,Error}->
            {error,Error};
        Preflist when length(Preflist)==WriteCount->
            NumOk = Bucket:w_quorum(),
            NumFail = WriteCount-NumOk+1,
            ToSave1 = join_save_batch(Preflist,ToSave,Data),
            PrefIndex = [Index||{Index,_}<-Preflist],
            pwrite(Bucket,Ring,UpNodes,Datas,[#results{ok_quorum=NumOk,fail_quorum=NumFail,indexes=PrefIndex}|Results],ToSave1);
        _Preflist->
            {error,insufficient_vnodes}
    end.

join_save_batch(Preflist,ToSave,Data)->
    ToSave1 = [{VNode,Data}||VNode<-lists:sort(Preflist)],
    orddict:merge(fun(_,Data1,Data2)->
                          orddict:merge(fun(_,V1,_)->
                                               V1 end,Data1,Data2)
                  end,ToSave,ToSave1).

prepare(timeout, #state{caller=Caller,data=Data,bucket=Bucket}=StateData) ->
    {ok,Ring} = riak_core_ring_manager:get_my_ring(),
    UpNodes = riak_core_node_watcher:nodes(etsdb),
    case pwrite(Bucket, Ring, UpNodes,Data,[],[]) of
        {error,Error}->
            reply_to_caller(Caller,{error,Error}),
            {stop,normal,StateData};
        {ToSave,Results}->
            {next_state,execute,StateData#state{results=Results,data=ToSave},0}
    end.
execute(timeout, #state{data=Data,bucket=Bucket,timeout=Timeout}=StateData) ->
    Ref = make_ref(),
    lists:foreach(fun({VNode,VNodeData})->
                          PutBatch = Bucket:serialize(VNodeData),
                          etsdb_vnode:put_external(Ref,[VNode],Bucket,PutBatch)
                          end,Data),
    {next_state,wait_result, StateData#state{data=undefined,req_ref=Ref},Timeout}.


wait_result({w,Index,ReqID,Res},#state{caller=Caller,results=Results,req_ref=ReqID,timeout=Timeout}=StateData) ->
    case Res of
        ok->
            ok;
        _->
            lager:error("Can't save data to ~p reason ~p",[Index,Res])
    end,
    case results(Index, Res,Results,[]) of
        {error,Error}->
            reply_to_caller(Caller,{error,Error}),
            {stop,normal,StateData#state{results=undefined,req_ref=undefined}};
        []->
            reply_to_caller(Caller,ok),
            {stop,normal,StateData#state{results=undefined,req_ref=undefined}};
        NewResults->
            {next_state,wait_result, StateData#state{results=NewResults},Timeout}
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

results(_Index,_Res,[],Acc)->
    Acc;
results(Index,Res,[#results{indexes=Indexes}=Result|Results],Acc)->
    case lists:member(Index,Indexes) of
        true->
            case add_result(Res,Result,Acc) of
                {error,_}=Error->
                    Error;
                Acc1->
                    results(Index,Res,Results,Acc1)
            end;
        _->
            results(Index,Res,Results,[Result|Acc])
    end.
add_result(ok,#results{num_ok=Count,ok_quorum=Quorum}=Result,Acc)->
    Count1 = Count+1,
    if
        Count1==Quorum->
            Acc;
        true->
            [Result#results{num_ok=Count1}|Acc]
    end;
add_result(_Res,#results{num_fail=Count,fail_quorum=Quorum}=Result,Acc)->
    Count1 = Count+1,
    if
        Count1==Quorum->
            {error,fail};
        true->
            [Result#results{num_fail=Count1}|Acc]
    end.
