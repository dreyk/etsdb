%% Author: gunin
%% Created: Nov 28, 2013
%% Description: TODO: Add description to etsdb_all_scan
-module(etsdb_mscan_all_fsm).

-behaviour(gen_fsm).

-export([start_link/4]).


-export([init/1, prepare/2,execute/2,wait_result/2, handle_event/3,
     handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-record(state, {caller,preflist,getquery,timeout,bucket,local_scaners,req_ref,data}).

-include("etsdb_request.hrl").

start_link(Caller,Bucket,Query,Timeout) ->
    gen_fsm:start_link(?MODULE, [Caller,Bucket,Query,Timeout], []).

init([Caller,Bucket,Query,Timeout]) ->
    {ok,prepare, #state{caller=Caller,getquery=Query,bucket=Bucket,timeout=Timeout},0}.


prepare(timeout, #state{caller=Caller}=StateData) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    AllOwners = riak_core_ring:all_owners(Ring),
    case pscan(AllOwners,[]) of
        {error,Error}->
            reply_to_caller(Caller,{error,Error}),
            {stop,normal,StateData};
        ReqNodes->
            {next_state,execute,StateData#state{preflist=ReqNodes,data=[]},0}
    end.
pscan([],Requests)->
    etsdb_util:reduce_orddict(fun merge_req_by_node/2,lists:keysort(1,Requests));
pscan([{Index,_}|Tail],Requests)->
   PartitionIdx = etsdb_util:hash_for_partition(Index),
   case preflist(PartitionIdx,1) of
        {error,Error}->
            {error,Error};
        [{ReqIndex,ReqNode}]->
            pscan(Tail,[{ReqNode,{ReqIndex,ReqNode}}|Requests]);
        _Preflist->
            {error,insufficient_vnodes}
    end.

merge_req_by_node(Data,'$start')->
    [Data];
merge_req_by_node('$end',Acc)->
    Acc;
merge_req_by_node(Data,Acc)->
    [Data|Acc].

execute(timeout, #state{preflist=Preflist,getquery=Query,bucket=Bucket,timeout=Timeout,caller=Caller}=StateData) ->
    Ref = make_ref(),
    case start_local_fsm(Preflist,Ref,Query,Bucket,Timeout,[]) of
        {error,Error,Started}->
            reply_to_caller(Caller,{error,Error}),
            stop_started(Started),
            {stop,Error,StateData};
        Started->
            {next_state,wait_result, StateData#state{preflist=Preflist,req_ref=Ref,local_scaners=Started},Timeout}
    end.

start_local_fsm([],_Ref,_Query,_Bucket,_Timeout,Monitors)->
    Monitors;
start_local_fsm([{Node,PrefList}|Tail],Ref,Query,Bucket,Timeout,Monitors)->
    case etsdb_mscan_local_fsm:start(Ref,Node) of
        {ok,Pid}->
             MRef = erlang:monitor(process,Pid),
             etsdb_mscan_local_fsm:ack(Pid, PrefList, Bucket, Query, Timeout),
             start_local_fsm(Tail,Ref,Query,Bucket,Timeout,[{MRef,Pid}|Monitors]);
        Else->
            lager:error("Can'r start local scan on ~p reason ~p",[Node,Else]),
            {error,insufficient_nodes,Monitors}
    end.

stop_started(Started)->
    lists:foreach(fun({_,Pid})->
                          etsdb_mscan_local_fsm:stop(Pid) end,Started).
wait_result({local_scan,ReqID,From,{ok,_AckIndex,LocalData}},#state{caller=Caller,local_scaners=Scaners,req_ref=ReqID,data=Data}=StateData) ->
    NewScaners = lists:keydelete(From, 2,Scaners),
    NewData = orddict:merge(fun(_,V1,_V2)->V1 end,LocalData,Data),
    case NewScaners of
        []->
            reply_to_caller(Caller,{ok,NewData}),
            {stop,normal,StateData#state{data=undefined,local_scaners=[]}};
        _->
             {next_state,wait_result, StateData#state{local_scaners=NewScaners,data=NewData},StateData#state.timeout}
    end;
wait_result({local_scan,ReqID,From,{error,Error}},#state{caller=Caller,req_ref=ReqID,local_scaners=Scaners}=StateData) ->
    lager:error("fail scan on ~p reason ~p",[node(From),Error]),
    NewScaners = lists:keydelete(From, 2,Scaners),
    reply_to_caller(Caller,{error,timeout}),
    stop_started(NewScaners),
    {stop,normal,StateData#state{data=undefined,local_scaners=[]}}.


handle_event(_Event, StateName, StateData) ->
    {next_state, StateName, StateData}.

handle_sync_event(_Event, _From, StateName, StateData) ->
    Reply = ok,
    {reply, Reply, StateName, StateData}.

handle_info({'DOWN',MonitorRef, _Type, Object, Info}, StateName, #state{caller=Caller,local_scaners=LocalScaners}=StateData) ->
    Node = if
               is_pid(Object)->
                   erlang:node(Object);
               true->
                   Object
           end,
    lager:error("fail scan on ~p reason ~p",[Node,Info]),
    case lists:keymember(MonitorRef,1,LocalScaners) of
        true->
            NewScaners = lists:keydelete(MonitorRef, 1,LocalScaners),
            reply_to_caller(Caller,{error,local_failed}),
            stop_started(NewScaners),
            {stop,normal,StateData#state{data=undefined,local_scaners=[]}};
        _->
            {next_state, StateName, StateData}
    end;
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
