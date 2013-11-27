%% Author: gunin
%% Created: Sep 5, 2013
%% Description: TODO: Add description to etsdb_mscan_fsm
-module(etsdb_mscan_local_fsm).

-behaviour(gen_fsm).

-export([start/0]).


-export([init/1, handle_event/3,wait_result/2,ack/2,
     handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-record(state, {caller,caller_mon,count,ack_index,getquery,timeout,req_ref,data}).

-include("etsdb_request.hrl").

start() ->
    Parent = self(),
    gen_fsm:start(?MODULE, [Parent], []).

%%{'DOWN', _MonitorRef, _Type, _Object, Info}
init([{Parent,_Ref}=Caller]) ->
    Ref = erlang:monitor(process,Parent),
    {ok,ack,#state{caller=Caller,caller_mon=Ref}}.


ack({Preflist,Bucket,Query,Timeout},StateData)->
    Ref = make_ref(),
    etsdb_vnode:get_query(Ref,Preflist,Bucket,Query),
    {next_state,wait_result,StateData#state{count=length(Preflist),ack_index=[],data=[],req_ref=Ref,timeout=Timeout},StateData#state.timeout}.

wait_result(timeout,#state{caller=Caller,count=Count,ack_index=AckIndex,data=Data}=StateData) ->
    lager:error("timeout wait response from ~p partitions",[Count]),
    reply_to_caller(Caller,{timeout,AckIndex,Data}),
    {stop,normal,StateData#state{data=undefined}};
wait_result({r,Index,ReqID,Res},#state{caller=Caller,count=Count,req_ref=ReqID,ack_index=AckIndex,data=Data}=StateData) ->
    NewCount = Count-1,
    case Res of
        {ok,ResultData}->
            NewData = orddict:merge(fun(_,V1,_V2)->V1 end,lists:ukeysort(1,ResultData),Data),
            NewAckIndex = [{Index,ok}|AckIndex];
        Else->
            lager:error("Error scan partiotion ~p - ~p",[Index,Else]),
            NewData = Data,
            NewAckIndex = [{Index,error}|AckIndex]
    end,
    if
        NewCount==0->
            reply_to_caller(Caller,{ok,NewAckIndex,NewData}),
            {stop,normal,StateData#state{data=undefined}};
        true->
            {next_state,wait_result,StateData#state{ack_index=NewAckIndex,count=NewCount,data=NewData},StateData#state.timeout}
    end.

handle_event(_Event, StateName, StateData) ->
    {next_state, StateName, StateData}.

handle_sync_event(_Event, _From, StateName, StateData) ->
    Reply = ok,
    {reply, Reply, StateName, StateData}.

handle_info({'DOWN',MonitorRef, _Type, _Object, Info},StateName, #state{caller_mon=MonitorRef}=StateData) ->
    lager:warning("Master process failed state ~p reason ~p",[StateName,Info]),
    {stop,normal,StateData#state{data=undefined}};
handle_info(_Info, StateName, StateData) ->
    {next_state, StateName, StateData}.


terminate(_Reason, _StateName, _StatData) ->
    ok.

code_change(_OldVsn, StateName, StateData, _Extra) ->
    {ok, StateName, StateData}.

reply_to_caller({Name,Ref},Reply)->
    gen_fsm:send_event(Name,{local_scan,Ref,Reply}).