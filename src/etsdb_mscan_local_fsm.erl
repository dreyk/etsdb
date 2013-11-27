%% Author: gunin
%% Created: Sep 5, 2013
%% Description: TODO: Add description to etsdb_mscan_fsm
-module(etsdb_mscan_local_fsm).

-behaviour(gen_fsm).

-export([start/0]).


-export([init/1, handle_event/3,
     handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-record(results,{num_ok=0,num_fail=0,ok_quorum=0,fail_quorum=0,indexes=[]}).
-record(state, {caller,caller_mon,preflist,getquery,timeout,req_ref,data}).

-include("etsdb_request.hrl").

start() ->
    Parent = self(),
    gen_fsm:start(?MODULE, [Parent], []).

%%{'DOWN', _MonitorRef, _Type, _Object, Info}
init([Parent]) ->
    Ref = erlang:monitor(process,Parent),
    {ok,ack,#state{caller=Parent,caller_mon=Ref}}.


ack({Preflist,Query,Timeout},StateData)->
    ok.
    
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

reply_to_caller({raw,Ref,To},Reply)->
    To ! {Ref,Reply}.