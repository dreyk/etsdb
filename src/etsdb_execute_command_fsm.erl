%%%-------------------------------------------------------------------
%%% @author lol4t0
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 01. мар 2015 22:06
%%%-------------------------------------------------------------------
-module(etsdb_execute_command_fsm).
-author("lol4t0").

-behaviour(gen_fsm).


%%API
-export([start_link/3]).


-export([init/1, execute/2,wait_result/2, handle_event/3, handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-record(state, {caller,command,timeout,req_ref}).

-type command() :: {Fun :: fun(), Args :: [Vnode :: non_neg_integer() | [any()]]}.
-type caller_id() :: {raw, reference(), pid()}.
-spec start_link(Caller :: caller_id(), Command :: command(), Timeout :: pos_integer()) -> gen:start_ret().
start_link(Caller, CommandFA,Timeout) ->
    gen_fsm:start_link(?MODULE, [Caller, CommandFA,Timeout], []).

init([Caller, CommandFA, Timeout]) ->
    {ok, execute, #state{caller=Caller, command= CommandFA,timeout=Timeout}, 0}.

execute(timeout, #state{command = {F, A}, timeout=Timeout}=StateData) ->
    ReqId = make_ref(),
    erlang:apply(etsdb_vnode, F, [ReqId | A]),
    {next_state,wait_result, StateData#state{req_ref=ReqId},Timeout}.

wait_result({e,_Index,ReqID,Res0},#state{caller=Caller, req_ref=ReqID}=StateData) ->
    reply_to_caller(Caller, Res0),
    {stop, normal,StateData};

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

