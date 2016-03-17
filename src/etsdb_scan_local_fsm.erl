%% Author: gunin
%% Created: Nov 28, 2013
%% Description: TODO: Add description to etsdb_scan_local_fsm
-module(etsdb_scan_local_fsm).

-behaviour(gen_fsm).

-export([start/2, ack/3, stop/1,ack/4]).


-export([init/1, handle_event/3, wait_result/2, ack/2,
    handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-record(state, {caller, caller_mon, count, to_ack, ack_index, scan, timeout, req_ref, data}).

-include("etsdb_request.hrl").

start(Ref, Node) ->
    Parent = {self(), Ref},
    rpc:call(Node, gen_fsm, start, [?MODULE, [Parent], []]).

stop(Name) ->
    gen_fsm:send_all_state_event(Name, stop).

ack(Pid, Scan,Stream, Timeout) ->
    gen_fsm:send_event(Pid, {Scan,Stream,Timeout}).
ack(Pid, Scan, Timeout) ->
    gen_fsm:send_event(Pid, {Scan, Timeout}).

%%{'DOWN', _MonitorRef, _Type, _Object, Info}
init([{Parent, _Ref} = Caller]) ->
    Ref = erlang:monitor(process, Parent),
    {ok, ack, #state{caller = Caller, caller_mon = Ref}}.

ack({Scan, Timeout}, StateData) ->
    ack({Scan, undefined, Timeout}, StateData);
ack({Scan, Stream, Timeout}, StateData) ->
    Ref = make_ref(),
    {Count, AckIndex} = lists:foldl(fun({VNode, Scans}, {CountAcc, RefAcc}) ->
        {RefAcc2, Scans1} = lists:foldl(fun({ScanRef, Scan1}, {RefAcc1, ScanAcc}) ->
            {[ScanRef | RefAcc1], [Scan1 | ScanAcc]}
                                        end, {[], []}, Scans),
        case Stream of
            undefined ->
                etsdb_vnode:scan(Ref, VNode, Scans1);
            _ ->
                etsdb_vnode:stream(Ref, VNode, Scans1, Stream)
        end,
        {CountAcc + 1, [{VNode, RefAcc2} | RefAcc]}
                                    end, {0, []}, Scan#scan_req.pscan),
    {next_state,
        wait_result,
        StateData#state{count = Count,
            scan = Scan#scan_req{pscan = undefined}, to_ack = [], ack_index = lists:ukeysort(1, AckIndex), data = [], req_ref = Ref, timeout = Timeout},
        Timeout}.

wait_result(timeout, #state{caller = Caller, count = Count, to_ack = ToAck, ack_index = AckIndex, data = Data} = StateData) ->
    lager:error("Timeout wait response from ~p partitions", [Count]),
    NewToAck = lists:foldl(fun({_, RefToAcc}, Acc) ->
        lists:foldl(fun(ScanRef, Acc1) -> [{ScanRef, error} | Acc1] end, Acc, RefToAcc)
                           end, ToAck, AckIndex),
    reply_to_caller(Caller, {error, timeout}, NewToAck, Data),
    {stop, normal, clear_state(StateData)};
wait_result({r, Index, ReqID, Result}, #state{caller = Caller, scan = Scan, count = Count, req_ref = ReqID, to_ack = ToAck, ack_index = AckIndex, data = Data} = StateData) ->
    NewCount = Count - 1,
    {RefToAcc, NewAckIndex} = case orddict:find(Index, AckIndex) of
                                  {ok, List} ->
                                      {List, orddict:erase(Index, AckIndex)};
                                  _ ->
                                      {[], AckIndex}
                              end,
    case Result of
        {ok, ResultData} ->
            NewData = etsdb_scan_master_fsm:join_data(Scan#scan_req.join_fun, final_data(Scan#scan_req.end_fun, ResultData), Data),
            NewToAck = lists:foldl(fun(ScanRef, Acc) -> [{ScanRef, ok} | Acc] end, ToAck, RefToAcc);
        Else ->
            lager:error("Error scan partiotion ~p - ~p", [Index, Else]),
            NewData = Data,
            NewToAck = lists:foldl(fun(ScanRef, Acc) -> [{ScanRef, error} | Acc] end, ToAck, RefToAcc)
    end,
    if
        NewCount == 0 ->
            reply_to_caller(Caller, NewToAck, NewData),
            {stop, normal, clear_state(StateData)};
        true ->
            {next_state, wait_result, StateData#state{to_ack = NewToAck, ack_index = NewAckIndex, count = NewCount, data = NewData}, StateData#state.timeout}
    end.

handle_event(stop, _StateName, StateData) ->
    {stop, normal, clear_state(StateData)};
handle_event(_Event, StateName, StateData) ->
    {next_state, StateName, StateData}.

handle_sync_event(_Event, _From, StateName, StateData) ->
    Reply = ok,
    {reply, Reply, StateName, StateData}.

handle_info({'DOWN', MonitorRef, _Type, _Object, Info}, StateName, #state{caller_mon = MonitorRef} = StateData) ->
    lager:warning("Master process failed state ~p reason ~p", [StateName, Info]),
    {stop, normal, clear_state(StateData)};
handle_info(_Info, StateName, StateData) ->
    {next_state, StateName, StateData}.


terminate(_Reason, _StateName, _StatData) ->
    ok.

code_change(_OldVsn, StateName, StateData, _Extra) ->
    {ok, StateName, StateData}.

reply_to_caller({Name, Ref}, Error, Ack, LocalData) ->
    gen_fsm:send_event(Name, {local_scan, Ref, self(), Error, Ack, LocalData}).
reply_to_caller({Name, Ref}, Ack, LocalData) ->
    gen_fsm:send_event(Name, {local_scan, Ref, self(), Ack, LocalData}).

clear_state(State) ->
    State#state{data = undefined, ack_index = undefined, scan = undefined}.

final_data({M, F, A}, Data) ->
    apply(M, F, [Data | A]);
final_data(Fun, Data) when is_function(Data) ->
    Fun(Data);
final_data(_Fun, Data) ->
    Data.