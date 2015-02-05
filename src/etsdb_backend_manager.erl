%%%-------------------------------------------------------------------
%%% @author lol4t0
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Jan 2015 9:08
%%%-------------------------------------------------------------------
-module(etsdb_backend_manager).
-author("lol4t0").

-behaviour(gen_server).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/1, add/6, acquire/2, release/3, list_backends/1, drop_partition/1]).

%% Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-type backend_key() :: {Start::non_neg_integer(), End::non_neg_integer()}.

-record(backend_info, {
    key :: {StartTimestamp :: non_neg_integer(), Partition :: term()},
    partition :: term(),
    start_timestamp :: non_neg_integer(),
    end_timestamp :: non_neg_integer(),
    path :: string(),
    backend_state = undefined,
    last_accessed = undefined,
    ref_count = 0,
    opts :: proplists:proplist(),
    module :: module(),
    owners :: dict:dict(pid(), reference())
}).
-record(state, {
    backends_table,
    max_loaded_backends::pos_integer(),
    current_loaded_backends = 0::non_neg_integer(),
    wait_queue = [] :: [{Key ::{From :: non_neg_integer(), Partition :: non_neg_integer()}, Ref :: term(), RequesterPid :: pid()}]
}).


%% API IMPLEMENTATION

-spec start_link(proplists:proplist()) -> {'ok', pid()} | 'ignore' | {'error', term()}.
start_link(Config) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Config, []).

-spec add(term(), string(), non_neg_integer(), non_neg_integer(), proplists:proplist(), module()) -> ok.
add(Partition, Path, From, To, Opts, Module) ->
    gen_server:call(?MODULE, {add, mk_key(#backend_info{partition = Partition, path = Path,
        start_timestamp = From, end_timestamp = To, opts = Opts, module = Module, owners = dict:new()})}).

-spec acquire(term(), backend_key()) ->
    {ok, BackendRef :: term()} | {busy, WaitObject :: term()} | {error, Reason :: term()}.
acquire(Partition, Key) ->
    gen_server:call(?MODULE, {acquire, Partition, Key}).

-spec release(term(), backend_key(), term()) -> ok.
release(Partition, Key, NewState) ->
    Caller = self(),
    gen_server:cast(?MODULE, {release, Partition, Key, NewState, Caller}),
    ok.

-spec list_backends(term()) -> [backend_key()].
list_backends(Partition) ->
    gen_server:call(?MODULE, {list_backends, Partition}).

drop_partition(Partition) ->
    gen_server:call(?MODULE, {drop_partition, Partition}).


%% CALLBACKS IMPLEMENTATION

init(Config)->
    MaxLoadedBackends = etsdb_util:propfind(max_loaded_backends, Config, 2),
    {ok, #state{
        max_loaded_backends = MaxLoadedBackends,
        backends_table = ets:new(undefined, [ordered_set, private, {keypos, #backend_info.key}])
    }}.

handle_call({add, BackendInfo}, _From, State = #state{backends_table = Tab}) ->
    Created = ets:insert_new(Tab, BackendInfo),
    {reply, Created, State};
handle_call({acquire, Partition, Key}, {From, _Tag}, State) ->
    process_acquire_results(load_backend(Partition, Key, State, From));
handle_call({list_backends, Partition}, _Form, State) ->
    {reply, list_backend_keys(Partition, State), State};
handle_call({drop_partition, Partition}, _From, State = #state{backends_table = Tab, current_loaded_backends = CurrLoaded}) ->
    Backends = list_active_backends_refs(Partition, State),
    ets:match_delete(Tab, #backend_info{_='_', partition = Partition}),
    TotalStopped = lists:foldl(
        fun
            ({BackendState, Mod}, StoppedCnt) ->
                ok = Mod:stop(BackendState),
                StoppedCnt + 1
        end,
        0, Backends),
    {reply, ok, State#state{current_loaded_backends = CurrLoaded - TotalStopped}}.

handle_cast({release, Partition, {From, _To}, NewBackendRef, Caller}, State = #state{backends_table = Tab}) ->
    Key = {From, Partition},
    I = ets:lookup(Tab, Key),
    case I of
        [#backend_info{ref_count = Cnt, owners = Owners}] ->
            NewCnt = Cnt - 1,
            case dict:find(Caller, Owners) of
                {ok, MonitorRef} ->
                    erlang:demonitor(MonitorRef, [flush]);
                error ->
                    ok
            end,
            Upd0 = [{#backend_info.ref_count, NewCnt}, {#backend_info.owners, dict:erase(Caller, Owners)}],
            Upd = if
                      NewBackendRef =/= undefined ->
                          [{#backend_info.backend_state, NewBackendRef} | Upd0];
                      true ->
                          Upd0
                  end,
            ets:update_element(Tab, Key, Upd),
            NewState = if
                           NewBackendRef == 'delete_backend' ->
                               ets:delete(Tab, Key),
                               CurrLoaded = State#state.current_loaded_backends,
                               UpdState = State#state{current_loaded_backends = CurrLoaded - 1},
                               load_waiting_backends(UpdState);
                           NewCnt == 0 ->
                               load_waiting_backends(State);
                           true ->
                               State
                       end,
            {noreply, NewState};
        [] ->
            {noreply, State}
    end.
    
handle_info({'DOWN', _MonitorRef, process, Pid, _Info}, State = #state{backends_table = Tab, wait_queue = WaitQueue}) ->
    Result = ets:foldl(
        fun(#backend_info{owners = Owners, key = Key, ref_count = RefCnt}, Result) ->
            case dict:find(Pid, Owners) of
                {ok, _} ->
                    ets:update_element(Tab, Key, [
                        {#backend_info.owners, dict:erase(Pid, Owners)},
                        {#backend_info.ref_count, RefCnt - 1}
                        ]),
                    lager:warning("Process ~p terminated while having backend ~p acquired", [Pid, Key]),
                    true;
                error ->
                    Result
            end
        end, false, Tab),
    NewWaitQueue = lists:filter(fun({_Key, _Ref, ReqPid}) -> ReqPid /= Pid end, WaitQueue),
    NewState = if
        Result ->
            load_waiting_backends(State#state{wait_queue = NewWaitQueue});
        true ->
            State
    end,
    {noreply, NewState}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% PRIVATE FUNCTIONS

list_backend_keys(Partition, #state{backends_table = Tab}) ->
    Spec = [{
        #backend_info{_ = '_', partition = '$2', start_timestamp = '$3', end_timestamp = '$4'},
        [{'=:=', '$2', Partition}],
        [{{'$3', '$4'}}]
    }],
    ets:select(Tab, Spec).

list_active_backends_refs(Partition, #state{backends_table = Tab}) ->
    Spec = [{
        #backend_info{_ = '_', backend_state = '$1', partition = '$2', module = '$3'},
        [{'=:=', '$2', Partition}, {'=/=', '$1', undefined}],
        [{{'$1', '$3'}}]
    }],
    ets:select(Tab, Spec).

load_waiting_backends(State = #state{wait_queue = WaitQueue}) ->
    {UpdState, UpdWaitQueue} = lists:foldl(
        fun(R = {{Start, CurrPartition}, Ref, Pid}, {CurrState, CurrWaitQueue}) ->
            case load_backend(CurrPartition, {Start, undefined}, CurrState, undefined) of
                {ok, Loaded, NewState} ->
                    Pid ! {Ref, ok, Loaded},
                    {NewState, CurrWaitQueue};
                {busy, undefined, NewState} ->
                    {NewState, [R | CurrWaitQueue]};
                {error, Reason, NewState} ->
                    Pid ! {Ref, error, Reason},
                    {NewState, CurrWaitQueue}
            end
        end, {State, []}, WaitQueue),
    UpdState#state{wait_queue = UpdWaitQueue}.

mk_key(#backend_info{partition = P, start_timestamp = S} = I) ->
    I#backend_info{key = {S, P}}.

load_backend(Partition, {From, _To}, State = #state{backends_table = Tab}, Pid) ->
    Key = {From, Partition},
    case ets:lookup(Tab, Key) of
        [I] ->
            load_existing_backend(I, Key, Partition, State, Pid);
        [] ->
            {error, does_not_exist, State}
    end.

load_existing_backend(I, Key, Partition, State = #state{backends_table = Tab, current_loaded_backends = CurrLoaded,
    max_loaded_backends = MaxLoaded, wait_queue = WaitQueue}, Pid) ->
    #backend_info{backend_state = BackendState, path = Path, opts = Config, module = Mod, ref_count = RefCnt, owners = Owners} = I,
    MonitorRef = erlang:monitor(process, Pid),
    if
        BackendState == undefined ->
            SupersedeRes = if
                               CurrLoaded >= MaxLoaded ->
                                   supersede_backends(1, Tab, Mod, CurrLoaded);
                               true ->
                                   {ok, CurrLoaded}
                           end,
            case SupersedeRes of
                {ok, NewLoadedCnt} ->
                    BackendConfig = lists:keyreplace(data_root, 1, Config, {data_root, Path}),
                    case Mod:init(Partition, BackendConfig) of
                        {ok, LoadedRef} ->
                            Loaded = I#backend_info{backend_state = LoadedRef, last_accessed = erlang:now(),
                                ref_count = 1, owners = dict:from_list([{Pid, MonitorRef}])},
                            ets:insert(Tab, Loaded),
                            lager:info("Acquire ok. ~p backends laoded", [NewLoadedCnt + 1]),
                            {ok, LoadedRef, State#state{current_loaded_backends = NewLoadedCnt + 1}};
                        {error, Reason} ->
                            {error, {backend_load_failed, Reason}, State#state{current_loaded_backends = NewLoadedCnt}}
                    end;
                {not_available, LoadedCnt} when is_pid(Pid)  ->
                    Ref = make_ref(),
                    NewState = State#state{wait_queue = WaitQueue ++ [{Key, Ref, Pid}], current_loaded_backends = LoadedCnt},
                    lager:info("Acquire busy. ~p backends loaded", [LoadedCnt]),
                    {busy, Ref, NewState};
                {not_available, LoadedCnt} when not is_pid(Pid)  ->
                    {busy, undefined, State#state{current_loaded_backends = LoadedCnt}};
                Else ->
                    {error, Else, State}
            end;
        true ->
            Ts = erlang:now(),
            ets:update_element(Tab, Key, [
                {#backend_info.last_accessed, Ts},
                {#backend_info.ref_count, RefCnt + 1},
                {#backend_info.owners, dict:store(Pid, MonitorRef, Owners)}
            ]),
            {ok, BackendState, State}
    end.

supersede_backends(1, Backends, BackendModule, CurrLoaded) ->
    InfTs = {inf, inf, inf},
    Oldest = ets:foldl(
        fun
            (#backend_info{last_accessed = undefined}, I) ->
                I;
            (I = #backend_info{last_accessed = A, ref_count = 0}, #backend_info{last_accessed = B}) when A < B ->
                I;
            (_, I) ->
                I
        end,
        #backend_info{last_accessed = InfTs}, Backends),
    case Oldest of
        #backend_info{last_accessed = InfTs} ->
            {not_available, CurrLoaded};
        #backend_info{backend_state = State} ->
            ok = BackendModule:stop(State),
            Updated = Oldest#backend_info{backend_state = undefined, last_accessed = undefined, ref_count = 0},
            ets:insert(Backends, Updated),
            {ok, CurrLoaded - 1}
    end.

process_acquire_results({ok, BackendState, NewState}) ->
    {reply, {ok, BackendState}, NewState};
process_acquire_results({busy, WaitObj, NewState}) ->
    {reply, {busy, WaitObj}, NewState};
process_acquire_results({error, Reason, NewState}) ->
    {reply, {error, Reason}, NewState}.


-ifdef(TEST).

init_test() ->
    Config = [
        {max_loaded_backends, 3}
    ],
    R = init(Config),
    ?assertMatch({ok, #state{current_loaded_backends = 0, max_loaded_backends = 3, wait_queue = []}}, R).

load_backend_test() ->
    catch meck:new(proxy_test_backend, [non_strict]),
    meck:expect(proxy_test_backend, init,
        fun(Partition, Config) ->
            ?assertEqual(112, Partition),
            ?assertEqual([{data_root, "/home/admin/data/0-1"}], Config),
            {ok, init}
        end),
    meck:expect(proxy_test_backend, stop, fun(_) -> ok end),

    Config = [
        {max_loaded_backends, 3}
    ],
    {ok, S0} = init(Config),
    Def = #backend_info{partition = 112, path = "/home/admin/data/0-1", opts = [{data_root, undefined}], module = proxy_test_backend},
    {reply, true, S1} = handle_call({add, mk_key(Def#backend_info{start_timestamp = 0, end_timestamp = 1})}, undefined, S0),
    {reply, true, S2} = handle_call({add, mk_key(Def#backend_info{start_timestamp = 1, end_timestamp = 2})}, undefined, S1),
    {reply, true, S3} = handle_call({add, mk_key(Def#backend_info{start_timestamp = 2, end_timestamp = 3})}, undefined, S2),
    {reply, true, S4} = handle_call({add, mk_key(Def#backend_info{start_timestamp = 3, end_timestamp = 4})}, undefined, S3),
    {reply, true, S5} = handle_call({add, mk_key(Def#backend_info{start_timestamp = 4, end_timestamp = 5})}, undefined, S4),
    R = S5,

    RR1 = load_backend(112, {0, 1}, R, undefined),
    ?assertMatch({ok, init, _}, RR1),
    {ok, _, R1} = RR1,
    ?assertMatch([
        #backend_info{start_timestamp = 0, end_timestamp = 1, backend_state = init, ref_count = 1},
        #backend_info{start_timestamp = 1, end_timestamp = 2, backend_state = undefined, ref_count = 0},
        #backend_info{start_timestamp = 2, end_timestamp = 3, backend_state = undefined},
        #backend_info{start_timestamp = 3, end_timestamp = 4, backend_state = undefined},
        #backend_info{start_timestamp = 4, end_timestamp = 5, backend_state = undefined}
    ],
        lists:keysort(#backend_info.start_timestamp, ets:tab2list(R#state.backends_table))),
    ?assertEqual(1, R1#state.current_loaded_backends),

    meck:expect(proxy_test_backend, init, fun(_Partition, _Config) -> {ok, init} end),

    RR2 = load_backend(112, {2, 3}, R1, self()),
    ?assertMatch({ok, init, _}, RR2),
    {ok, _, R2} = RR2,
    ?assertMatch([
        #backend_info{start_timestamp = 0, end_timestamp = 1, backend_state = init, ref_count = 1},
        #backend_info{start_timestamp = 1, end_timestamp = 2, backend_state = undefined},
        #backend_info{start_timestamp = 2, end_timestamp = 3, backend_state = init, ref_count = 1},
        #backend_info{start_timestamp = 3, end_timestamp = 4, backend_state = undefined},
        #backend_info{start_timestamp = 4, end_timestamp = 5, backend_state = undefined}
    ],
        lists:keysort(#backend_info.start_timestamp, ets:tab2list(R2#state.backends_table))),
    ?assertEqual(2, R2#state.current_loaded_backends),

    RR3 = load_backend(112, {3, 4}, R2, self()),
    ?assertMatch({ok, init, _}, RR3),
    {ok, _, R3} = RR3,
    ?assertEqual(3, R3#state.current_loaded_backends),
    ?assertMatch([
        #backend_info{start_timestamp = 0, end_timestamp = 1, backend_state = init, ref_count = 1},
        #backend_info{start_timestamp = 1, end_timestamp = 2, backend_state = undefined},
        #backend_info{start_timestamp = 2, end_timestamp = 3, backend_state = init, ref_count = 1},
        #backend_info{start_timestamp = 3, end_timestamp = 4, backend_state = init, ref_count = 1},
        #backend_info{start_timestamp = 4, end_timestamp = 5, backend_state = undefined}
    ],
        lists:keysort(#backend_info.start_timestamp, ets:tab2list(R#state.backends_table))),

    RR4 = load_backend(112, {3, 4}, R3, self()),
    ?assertMatch({ok, init, _}, RR4),
    {ok, _, R4} = RR4,
    ?assertEqual(3, R4#state.current_loaded_backends),
    ?assertMatch([
        #backend_info{start_timestamp = 0, end_timestamp = 1, backend_state = init, ref_count = 1},
        #backend_info{start_timestamp = 1, end_timestamp = 2, backend_state = undefined},
        #backend_info{start_timestamp = 2, end_timestamp = 3, backend_state = init, ref_count = 1},
        #backend_info{start_timestamp = 3, end_timestamp = 4, backend_state = init, ref_count = 2},
        #backend_info{start_timestamp = 4, end_timestamp = 5, backend_state = undefined}
    ],
        lists:keysort(#backend_info.start_timestamp, ets:tab2list(R#state.backends_table))),

    RR5 = load_backend(112, {0, 1}, R4, self()),
    ?assertMatch({ok, init, _}, RR5),
    {ok, _, R5} = RR5,
    ?assertEqual(3, R5#state.current_loaded_backends),
    ?assertMatch([
        #backend_info{start_timestamp = 0, end_timestamp = 1, backend_state = init, ref_count = 2},
        #backend_info{start_timestamp = 1, end_timestamp = 2, backend_state = undefined},
        #backend_info{start_timestamp = 2, end_timestamp = 3, backend_state = init, ref_count = 1},
        #backend_info{start_timestamp = 3, end_timestamp = 4, backend_state = init, ref_count = 2},
        #backend_info{start_timestamp = 4, end_timestamp = 5, backend_state = undefined}
    ],
        lists:keysort(#backend_info.start_timestamp, ets:tab2list(R#state.backends_table))),

    RR6 = load_backend(112, {1, 2}, R5, self()),
    ?assertMatch({busy, _, _}, RR6),
    {busy, Ref1, R6} = RR6,
    ?assert(is_reference(Ref1)),
    ?assertMatch([
        #backend_info{start_timestamp = 0, end_timestamp = 1, backend_state = init, ref_count = 2},
        #backend_info{start_timestamp = 1, end_timestamp = 2, backend_state = undefined},
        #backend_info{start_timestamp = 2, end_timestamp = 3, backend_state = init, ref_count = 1},
        #backend_info{start_timestamp = 3, end_timestamp = 4, backend_state = init, ref_count = 2},
        #backend_info{start_timestamp = 4, end_timestamp = 5, backend_state = undefined}
    ],
        lists:keysort(#backend_info.start_timestamp, ets:tab2list(R#state.backends_table))),
    ?assertEqual([{{1, 112}, Ref1, self()}], R6#state.wait_queue),
    receive 
        {_, _, _} ->
            ?assert(false)
    after 0 ->
        ok
    end,
    
    RR7 = handle_cast({release, 112, {0, 1}, updated, self()}, R6),
    ?assertMatch({noreply, _}, RR7),
    {_, R7} = RR7,
    ?assertMatch([
        #backend_info{start_timestamp = 0, end_timestamp = 1, backend_state = updated, ref_count = 1},
        #backend_info{start_timestamp = 1, end_timestamp = 2, backend_state = undefined},
        #backend_info{start_timestamp = 2, end_timestamp = 3, backend_state = init, ref_count = 1},
        #backend_info{start_timestamp = 3, end_timestamp = 4, backend_state = init, ref_count = 2},
        #backend_info{start_timestamp = 4, end_timestamp = 5, backend_state = undefined}
    ],
        lists:keysort(#backend_info.start_timestamp, ets:tab2list(R#state.backends_table))),
    ?assertEqual([{{1, 112}, Ref1, self()}], R7#state.wait_queue),
    receive
        {_, _, _} ->
            ?assert(false)
    after 0 ->
        ok
    end,

    RR8 = handle_cast({release, 112, {3, 4}, undefined, self()}, R7),
    ?assertMatch({noreply, _}, RR8),
    {_, R8} = RR8,
    ?assertMatch([
        #backend_info{start_timestamp = 0, end_timestamp = 1, backend_state = updated, ref_count = 1},
        #backend_info{start_timestamp = 1, end_timestamp = 2, backend_state = undefined},
        #backend_info{start_timestamp = 2, end_timestamp = 3, backend_state = init, ref_count = 1},
        #backend_info{start_timestamp = 3, end_timestamp = 4, backend_state = init, ref_count = 1},
        #backend_info{start_timestamp = 4, end_timestamp = 5, backend_state = undefined}
    ],
        lists:keysort(#backend_info.start_timestamp, ets:tab2list(R#state.backends_table))),
    ?assertEqual([{{1, 112}, Ref1, self()}], R8#state.wait_queue),
    receive
        {_, _, _} ->
            ?assert(false)
    after 0 ->
        ok
    end,

    RR9 = handle_cast({release, 112, {3, 4}, lala, self()}, R8),
    ?assertMatch({noreply, _}, RR9),
    {_, R9} = RR9,
    ?assertMatch([
        #backend_info{start_timestamp = 0, end_timestamp = 1, backend_state = updated, ref_count = 1},
        #backend_info{start_timestamp = 1, end_timestamp = 2, backend_state = init, ref_count = 1},
        #backend_info{start_timestamp = 2, end_timestamp = 3, backend_state = init, ref_count = 1},
        #backend_info{start_timestamp = 3, end_timestamp = 4, backend_state = undefined, ref_count = 0},
        #backend_info{start_timestamp = 4, end_timestamp = 5, backend_state = undefined}
    ],
        lists:keysort(#backend_info.start_timestamp, ets:tab2list(R#state.backends_table))),
    ?assertEqual([], R9#state.wait_queue),
    receive
        {_, _, _} = V ->
            ?assertEqual({Ref1, ok, init}, V)
    after 0 ->
        ?assert(false)
    end,

    RR10 = load_backend(112, {4, 5}, R9, self()),
    ?assertMatch({busy, _, _}, RR10),
    {busy, Ref2, R10} = RR10,
    RR11 = load_backend(112, {3, 4}, R10, self()),
    ?assertMatch({busy, _, _}, RR11),
    {busy, Ref3, R11} = RR11,
    RR12 = load_backend(112, {4, 5}, R11, self()),
    ?assertMatch({busy, _, _}, RR12),
    {busy, Ref4, R12} = RR12,
    ?assertMatch([
        #backend_info{start_timestamp = 0, end_timestamp = 1, backend_state = updated, ref_count = 1},
        #backend_info{start_timestamp = 1, end_timestamp = 2, backend_state = init, ref_count = 1},
        #backend_info{start_timestamp = 2, end_timestamp = 3, backend_state = init, ref_count = 1},
        #backend_info{start_timestamp = 3, end_timestamp = 4, backend_state = undefined},
        #backend_info{start_timestamp = 4, end_timestamp = 5, backend_state = undefined}
    ],
        lists:keysort(#backend_info.start_timestamp, ets:tab2list(R#state.backends_table))),
    ?assertEqual([{{4, 112}, Ref2, self()},{{3, 112}, Ref3, self()},{{4, 112}, Ref4, self()}], R12#state.wait_queue),
    
    RR13 = handle_cast({release, 112, {0, 1}, updated11, self()}, R12),
    ?assertMatch({noreply, _}, RR13),
    {_, R13} = RR13,
    ?assertMatch([
        #backend_info{start_timestamp = 0, end_timestamp = 1, backend_state = undefined, ref_count = 0},
        #backend_info{start_timestamp = 1, end_timestamp = 2, backend_state = init, ref_count = 1},
        #backend_info{start_timestamp = 2, end_timestamp = 3, backend_state = init, ref_count = 1},
        #backend_info{start_timestamp = 3, end_timestamp = 4, backend_state = undefined},
        #backend_info{start_timestamp = 4, end_timestamp = 5, backend_state = init, ref_count = 2}
    ],
        lists:keysort(#backend_info.start_timestamp, ets:tab2list(R#state.backends_table))),
    ?assertEqual([{{3, 112}, Ref3, self()}], R13#state.wait_queue),
    BackendsList = handle_call({list_backends, 112}, self(), R13),
    ?assertEqual({reply, [{0,1}, {1,2}, {2,3}, {3,4}, {4,5}], R13}, BackendsList),
    
    meck:expect(proxy_test_backend, init, fun(_,_) -> {error, failed} end),

    RR14 = handle_cast({release, 112, {1, 2}, 'RULE 34', self()}, R13),
    ?assertMatch({noreply, _}, RR14),
    {_, R14} = RR14,
    ?assertMatch([
        #backend_info{start_timestamp = 0, end_timestamp = 1, backend_state = undefined, ref_count = 0},
        #backend_info{start_timestamp = 1, end_timestamp = 2, backend_state = undefined, ref_count = 0},
        #backend_info{start_timestamp = 2, end_timestamp = 3, backend_state = init, ref_count = 1},
        #backend_info{start_timestamp = 3, end_timestamp = 4, backend_state = undefined},
        #backend_info{start_timestamp = 4, end_timestamp = 5, backend_state = init, ref_count = 2}
    ],
        lists:keysort(#backend_info.start_timestamp, ets:tab2list(R#state.backends_table))),
    ?assertEqual([], R14#state.wait_queue),
    ?assertEqual(2, R14#state.current_loaded_backends),
    
    receive
        {Ref3, _, _} = E ->
            ?assertEqual({Ref3, error, {backend_load_failed, failed}}, E)
    after 0 ->
        ?assert(false)
    end,

    {reply, true, R15} = handle_call({add, mk_key(Def#backend_info{partition = 42, 
        start_timestamp = 4, end_timestamp = 5})}, undefined, R14),

    RR16 = handle_call({drop_partition, 112}, self(), R15),
    ?assertMatch({reply, ok, #state{}}, RR16),
    ?assertMatch([
        #backend_info{partition = 42, start_timestamp = 4, end_timestamp = 5, backend_state = undefined, ref_count = 0}
    ],
        lists:keysort(#backend_info.start_timestamp, ets:tab2list(R#state.backends_table))),
    catch meck:unload().

-endif.

