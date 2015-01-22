%%%-------------------------------------------------------------------
%%% @author lol4t0
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Jan 2015 12:02
%%%-------------------------------------------------------------------
-module(etsdb_dbsequence_proxy_backend).
-author("lol4t0").

-behaviour(etsdb_backend).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([init/2, stop/1, drop/1, save/3, scan/3, scan/5, fold_objects/3, find_expired/2, delete/3, is_empty/1]).

-record(backend_info, {start_timestamp, last_timestamp, path, backend_state = undefined, last_accessed = undefined}).
-record(state, {
    partition::non_neg_integer(),
    source_backends,
    source_module::module(),
    current_backend::#backend_info{} | undefined,
    config::proplists:proplist(),
    max_loaded_backends::pos_integer(),
    current_loaded_backends = 0::non_neg_integer()
}).

init(Partition, Config) ->
    SourceBackendsTable = empty_backends_table(),
    init_sequence(SourceBackendsTable, Config),
    [SourceBackend | RestBackends ] = etsdb_util:propfind(proxy_source, Config, [etsdb_leveldb_backend]),
    MaxLoadedBackends = etsdb_util:propfind(max_loaded_backends, Config, 2),
    NewConfig = lists:keyreplace(proxy_source, 1, Config, {proxy_source, RestBackends}),
    {ok, #state{partition = Partition, source_backends = SourceBackendsTable, source_module = SourceBackend,
        config = NewConfig, max_loaded_backends = MaxLoadedBackends}}.


stop(#state{source_module = SrcModule, source_backends = Backends}) ->
    ets:foldl(
        fun
            (#backend_info{backend_state = undefined}, ok) ->
                ok;
            (S = #backend_info{backend_state = State}, ok) ->
                ok = SrcModule:stop(State),
                ets:insert(Backends, S),
                ok
        end,
        ok, Backends).


drop(SelfState = #state{source_module = SrcModule, source_backends = Backends, current_backend = CurrB}) ->
    {SrcDropResult, NewCurrBResult} = ets:foldl(
        fun
            (#backend_info{backend_state = undefined}, ResultCurrB) ->
                ResultCurrB;
            (I = #backend_info{backend_state = OldState}, {Result, Backend}) ->
                {NewResult, NewBackendInfo} = case SrcModule:drop(OldState) of
                    {ok, State} ->
                        {Result, I#backend_info{backend_state = State}};
                    {error, Reason, State} ->
                        I2 = I#backend_info{backend_state = State},
                        {[{I2, Reason}|Result], I2}
                end,
                ets:insert(Backends, NewBackendInfo),
                NewBackendInfoPattern = NewBackendInfo#backend_info{backend_state = '_'},
                NewCurrB = case Backend#backend_info{backend_state = '_'} of
                    NewBackendInfoPattern ->
                        NewBackendInfo;
                    _ ->
                        Backend
                end,
                {NewResult, NewCurrB}
        end,
        {[], CurrB}, Backends),
    NewSelfState = SelfState#state{current_backend = NewCurrBResult},
    if
        SrcDropResult =:= [] ->
            ok = stop(NewSelfState),
            drop_self(NewSelfState#state{current_loaded_backends = 0});
        true ->
            {error, SrcDropResult, NewSelfState}
    end.

save(_, _, _) ->
    erlang:error(not_implemented).

scan(_, _, _) ->
    erlang:error(not_implemented).

scan(_, _, _, _, _) ->
    erlang:error(not_implemented).

fold_objects(_, _, _) ->
    erlang:error(not_implemented).

find_expired(_, _) ->
    erlang:error(not_implemented).

delete(_, _, _) ->
    erlang:error(not_implemented).

is_empty(S = #state{source_backends = Backends, source_module = Mod}) ->
    ets:foldl(
        fun
            (_Backend, R = {false, _CurrS}) ->
                R;
            (Backend, {true, CurrS}) ->
                {#backend_info{backend_state = BackendState}, NewS} = load_backend(Backend, CurrS),
                {R, BackendState} = Mod:is_empty(BackendState),
                {R, NewS}
        end, {true, S}, Backends).


%% PRIVATE

empty_backends_table() ->
    ets:new(undefined, [set, private, {keypos, #backend_info.start_timestamp}]).

init_sequence(Table, Config) ->
    DataRoot = etsdb_util:propfind(data_root, Config, "./data"),
    SequencePaths = etsdb_dbsequence_proxy_fileaccess:read_sequence(DataRoot),
    lists:foreach(
        fun(Path) ->
            TimeStampRange = filename:basename(Path),
            [FromStr, ToStr] = string:tokens(TimeStampRange, "-"),
            Item = #backend_info{
                start_timestamp = list_to_integer(FromStr),
                last_timestamp = list_to_integer(ToStr),
                path = Path
            },
            ets:insert(Table, Item)
        end,
        SequencePaths).

drop_self(State = #state{config = Config}) ->
    DataRoot = etsdb_util:propfind(data_root, Config, "./data"),
    case etsdb_dbsequence_proxy_fileaccess:remove_root_path(DataRoot) of
        true ->
            {ok, State#state{current_backend = undefined, source_backends = empty_backends_table()}};
        {error, Reason} ->
            {error, Reason, State}
    end.

load_backend(I = #backend_info{backend_state = undefined, path = Path},
    S = #state{config = Config, source_backends = Backends, partition = Partition, source_module = Mod,
        current_loaded_backends = CurrLoaded, max_loaded_backends = MaxLoaded}) ->
    NewLoadedCnt = if
        CurrLoaded >= MaxLoaded ->
            ok = supersede_backends(CurrLoaded - MaxLoaded + 1, Backends, Mod),
            MaxLoaded;
        true ->
            CurrLoaded + 1
    end,
    BackendConfig = lists:keyreplace(data_root, 1, Config, {data_root, Path}),
    case Mod:init(Partition, BackendConfig) of
        {ok, State} ->
            Loaded = I#backend_info{backend_state = State, last_accessed = erlang:now()},
            ets:insert(Backends, Loaded),
            {Loaded, S#state{current_loaded_backends = NewLoadedCnt}};
        {error, Reason} ->
            throw({error, {backend_load_failed, Reason}})
    end;

load_backend(I = #backend_info{start_timestamp = Key}, S = #state{source_backends = Backends}) ->
    Ts = erlang:now(),
    ets:update_element(Backends, Key, {#backend_info.last_accessed, Ts}),
    {I#backend_info{last_accessed = Ts}, S}.

supersede_backends(1, Backends, BackendModule) ->
    InfTs = {inf, inf, inf},
    Oldest = ets:foldl(
        fun
            (#backend_info{last_accessed = undefined}, I) ->
                I;
            (I = #backend_info{last_accessed = A}, #backend_info{last_accessed = B}) when A < B ->
                I;
            (_, I) ->
                I
        end,
        #backend_info{last_accessed = InfTs}, Backends),
    #backend_info{backend_state = State} = Oldest,
    ok = BackendModule:stop(State),
    Updated = Oldest#backend_info{backend_state = undefined, last_accessed = undefined},
    ets:insert(Backends, Updated),
    ok.


%% ------------------------------------ TEST ---------------------------------------------------------------------------

-ifdef(TEST).

prepare_test() ->
    meck:new(etsdb_dbsequence_proxy_fileaccess, [strict]),
    meck:new(proxy_test_backend, [non_strict]).

init_test() ->
    mock_read_sequence(),
    Config = [{proxy_source, [proxy_test_backend, deeper_backend]}, {data_root, "/home/admin/data"}, {max_loaded_backends, 3}],
    R = init(112, Config),
    ?assertMatch({ok, #state{}}, R),
    {ok, #state{source_backends = Backends, config = ActualConfig, partition = Partition, source_module = SrcMod,
        current_backend = CurrB, max_loaded_backends = MaxLoaded, current_loaded_backends = CurrLoaded}} = R,
    BaclendsList = lists:keysort(#backend_info.start_timestamp, ets:tab2list(Backends)),
    ?assertEqual([
        #backend_info{start_timestamp = 0, last_timestamp = 1, path = "/home/admin/data/0-1"},
        #backend_info{start_timestamp = 1, last_timestamp = 2, path = "/home/admin/data/1-2"},
        #backend_info{start_timestamp = 2, last_timestamp = 3, path = "/home/admin/data/2-3"},
        #backend_info{start_timestamp = 3, last_timestamp = 4, path = "/home/admin/data/3-4"},
        #backend_info{start_timestamp = 4, last_timestamp = 5, path = "/home/admin/data/4-5"}
    ],
    BaclendsList),
    ?assertEqual([{proxy_source, [deeper_backend]}, {data_root, "/home/admin/data"}, {max_loaded_backends, 3}], ActualConfig),
    ?assertEqual(112, Partition),
    ?assertEqual(proxy_test_backend, SrcMod),
    ?assertEqual(undefined, CurrB),
    ?assertEqual(3, MaxLoaded),
    ?assertEqual(0, CurrLoaded).

stop_test() ->
    mock_read_sequence(),
    Config = [{proxy_source, [proxy_test_backend, deeper_backend]}, {data_root, "/home/admin/data"}],
    {ok, State} = init(112, Config),
    State2 = enable_one_backend(State),
    ?assertEqual(ok, stop(State2)).

drop_test_() ->
    mock_read_sequence(),
    Config = [{proxy_source, [proxy_test_backend, deeper_backend]}, {data_root, "/home/admin/data"}],
    [
        fun() -> %% backends ok
            {ok, State} = init(112, Config),
            State2 = enable_one_backend(State),
            meck:expect(etsdb_dbsequence_proxy_fileaccess, remove_root_path, fun(DataRoot) when is_list(DataRoot) -> true end),
            meck:expect(proxy_test_backend, drop, fun(A) -> ?assertEqual(enabled, A), {ok, desibled} end),
            R = drop(State2),
            ?assertMatch({ok, #state{}}, R),
            {ok, #state{current_backend = CurrB, source_backends = SrcBackends, current_loaded_backends = CurrLoaded}} = R,
            ?assertEqual(undefined, CurrB),
            ?assertEqual(0, CurrLoaded),
            ?assertEqual([], ets:tab2list(SrcBackends))
        end,

        fun() -> %% undefined backends fail
            {ok, State} = init(112, Config),
            meck:expect(etsdb_dbsequence_proxy_fileaccess, remove_root_path,
                fun(DataRoot) when is_list(DataRoot) -> {error, "can't drop root"} end),
            R = drop(State),
            ?assertMatch({error, "can't drop root", #state{}}, R),
            {error, _, #state{current_backend = CurrB, source_backends = SrcBackends, current_loaded_backends = CurrLoaded}} = R,
            ?assertEqual(undefined, CurrB),
            ?assertEqual(0, CurrLoaded),
            ?assertEqual([
                #backend_info{start_timestamp = 0, last_timestamp = 1, path = "/home/admin/data/0-1"},
                #backend_info{start_timestamp = 1, last_timestamp = 2, path = "/home/admin/data/1-2"},
                #backend_info{start_timestamp = 2, last_timestamp = 3, path = "/home/admin/data/2-3"},
                #backend_info{start_timestamp = 3, last_timestamp = 4, path = "/home/admin/data/3-4"},
                #backend_info{start_timestamp = 4, last_timestamp = 5, path = "/home/admin/data/4-5"}
            ],
            lists:keysort(#backend_info.start_timestamp, ets:tab2list(SrcBackends)))
        end,

        fun() -> %% enabled backends fail1
            {ok, State} = init(112, Config),
            State2 = enable_one_backend(State),
            meck:expect(etsdb_dbsequence_proxy_fileaccess, remove_root_path, fun(DataRoot) when is_list(DataRoot) -> true end),
            meck:expect(proxy_test_backend, drop, fun(A) -> ?assertEqual(enabled, A), {error, fail, some} end),
            R = drop(State2),
            ?assertMatch({
                error,
                [
                    {#backend_info{start_timestamp = 4, last_timestamp = 5, path = "/home/admin/data/4-5", backend_state = some}, fail},
                    {#backend_info{start_timestamp = 0, last_timestamp = 1, path = "/home/admin/data/0-1", backend_state = some}, fail}
                ],
                #state{}
            }, R),
            {error, _, #state{current_backend = CurrB, source_backends = SrcBackends, current_loaded_backends = CurrLoaded}} = R,
            ?assertEqual(2, CurrLoaded),
            ?assertEqual(#backend_info{start_timestamp = 4, last_timestamp = 5, path = "/home/admin/data/4-5", backend_state = some}, CurrB),
            ?assertEqual([
                #backend_info{start_timestamp = 0, last_timestamp = 1, path = "/home/admin/data/0-1", backend_state = some},
                #backend_info{start_timestamp = 1, last_timestamp = 2, path = "/home/admin/data/1-2"},
                #backend_info{start_timestamp = 2, last_timestamp = 3, path = "/home/admin/data/2-3"},
                #backend_info{start_timestamp = 3, last_timestamp = 4, path = "/home/admin/data/3-4"},
                #backend_info{start_timestamp = 4, last_timestamp = 5, path = "/home/admin/data/4-5", backend_state = some}
            ],
                lists:keysort(#backend_info.start_timestamp, ets:tab2list(SrcBackends)))
        end,

        fun() -> %% enabled backends fail2
            {ok, State} = init(112, Config),
            State2 = enable_one_backend(State),
            meck:expect(etsdb_dbsequence_proxy_fileaccess, remove_root_path,
                fun(DataRoot) when is_list(DataRoot) -> {error, "can't drp root"} end),
            meck:expect(proxy_test_backend, drop, fun(A) -> ?assertEqual(enabled, A), {error, fail, some} end),
            R = drop(State2),
            ?assertMatch({
                error,
                [
                    {#backend_info{start_timestamp = 4, last_timestamp = 5, path = "/home/admin/data/4-5", backend_state = some}, fail},
                    {#backend_info{start_timestamp = 0, last_timestamp = 1, path = "/home/admin/data/0-1", backend_state = some}, fail}
                ],
                #state{}
            }, R),
            {error, _, #state{current_backend = CurrB, source_backends = SrcBackends}} = R,
            ?assertEqual(#backend_info{start_timestamp = 4, last_timestamp = 5, path = "/home/admin/data/4-5", backend_state = some}, CurrB),
            ?assertEqual([
                #backend_info{start_timestamp = 0, last_timestamp = 1, path = "/home/admin/data/0-1", backend_state = some},
                #backend_info{start_timestamp = 1, last_timestamp = 2, path = "/home/admin/data/1-2"},
                #backend_info{start_timestamp = 2, last_timestamp = 3, path = "/home/admin/data/2-3"},
                #backend_info{start_timestamp = 3, last_timestamp = 4, path = "/home/admin/data/3-4"},
                #backend_info{start_timestamp = 4, last_timestamp = 5, path = "/home/admin/data/4-5", backend_state = some}
            ],
                lists:keysort(#backend_info.start_timestamp, ets:tab2list(SrcBackends)))
        end
    ].


load_backend_test() ->
    mock_read_sequence(),
    meck:expect(proxy_test_backend, init,
        fun(Partition, Config) ->
            ?assertEqual(112, Partition),
            ?assertEqual([
                {proxy_source, [deeper_backend]}, {data_root, "/home/admin/data/0-1"}, {max_loaded_backends, 3}
            ], Config),
            {ok, init}
        end),
    meck:expect(proxy_test_backend, stop, fun(_) -> ok end),

    Config = [{proxy_source, [proxy_test_backend, deeper_backend]}, {data_root, "/home/admin/data"}, {max_loaded_backends, 3}],
    {ok, R} = init(112, Config),
    {B, R1} = load_backend(#backend_info{start_timestamp = 0, last_timestamp = 1, path = "/home/admin/data/0-1"}, R),
    ?assertMatch(#backend_info{start_timestamp = 0, last_timestamp = 1, path = "/home/admin/data/0-1", backend_state = init}, B),
    LA = B#backend_info.last_accessed,
    ?assertNotEqual(undefined, LA),
    ?assertEqual([
        #backend_info{start_timestamp = 0, last_timestamp = 1, path = "/home/admin/data/0-1", backend_state = init, last_accessed = LA},
        #backend_info{start_timestamp = 1, last_timestamp = 2, path = "/home/admin/data/1-2"},
        #backend_info{start_timestamp = 2, last_timestamp = 3, path = "/home/admin/data/2-3"},
        #backend_info{start_timestamp = 3, last_timestamp = 4, path = "/home/admin/data/3-4"},
        #backend_info{start_timestamp = 4, last_timestamp = 5, path = "/home/admin/data/4-5"}
    ],
    lists:keysort(#backend_info.start_timestamp, ets:tab2list(R#state.source_backends))),
    ?assertEqual(1, R1#state.current_loaded_backends),

    meck:expect(proxy_test_backend, init, fun(_Partition, _Config) -> {ok, init} end),

    {_, R2} = load_backend(#backend_info{start_timestamp = 2, last_timestamp = 3, path = "/home/admin/data/2-3"}, R1),
    ?assertMatch([
        #backend_info{start_timestamp = 0, last_timestamp = 1, backend_state = init},
        #backend_info{start_timestamp = 1, last_timestamp = 2, backend_state = undefined},
        #backend_info{start_timestamp = 2, last_timestamp = 3, backend_state = init},
        #backend_info{start_timestamp = 3, last_timestamp = 4, backend_state = undefined},
        #backend_info{start_timestamp = 4, last_timestamp = 5, backend_state = undefined}
    ],
    lists:keysort(#backend_info.start_timestamp, ets:tab2list(R#state.source_backends))),
    ?assertEqual(2, R2#state.current_loaded_backends),

    {_, R3} = load_backend(#backend_info{start_timestamp = 3, last_timestamp = 4, path = "/home/admin/data/3-4"}, R2),
    ?assertEqual(3, R3#state.current_loaded_backends),
    ?assertMatch([
        #backend_info{start_timestamp = 0, last_timestamp = 1, backend_state = init},
        #backend_info{start_timestamp = 1, last_timestamp = 2, backend_state = undefined},
        #backend_info{start_timestamp = 2, last_timestamp = 3, backend_state = init},
        #backend_info{start_timestamp = 3, last_timestamp = 4, backend_state = init},
        #backend_info{start_timestamp = 4, last_timestamp = 5, backend_state = undefined}
    ],
    lists:keysort(#backend_info.start_timestamp, ets:tab2list(R#state.source_backends))),

    {_, R4} = load_backend(#backend_info{start_timestamp = 3, last_timestamp = 4, path = "/home/admin/data/3-4", backend_state = init}, R3),
    ?assertEqual(3, R4#state.current_loaded_backends),
    ?assertMatch([
        #backend_info{start_timestamp = 0, last_timestamp = 1, backend_state = init},
        #backend_info{start_timestamp = 1, last_timestamp = 2, backend_state = undefined},
        #backend_info{start_timestamp = 2, last_timestamp = 3, backend_state = init},
        #backend_info{start_timestamp = 3, last_timestamp = 4, backend_state = init},
        #backend_info{start_timestamp = 4, last_timestamp = 5, backend_state = undefined}
    ],
    lists:keysort(#backend_info.start_timestamp, ets:tab2list(R#state.source_backends))),

    {_, R5} = load_backend(#backend_info{start_timestamp = 0, last_timestamp = 1, path = "/home/admin/data/0-1"}, R4),
    ?assertMatch([
        #backend_info{start_timestamp = 0, last_timestamp = 1, backend_state = init},
        #backend_info{start_timestamp = 1, last_timestamp = 2, backend_state = undefined},
        #backend_info{start_timestamp = 2, last_timestamp = 3, backend_state = init},
        #backend_info{start_timestamp = 3, last_timestamp = 4, backend_state = init},
        #backend_info{start_timestamp = 4, last_timestamp = 5, backend_state = undefined}
    ],
    lists:keysort(#backend_info.start_timestamp, ets:tab2list(R#state.source_backends))),

    load_backend(#backend_info{start_timestamp = 1, last_timestamp = 2, path = "/home/admin/data/1-2"}, R5),
    ?assertMatch([
        #backend_info{start_timestamp = 0, last_timestamp = 1, backend_state = init},
        #backend_info{start_timestamp = 1, last_timestamp = 2, backend_state = init},
        #backend_info{start_timestamp = 2, last_timestamp = 3, backend_state = undefined},
        #backend_info{start_timestamp = 3, last_timestamp = 4, backend_state = init},
        #backend_info{start_timestamp = 4, last_timestamp = 5, backend_state = undefined}
    ],
        lists:keysort(#backend_info.start_timestamp, ets:tab2list(R#state.source_backends))).



tear_down_test() ->
    meck:unload().


%% MOCKS

mock_read_sequence() ->
    meck:expect(etsdb_dbsequence_proxy_fileaccess, read_sequence,
        fun(DataRoot) when is_list(DataRoot) ->
            [filename:join(DataRoot, integer_to_list(X)) ++ "-" ++ integer_to_list(X + 1) || X <- lists:seq(0, 4)]
        end).

enable_one_backend(S = #state{source_backends = Backends}) ->
    [I0] = ets:lookup(Backends, 4),
    I = I0#backend_info{backend_state = 'enabled'},
    ets:insert(Backends, I),

    [J0] = ets:lookup(Backends, 0),
    J = J0#backend_info{backend_state = 'enabled'},
    ets:insert(Backends, J),

    meck:expect(proxy_test_backend, stop, fun(A) -> ?assert(enabled == A orelse desibled == A), ok end),
    S#state{current_backend = I, current_loaded_backends = 2}.


-endif.

