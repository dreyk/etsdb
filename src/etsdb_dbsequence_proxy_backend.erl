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

-record(backend_info, {start_timestamp, last_timestamp, path, backend_state = undefined, last_accessed = 0}).
-record(state, {
    partition::non_neg_integer(),
    source_backends,
    source_module::module(),
    current_backend::#backend_info{} | undefined,
    config::proplists:proplist()
}).

init(Partition, Config) ->
    SourceBackendsTable = empty_backends_table(),
    init_sequence(SourceBackendsTable, Config),
    [SourceBackend | RestBackends ] = etsdb_util:propfind(proxy_source, Config, [etsdb_leveldb_backend]),
    NewConfig = lists:keyreplace(proxy_source, 1, Config, {proxy_source, RestBackends}),
    {ok, #state{partition = Partition, source_backends = SourceBackendsTable, source_module = SourceBackend, config = NewConfig}}.


stop(#state{source_module = SrcModule, source_backends = Backends}) ->
    ets:foldl(
        fun
            (#backend_info{backend_state = undefined}, ok) ->
                ok;
            (#backend_info{backend_state = State}, ok) ->
                SrcModule:stop(State)
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
            drop_self(NewSelfState);
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

is_empty(_) ->
    erlang:error(not_implemented).


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

%% ------------------------------------ TEST ---------------------------------------------------------------------------

-ifdef(TEST).

prepare_test() ->
    meck:new(etsdb_dbsequence_proxy_fileaccess, [strict]),
    meck:new(proxy_test_backend, [non_strict]).

init_test() ->
    mock_read_sequence(),
    Config = [{proxy_source, [proxy_test_backend, deeper_backend]}, {data_root, "/home/admin/data"}],
    R = init(112, Config),
    ?assertMatch({ok, #state{}}, R),
    {ok, #state{source_backends = Backends, config = ActualConfig, partition = Partition, source_module = SrcMod,
        current_backend = CurrB}} = R,
    BaclendsList = lists:keysort(#backend_info.start_timestamp, ets:tab2list(Backends)),
    ?assertEqual([
        #backend_info{start_timestamp = 0, last_timestamp = 1, path = "/home/admin/data/0-1"},
        #backend_info{start_timestamp = 1, last_timestamp = 2, path = "/home/admin/data/1-2"},
        #backend_info{start_timestamp = 2, last_timestamp = 3, path = "/home/admin/data/2-3"},
        #backend_info{start_timestamp = 3, last_timestamp = 4, path = "/home/admin/data/3-4"},
        #backend_info{start_timestamp = 4, last_timestamp = 5, path = "/home/admin/data/4-5"}
    ],
    BaclendsList),
    ?assertEqual([{proxy_source, [deeper_backend]}, {data_root, "/home/admin/data"}], ActualConfig),
    ?assertEqual(112, Partition),
    ?assertEqual(proxy_test_backend, SrcMod),
    ?assertEqual(undefined, CurrB).

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
            {ok, #state{current_backend = CurrB, source_backends = SrcBackends}} = R,
            ?assertEqual(undefined, CurrB),
            ?assertEqual([], ets:tab2list(SrcBackends))
        end,

        fun() -> %% undefined backends fail
            {ok, State} = init(112, Config),
            meck:expect(etsdb_dbsequence_proxy_fileaccess, remove_root_path,
                fun(DataRoot) when is_list(DataRoot) -> {error, "can't drop root"} end),
            R = drop(State),
            ?assertMatch({error, "can't drop root", #state{}}, R),
            {error, _, #state{current_backend = CurrB, source_backends = SrcBackends}} = R,
            ?assertEqual(undefined, CurrB),
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

    meck:expect(proxy_test_backend, stop, fun(A) -> ?assertEqual(enabled, A), ok end),
    S#state{current_backend = I}.


-endif.

