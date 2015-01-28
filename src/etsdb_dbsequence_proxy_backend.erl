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

-record(state, {
    partition::non_neg_integer(),
    source_module::module(),
    config::proplists:proplist(),
    rotation_interval::pos_integer()
}).

init(Partition, Config) ->
    [SourceBackend | RestBackends ] = etsdb_util:propfind(proxy_source, Config, [etsdb_leveldb_backend]),
    init_sequence(Partition, Config, SourceBackend),
    RotationInterval = etsdb_pretty_time:to_sec(etsdb_util:propfind(rotation_interval, Config, {30, d})),
    NewConfig = lists:keyreplace(proxy_source, 1, Config, {proxy_source, RestBackends}),
    {ok, #state{partition = Partition, source_module = SourceBackend,
        config = NewConfig, rotation_interval = RotationInterval}}.


stop(#state{source_module = SrcModule, partition = Partition}) ->
    lists:foldl(
        fun
            ({undefined, _}, ok) ->
                ok;
            ({State, _}, ok) ->
                ok = SrcModule:stop(State)
        end,
        ok, etsdb_backend_manager:drop_partition(Partition)).


drop(SelfState = #state{source_module = SrcModule, partition = Partition}) ->
    SrcDropResult = lists:foldl(
        fun({_, Key}, Result) ->
            case acquire(Partition, Key) of
                {ok, Ref} ->
                    do_drop_backend(Partition, Key, SrcModule, Ref, Result);
                Else ->
                    [Else | Result]
            end
        end, [], etsdb_backend_manager:list_backends(Partition)),
    if
        SrcDropResult =:= [] ->
            ok = stop(SelfState),
            drop_self(SelfState);
        true ->
            {error, SrcDropResult, SelfState}
    end.



save(Bucket, KvList, State = #state{rotation_interval = RotationInterval}) ->
    TKvList = Bucket:partition_by_time(KvList, RotationInterval),
    case lists:foldl(fun(E, Acc) -> do_save(E, Acc, Bucket, State) end, State, TKvList) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, {backend_save_failed, Reason}, State}
            
    end.

scan(Query, Acc, State) ->
    erlang:error(not_implemented).

scan(_, _, _, _, _) ->
    erlang:error(not_implemented).

fold_objects(_, _, _) ->
    erlang:error(not_implemented).

find_expired(_, _) ->
    erlang:error(not_implemented).

delete(_, _, _) ->
    erlang:error(not_implemented).

is_empty(#state{source_module = Mod, partition = Partition}) ->
    ets:foldl(
        fun
            (_Backend, false) ->
                false;
            ({_, Key}, true) ->
                {ok, Ref} = acquire(Partition, Key),
                R = Mod:is_empty(Ref),
                etsdb_backend_manager:release(Partition, Key, undefined),
                R
        end, 
        true, etsdb_backend_manager:list_backends(Partition)).


%% PRIVATE

init_sequence(Partition, Config, SrcModule) ->
    DataRoot = etsdb_util:propfind(data_root, Config, "./data"),
    SequencePaths = etsdb_dbsequence_proxy_fileaccess:read_sequence(DataRoot),
    lists:foreach(
        fun(Path) ->
            TimeStampRange = filename:basename(Path),
            [FromStr, ToStr] = string:tokens(TimeStampRange, "-"),
            etsdb_backend_manager:add(Partition, Path, list_to_integer(FromStr), list_to_integer(ToStr), Config, SrcModule)
        end,
        SequencePaths).

drop_self(State = #state{config = Config}) ->
    DataRoot = etsdb_util:propfind(data_root, Config, "./data"),
    case etsdb_dbsequence_proxy_fileaccess:remove_root_path(DataRoot) of
        true ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

acquire(Partition, Key) ->
    case etsdb_backend_manager:acquire(Partition, Key) of
        {ok, BackendRef} ->
            {ok, BackendRef};
        {busy, Ref} ->
            receive
                {Ref, ok, BackendRef} ->
                    {ok, BackendRef}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

do_drop_backend(Partition, Key, SrcModule, BackendRef, Result) ->
    case SrcModule:drop(BackendRef) of
        {ok, NewState} ->
            etsdb_backend_manager:release(Partition, Key, NewState),
            Result;
        {error, Reason, NewState} ->
            etsdb_backend_manager:release(Partition, Key, NewState),
            [Reason | Result]
    end.

backend_path(Start, #state{config = Config, rotation_interval = Interval}) ->
    DataRoot = etsdb_util:propfind(data_root, Config, "./data"),
    End = Start + Interval,
    BackendFileName = io_lib:format("~20..0B-~20..0B", [Start, End]),
    filename:join(DataRoot, BackendFileName).

do_save({Start, End, IntervalKvList}, ok, Bucket, State = #state{partition = Partition, source_module = Mod}) ->
    Key = {Start, End},
    case acquire(Partition, Key) of
        {error, does_not_exist} ->
            #state{config = Config} = State,
            Path = backend_path(Start, State),
            true = etsdb_backend_manager:add(Partition, Path, Start, End, Config, Mod),
            do_save({Start, End, IntervalKvList}, ok, Bucket, State);
        {ok, Ref} ->
            case Mod:save(Bucket, IntervalKvList, Ref) of
                {ok, S} ->
                    etsdb_backend_manager:release(Partition, Key, S),
                    ok;
                {error, Reason, S} ->
                    etsdb_backend_manager:release(Partition, Key, S),
                    {error, Reason}
            end
    end;
do_save(_, {error, Reason}, _, _) ->
    {error, Reason}.

%% ------------------------------------ TEST ---------------------------------------------------------------------------

-ifdef(TEST).

a_prepare_test() ->
    meck:new(etsdb_dbsequence_proxy_fileaccess, [strict]),
    meck:new(proxy_test_backend, [non_strict]).

z_tear_down_test() ->
    meck:unload().

is_empty_test_() ->
    Config = [{proxy_source, [proxy_test_backend, deeper_backend]}, {data_root, "/home/admin/data"}, {max_loaded_backends, 3}],
    etsdb_backend_manager:start_link(Config),
    meck:new(proxy_test_backend, [non_strict]),
    mock_read_sequence(),
    meck:expect(proxy_test_backend, stop, fun(_) -> ok end),
    meck:expect(proxy_test_backend, init, fun(_Partition, _Config) -> {ok, init} end),
    [
        fun() ->
            meck:expect(proxy_test_backend, is_empty, fun(State) -> {true, State} end),
            {ok, R} = init(112, Config),
            ?assertEqual(true, is_empty(R))
        end,

        fun() ->
            meck:expect(proxy_test_backend, is_empty, fun(State) -> {false, State} end),
            {ok, R} = init(112, Config),
            ?assertEqual(false, is_empty(R))
        end
    ].

save_test_() ->
    Config = [{proxy_source, [proxy_test_backend, deeper_backend]}, {data_root, "/home/admin/data"}, 
        {max_loaded_backends, 3}, {rotation_interval, {1,s}}],
    etsdb_backend_manager:start_link(Config),
    mock_read_sequence(),
    meck:expect(proxy_test_backend, stop, fun(_) -> ok end),
    meck:expect(proxy_test_backend, init, fun(_Partition, _Config) -> {ok, init} end),
    TestData = [{k1, v1}, {k2, v2}, {k3, v3}, {k4, v4}],
    meck:new(proxy_test_bucket, [non_strict]),
    meck:expect(proxy_test_bucket, partition_by_time,
        fun(_Kv, Interval) ->
            ?assertEqual(1, Interval),
            [{0, 1, [{k1, v1}]}, {3, 4, [{k2, v2}, {k3, v3}]}, {5, 6, [{k4, v4}]}]
        end),
    [
        fun() ->
            meck:expect(proxy_test_backend, save,
                fun(Bucket, KvList, State) ->
                    ?assertEqual(init, State),
                    ?assertEqual(proxy_test_bucket, Bucket),
                    ?assert(
                        KvList ==  [{k1, v1}]
                        orelse KvList == [{k2, v2}, {k3, v3}]
                        orelse KvList == [{k4, v4}]
                    ),
                    {ok, saved}
                end),
            {ok, R} = init(112, Config),
            ?assertMatch({ok, #state{}}, save(proxy_test_bucket, TestData, R))
        end,

        fun() ->
            meck:expect(proxy_test_backend, save,
                fun
                    (_Bucket, [{k2, v2}, {k3, v3}], State) ->
                        {error, failed, State};
                    (_, _, State) ->
                        {ok, State}
                end),
            {ok, R} = init(112, Config),
            ?assertMatch({error, {backend_save_failed, failed}, #state{}}, save(proxy_test_bucket, TestData, R))
        end
    ].


%% MOCKS

mock_read_sequence() ->
    meck:expect(etsdb_dbsequence_proxy_fileaccess, read_sequence,
        fun(DataRoot) when is_list(DataRoot) ->
            [filename:join(DataRoot, integer_to_list(X)) ++ "-" ++ integer_to_list(X + 1) || X <- lists:seq(0, 4)]
        end).

-endif.
