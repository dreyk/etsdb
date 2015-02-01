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


stop(#state{partition = Partition}) ->
    ok = etsdb_backend_manager:drop_partition(Partition).


drop(SelfState = #state{source_module = SrcModule, partition = Partition}) ->
    SrcDropResult = lists:foldl(
        fun(Key, Result) ->
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
    case lists:foldl(fun(E, {Acc, CurrState}) -> do_save(E, Acc, Bucket, CurrState) end, {ok, State}, TKvList) of
        {ok, NewState} ->
            {ok, NewState};
        {{error, Reason}, NewState} ->
            {error, {backend_save_failed, Reason}, NewState}
            
    end.

scan(Query, Acc, #state{partition = Partition, source_module = Mod}) ->
    try
        Resp = lists:foldl(
            fun(Key, Data) ->
                case acquire(Partition, Key) of
                    {ok, Ref} ->
                        R = Mod:scan(Query, Data, Ref),
                        etsdb_backend_manager:release(Partition, Key, undefined),
                        R;
                    {error, Reason} ->
                        etsdb_backend_manager:release(Partition, Key, undefined),
                        throw({scan_failed, Key, Reason})
                end
            end, 
            Acc, etsdb_backend_manager:list_backends(Partition)),
        {ok, Resp}
    catch
        {scan_failed, Key, Reason} ->
            {error, scan_failed, Partition, Key, Reason}
    end.


scan(Bucket,From,To,Acc,#state{partition = Partition, source_module = Mod, rotation_interval = ExchangeInterval}) ->
    Intervals = calc_scan_intervals(From, To, ExchangeInterval),
    try
        Resp = lists:foldl(
            fun(Key, Data) ->
                case acquire(Partition, Key) of
                    {ok, Ref} ->
                        R = Mod:scan(Bucket, From, To, Data, Ref),
                        etsdb_backend_manager:release(Partition, Key, undefined),
                        R;
                    {error, Reason} ->
                        etsdb_backend_manager:release(Partition, Key, undefined),
                        throw({scan_failed, Key, Reason})
                end
            end,
            Acc, Intervals),
        {ok, Resp}
    catch
        {scan_failed, Key, Reason} ->
            {error, scan_failed, Partition, Key, Reason}
    end.

fold_objects(FoldObjectsFun, Acc, #state{partition = Partition, source_module = Mod}) ->
    try
        Resp = lists:foldl(
            fun(Key, Data) ->
                case acquire(Partition, Key) of
                    {ok, Ref} ->
                        R = Mod:fold_objects(FoldObjectsFun, Data, Ref),
                        etsdb_backend_manager:release(Partition, Key, undefined),
                        R;
                    {error, Reason} ->
                        etsdb_backend_manager:release(Partition, Key, undefined),
                        throw({fold_failed, Key, Reason})
                end
            end,
            Acc, etsdb_backend_manager:list_backends(Partition)),
        {ok, Resp}
    catch
        {fold_failed, Key, Reason} ->
            {error, fold_failed, Partition, Key, Reason}
    end.

find_expired(_Bucket, #state{partition = Partition, config = Config, rotation_interval = ExchangeInterval}) ->
    ExparartionTime = etsdb_pretty_time:to_sec(etsdb_util:propfind(exparation_time, Config, {24, h})),
    ExpireBefore = etsdb_util:system_time(sec) - ExparartionTime,
    ExpireIntervalsUpTo = ExpireBefore - (ExpireBefore rem ExchangeInterval),
    Intervals = etsdb_backend_manager:list_backends(Partition),
    ExpiredIntervals = [{expired_interval, I} || I = {_from, To} <- Intervals, To < ExpireIntervalsUpTo],
    {expired_records, {length(ExpiredIntervals), ExpiredIntervals}}.

delete(_Bucket, Data, #state{partition = Partition, source_module = Mod, config = Config} = State) ->
    Res = lists:foldl(
        fun({expired_interval, Interval}, Result) ->
            case acquire(Partition, Interval) of
                {ok, Ref} ->
                    DropRes = case Mod:drop(Ref) of
                        {ok, NewRef} ->
                            ok = Mod:stop(NewRef),
                            file:del_dir(mk_path(Config, Interval)),
                            Result;
                        {error, Reason, NewRef} ->
                            ok = Mod:stop(NewRef),
                            [{Interval, Reason} | Result]
                    end,
                    etsdb_backend_manager:release(Partition, Interval, delete_backend),
                    DropRes;
                {error, Reason} ->
                   [{Interval, Reason} | Result] 
            end
        end, [], Data),
    if 
        Res == [] ->
            {ok, State};
        true ->
            {error, {backends_failed, Res}, State}
    end.


is_empty(#state{source_module = Mod, partition = Partition}) ->
    lists:foldl(
        fun
            (_Backend, false) ->
                false;
            (Key, true) ->
                {ok, Ref} = acquire(Partition, Key),
                R = Mod:is_empty(Ref),
                etsdb_backend_manager:release(Partition, Key, undefined),
                R
        end, 
        true, etsdb_backend_manager:list_backends(Partition)).


%% PRIVATE

mk_path(Config, {Start, End}) ->
    DataRoot = etsdb_util:propfind(data_root, Config, "./data"),
    BackendFileName = io_lib:format("~20..0B-~20..0B", [Start, End]),
    filename:join(DataRoot, BackendFileName).

calc_scan_intervals(From, To, ExchangeInterval) ->
    First = From - (From rem ExchangeInterval),
    Last = To - (To rem ExchangeInterval) + ExchangeInterval,
    lists:seq(First, Last, ExchangeInterval).

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
                    {ok, State};
                {error, Reason, S} ->
                    etsdb_backend_manager:release(Partition, Key, S),
                    {{error, Reason}, State}
            end
    end;
do_save(_, {error, Reason}, _, State) ->
    {{error, Reason}, State}.

%% ------------------------------------ TEST ---------------------------------------------------------------------------

-ifdef(TEST).

a_prepare_test() ->
    meck:new(etsdb_dbsequence_proxy_fileaccess, [strict]),
    meck:new(proxy_test_backend, [non_strict]).

z_tear_down_test() ->
    meck:unload().

is_empty_test_() ->
    Config = [{proxy_source, [proxy_test_backend, deeper_backend]},
        {data_root, "/home/admin/data"}, {max_loaded_backends, 3}],
    etsdb_backend_manager:start_link(Config),
    meck:new(proxy_test_backend, [non_strict]),
    mock_read_sequence(),
    meck:expect(proxy_test_backend, stop, fun(_) -> ok end),
    meck:expect(proxy_test_backend, init, fun(_Partition, _Config) -> {ok, init} end),
    [
        fun() ->
            meck:expect(proxy_test_backend, is_empty, fun(_State) -> true end),
            {ok, R} = init(112, Config),
            ?assertEqual(true, is_empty(R))
        end,

        fun() ->
            meck:expect(proxy_test_backend, is_empty, fun(_State) -> false end),
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
            RR = save(proxy_test_bucket, TestData, R),
            ?assertMatch({ok, #state{}}, RR),
            {ok, S} = RR,
            ?assertMatch(ok, stop(S))
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
            RR = save(proxy_test_bucket, TestData, R),
            ?assertMatch({error, {backend_save_failed, failed}, #state{}}, RR),
            {_, _, S} = RR,
            ?assertMatch(ok, stop(S))
        end
    ].

find_expired_test_() ->
    Config = [{proxy_source, [proxy_test_backend, deeper_backend]}, {data_root, "/home/admin/data"},
        {max_loaded_backends, 3}, {rotation_interval, {1,s}}, {exparation_time, {1, s}}],
    etsdb_backend_manager:start_link(Config),
    mock_read_sequence(),
    meck:new(etsdb_util, [strict, passthrough]),
    meck:expect(proxy_test_backend, drop, fun(init) -> {ok, destoyed} end),
    [
        fun() ->
            {ok, R} = init(112, Config),
            meck:expect(etsdb_util, system_time, fun(sec) -> 4 end),
            Expired = find_expired(proxy_test_bucket, R),
            ?assertEqual({expired_records, {2, [{expired_interval,{0,1}},{expired_interval,{1,2}}]}},
                Expired),
            RR1 = delete(proxy_test_bucket, [{expired_interval,{0,1}},{expired_interval,{1,2}}], R),
            ?assertMatch({ok, _}, RR1),
            ?assertEqual([{2,3},{3,4},{4,5}], etsdb_backend_manager:list_backends(112))
        end,
        fun() ->
            {ok, R} = init(112, Config),
            meck:expect(etsdb_util, system_time, fun(sec) -> 5 end),
            ?assertEqual({expired_records, {3, [{expired_interval,{0,1}},{expired_interval,{1,2}}, {expired_interval,{2,3}}]}},
                find_expired(proxy_test_bucket, R))
        end
    ].

drop_test_() ->
    Config = [{proxy_source, [proxy_test_backend, deeper_backend]}, {data_root, "/home/admin/data"},
        {max_loaded_backends, 3}, {rotation_interval, {1,s}}, {exparation_time, {1, s}}],
    etsdb_backend_manager:start_link(Config),
    mock_read_sequence(),
    catch meck:new(etsdb_dbsequence_proxy_fileaccess, [strict]),
    [
        fun() ->
            meck:expect(etsdb_dbsequence_proxy_fileaccess, remove_root_path, fun("/home/admin/data") -> true end),
            {ok, R} = init(112, Config),
            {ok, Ref} = etsdb_backend_manager:acquire(112, {0,1}),
            RR1 = drop(R),
            ?assertMatch({ok, #state{}}, RR1),
            ?assertEqual([], etsdb_backend_manager:list_backends(112)),
            ?assertEqual(ok, etsdb_backend_manager:release(112, {0, 1}, Ref))
        end,

        fun() ->
            meck:expect(etsdb_dbsequence_proxy_fileaccess, remove_root_path, fun("/home/admin/data") -> {error, fail} end),
            {ok, R} = init(112, Config),
            RR1 = drop(R),
            ?assertMatch({error, fail, #state{}}, RR1)
        end
    ].



%% MOCKS

mock_read_sequence() ->
    meck:expect(etsdb_dbsequence_proxy_fileaccess, read_sequence,
        fun(DataRoot) when is_list(DataRoot) ->
            [filename:join(DataRoot, integer_to_list(X)) ++ "-" ++ integer_to_list(X + 1) || X <- lists:seq(0, 4)]
        end).

-endif.
