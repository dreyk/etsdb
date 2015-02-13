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

-include("etsdb_request.hrl").

%% API
-export([init/2, stop/1, drop/1, save/3, scan/3, scan/5, fold_objects/3, find_expired/2, delete/3, is_empty/1]).

-record(state, {
    partition::non_neg_integer(),
    source_module::module(),
    config::proplists:proplist(),
    rotation_interval::pos_integer(),
    current_save_partition :: {Key :: term(), PartitionRef :: term()} | undefined
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

scan(Query = [#pscan_req{start_time = Start, end_time = End}], Acc, 
        #state{partition = Partition, source_module = Mod}) ->
    Resp = fun() ->
        try
            ScanRes = lists:foldl(
                fun(Key, Data) ->
                    case acquire(Partition, Key) of
                        {ok, Ref} ->
                            {async, Fun} = Mod:scan(Query, Data, Ref),
                            process_scan_result(Fun, Partition, Key);
                        {error, Reason} ->
                            etsdb_backend_manager:release(Partition, Key, undefined),
                            throw({scan_failed, Key, Reason})
                    end
                end, 
                Acc, calc_scan_intervals(Start, End, Partition)),
            {ok, ScanRes}
        catch
            {scan_failed, Key, Reason} ->
                {error, {scan_failed, Partition, Key, Reason}}
        end
    end,
    {async, Resp}.

scan(Bucket,From,To,Acc,#state{partition = Partition, source_module = Mod}) ->
    Intervals = calc_scan_intervals(From, To, Partition),
    Resp = fun() ->
        try
            ScanRes = lists:foldl(
                fun(Key, Data) ->
                    case acquire(Partition, Key) of
                        {ok, Ref} ->
                            {async, Fun} = Mod:scan(Bucket, From, To, Data, Ref),
                            process_scan_result(Fun, Partition, Key);
                        {error, Reason} ->
                            etsdb_backend_manager:release(Partition, Key, undefined),
                            throw({scan_failed, Key, Reason})
                    end
                end,
                Acc, Intervals),
            {ok, ScanRes}
        catch
            {scan_failed, Key, Reason} ->
                {error, {scan_failed, Partition, Key, Reason}}
        end
    end,
    {async, Resp}.

fold_objects(FoldObjectsFun, Acc, #state{partition = Partition, source_module = Mod}) ->
    Resp = fun() ->
        try
            ScanRes = lists:foldl(
                fun(Key, Data) ->
                    case acquire(Partition, Key) of
                        {ok, Ref} ->
                            {async, Fun} = Mod:fold_objects(FoldObjectsFun, Data, Ref),
                            process_scan_result(Fun, Partition, Key);
                        {error, Reason} ->
                            etsdb_backend_manager:release(Partition, Key, undefined),
                            throw({scan_failed, Key, Reason})
                    end
                end,
                Acc, etsdb_backend_manager:list_backends(Partition)),
            {ok, ScanRes}
        catch
            {scan_failed, Key, Reason} ->
                {error, {fold_failed, Partition, Key, Reason}}
        end
    end,
    {async, Resp}.

find_expired(_Bucket, #state{partition = Partition, config = Config, rotation_interval = ExchangeInterval}) ->
    Resp = fun() ->
        ExparartionTime = etsdb_pretty_time:to_sec(etsdb_util:propfind(expiration_time, Config, {24, h})),
        ExpireBefore = etsdb_util:system_time(sec) - ExparartionTime,
        ExpireIntervalsUpTo = ExpireBefore - (ExpireBefore rem ExchangeInterval),
        Intervals = etsdb_backend_manager:list_backends(Partition),
        ExpiredIntervals = [{expired_interval, I} || I = {_from, To} <- Intervals, To < ExpireIntervalsUpTo],
        {expired_records, {length(ExpiredIntervals), ExpiredIntervals}}
    end,
    {async, Resp}.

delete(_Bucket, Data, #state{partition = Partition, source_module = Mod, config = Config} = State) ->
    Res = lists:foldl(
        fun({expired_interval, Interval}, Result) ->
            case acquire(Partition, Interval) of
                {ok, Ref} ->
                    DropRes = case Mod:drop(Ref) of
                        {ok, NewRef} ->
                            ok = Mod:stop(NewRef),
                            etsdb_dbsequence_proxy_fileaccess:remove_dir_recursively(mk_path(Config, Interval)),
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

process_scan_result(Fun, Partition, Key) ->
    case Fun() of
        {ok, CurrAcc} ->
            etsdb_backend_manager:release(Partition, Key, undefined),
            CurrAcc;
        {error, Reason} ->
            etsdb_backend_manager:release(Partition, Key, undefined),
            throw({scan_failed, Key, Reason})
    end.

mk_path(Config, {Start, End}) ->
    DataRoot = etsdb_util:propfind(data_root, Config, "./data"),
    BackendFileName = io_lib:format("~20..0B-~20..0B", [Start, End]),
    filename:join(DataRoot, BackendFileName).


calc_scan_intervals(From, inf, Partition) ->
    lists:dropwhile(fun({_IFrom, ITo}) -> ITo =< From end, etsdb_backend_manager:list_backends(Partition));
calc_scan_intervals(From, To, Partition) ->
    lists:takewhile(fun({IFrom, _ITo}) -> IFrom =< To end, calc_scan_intervals(From, inf, Partition)).

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
    case etsdb_dbsequence_proxy_fileaccess:remove_dir_recursively(DataRoot) of
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

do_save({Start, End, IntervalKvList}, ok, Bucket, OldState = #state{partition = Partition, source_module = Mod}) ->
    Key = {Start, End},
    {AcqRes, State} = save_acquire(Partition, Key, OldState),
    case  AcqRes of
        {error, does_not_exist} ->
            #state{config = Config} = State,
            Path = backend_path(Start, State),
            NewState = relase_current_partition(State),
            true = etsdb_backend_manager:add(Partition, Path, Start, End, Config, Mod),
            do_save({Start, End, IntervalKvList}, ok, Bucket, NewState);
        {ok, Ref} ->
            case Mod:save(Bucket, IntervalKvList, Ref) of
                {ok, S} ->
                    NewState = save_release(Partition, Key, S, State),
                    {ok, NewState};
                {error, Reason, S} ->
                    NewState = save_release(Partition, Key, S, State),
                    {{error, Reason}, NewState}
            end
    end;
do_save(_, {error, Reason}, _, State) ->
    {{error, Reason}, State}.

save_acquire(_Partition, Key, State = #state{current_save_partition = {Key, Ref}}) ->
    {{ok, Ref}, State};
save_acquire(Partition, Key, State = #state{current_save_partition = undefined}) ->
    case acquire(Partition, Key) of
        {ok, Ref} = Res ->
            {Res, State#state{current_save_partition = {Key, Ref}}};
        Else ->
            {Else, State}
    end;
save_acquire(Partition, Key, State) ->
    {acquire(Partition, Key), State}.

save_release(_Partition, Key, BackendRef, State = #state{current_save_partition = {Key, _}}) ->
    State#state{current_save_partition = {Key, BackendRef}};
save_release(Partition, Key, BackendRef, State) ->
    ok = etsdb_backend_manager:release(Partition, Key, BackendRef),
    State.

relase_current_partition(State = #state{current_save_partition = undefined}) ->
    State;
relase_current_partition(State = #state{current_save_partition = {Key, Ref}, partition = Partition}) ->
    ok = etsdb_backend_manager:release(Partition, Key,Ref),
    State#state{current_save_partition = undefined}.

%% ------------------------------------ TEST ---------------------------------------------------------------------------

-ifdef(TEST).

prepare_test() ->
    catch meck:new(etsdb_dbsequence_proxy_fileaccess, [strict]),
    catch meck:new(proxy_test_backend, [non_strict]).

is_empty_test_() ->
    Config = [{proxy_source, [proxy_test_backend, deeper_backend]},
        {data_root, "/home/admin/data"}, {max_loaded_backends, 3}],
    etsdb_backend_manager:start_link(Config),
    catch meck:new(proxy_test_backend, [non_strict]),
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
    catch meck:new(proxy_test_bucket, [non_strict]),
    meck:expect(proxy_test_bucket, partition_by_time,
        fun(_Kv, Interval) ->
            ?assertEqual(1, Interval),
            [{0, 1, [{k1, v1}]}, {3, 4, [{k2, v2}, {k3, v3}]}, {5, 6, [{k4, v4}]}]
        end),
    [
        fun() ->
            {ok, R} = init(112, Config),
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
            
            RR1 = save(proxy_test_bucket, TestData, R),
            ?assertMatch({ok, #state{current_save_partition = {{5,6}, saved}}}, RR1),
            {ok, S1} = RR1,

            meck:expect(proxy_test_backend, save,
                fun(Bucket, KvList, State) ->
                    ?assertEqual(saved, State),
                    ?assertEqual(proxy_test_bucket, Bucket),
                    ?assert(
                        KvList ==  [{k1, v1}]
                            orelse KvList == [{k2, v2}, {k3, v3}]
                            orelse KvList == [{k4, v4}]
                    ),
                    {ok, saved2}
                end),
            RR2 = save(proxy_test_bucket, TestData, S1),
            ?assertMatch({ok, #state{current_save_partition = {{5,6}, saved2}}}, RR2),
            {ok, S2} = RR2,
            ?assertMatch(ok, stop(S2))
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
            RR1 = save(proxy_test_bucket, TestData, R),
            ?assertMatch({error, {backend_save_failed, failed}, #state{}}, RR1),
            {_, _, S1} = RR1,
            ?assertMatch(ok, stop(S1))
        end
    ].

find_expired_test_() ->
    Config = [{proxy_source, [proxy_test_backend, deeper_backend]}, {data_root, "/home/admin/data"},
        {max_loaded_backends, 3}, {rotation_interval, {1,s}}, {expiration_time, {1, s}}],
    etsdb_backend_manager:start_link(Config),
    mock_read_sequence(),
    catch meck:new(etsdb_util, [strict, passthrough]),
    meck:expect(proxy_test_backend, drop, fun(init) -> {ok, destoyed} end),
    [
        fun() ->
            {ok, R} = init(112, Config),
            meck:expect(etsdb_util, system_time, fun(sec) -> 4 end),
            ExpiredResp = find_expired(proxy_test_bucket, R),
            ?assertMatch({async, _}, ExpiredResp),
            {async, ExpiredFun} = ExpiredResp,
            ?assert(is_function(ExpiredFun, 0)),
            Expired = ExpiredFun(),
            ?assertEqual({expired_records, {2, [{expired_interval,{0,1}},{expired_interval,{1,2}}]}},
                Expired),
            
            catch meck:new(etsdb_dbsequence_proxy_fileaccess, [strict]),
            meck:expect(etsdb_dbsequence_proxy_fileaccess, remove_dir_recursively, 
                [
                    {["/home/admin/data/00000000000000000000-00000000000000000001"], true},
                    {["/home/admin/data/00000000000000000001-00000000000000000002"], true}
                ]),
            RR1 = delete(proxy_test_bucket, [{expired_interval,{0,1}},{expired_interval,{1,2}}], R),
            ?assertMatch({ok, _}, RR1),
            ?assertEqual([{2,3},{3,4},{4,5}], etsdb_backend_manager:list_backends(112))
        end,
        fun() ->
            {ok, R} = init(112, Config),
            meck:expect(etsdb_util, system_time, fun(sec) -> 5 end),
            ExpiredResp = find_expired(proxy_test_bucket, R),
            ?assertMatch({async, _}, ExpiredResp),
            {async, ExpiredFun} = ExpiredResp,
            ?assert(is_function(ExpiredFun, 0)),
            Expired = ExpiredFun(),
            ?assertEqual({expired_records, {3, [{expired_interval,{0,1}},{expired_interval,{1,2}}, {expired_interval,{2,3}}]}},
                Expired)
        end
    ].

drop_test_() ->
    Config = [{proxy_source, [proxy_test_backend, deeper_backend]}, {data_root, "/home/admin/data"},
        {max_loaded_backends, 3}, {rotation_interval, {1,s}}, {expiration_time, {1, s}}],
    etsdb_backend_manager:start_link(Config),
    mock_read_sequence(),
    catch meck:new(etsdb_dbsequence_proxy_fileaccess, [strict]),
    [
        fun() ->
            meck:expect(etsdb_dbsequence_proxy_fileaccess, remove_dir_recursively, fun("/home/admin/data") -> true end),
            {ok, R} = init(112, Config),
            {ok, Ref} = etsdb_backend_manager:acquire(112, {0,1}),
            RR1 = drop(R),
            ?assertMatch({ok, #state{}}, RR1),
            ?assertEqual([], etsdb_backend_manager:list_backends(112)),
            ?assertEqual(ok, etsdb_backend_manager:release(112, {0, 1}, Ref))
        end,

        fun() ->
            meck:expect(etsdb_dbsequence_proxy_fileaccess, remove_dir_recursively, fun("/home/admin/data") -> {error, fail} end),
            {ok, R} = init(112, Config),
            RR1 = drop(R),
            ?assertMatch({error, fail, #state{}}, RR1)
        end
    ].

scan_req_test_() ->
    Config = [{proxy_source, [proxy_test_backend, deeper_backend]}, {data_root, "/home/admin/data"},
        {max_loaded_backends, 3}, {rotation_interval, {1,s}}, {expiration_time, {1, s}}],
    etsdb_backend_manager:start_link(Config),
    mock_read_sequence(),
    Req = [#pscan_req{}],
    catch meck:new(proxy_test_backend, [non_strict]),
    [
        fun() -> 
            meck:expect(proxy_test_backend, scan,
                fun
                    (R, [ ], init) when R == Req -> {async, fun() -> {ok, [1]} end};
                    (R, [1], init) when R == Req -> {async, fun() -> {ok, [2]} end};
                    (R, [2], init) when R == Req -> {async, fun() -> {ok, [3]} end};
                    (R, [3], init) when R == Req -> {async, fun() -> {ok, [4]} end};
                    (R, [4], init) when R == Req -> {async, fun() -> {ok, [5]} end}
                end),
            {ok, R} = init(112, Config),
            RR1 = scan(Req, [], R),
            ?assertMatch({async, _}, RR1),
            {async, Fun} = RR1,
            ?assert(is_function(Fun, 0)),
            RR2 = Fun(),
            ?assertEqual({ok, [5]}, RR2)
        end,
        
        fun() ->
            meck:expect(proxy_test_backend, scan,
                fun
                    (R, [ ], init) when R == Req -> {async, fun() -> {ok, [1]} end};
                    (R, [1], init) when R == Req -> {async, fun() -> {ok, [2]} end};
                    (R, [2], init) when R == Req -> {async, fun() -> {error, failed} end};
                    (R, [3], init) when R == Req -> {async, fun() -> {ok, [4]} end};
                    (R, [4], init) when R == Req -> {async, fun() -> {ok, [5]} end}
                end),
            {ok, R} = init(112, Config),
            RR1 = scan(Req, [], R),
            ?assertMatch({async, _}, RR1),
            {async, Fun} = RR1,
            ?assert(is_function(Fun, 0)),
            RR2 = Fun(),
            ?assertMatch({error, _}, RR2),
            {error, Reason} = RR2,
            ?assertEqual({scan_failed, 112, {2,3}, failed}, Reason)
        end
    ].

scan_simple_test_() ->
    Config = [{proxy_source, [proxy_test_backend, deeper_backend]}, {data_root, "/home/admin/data"},
        {max_loaded_backends, 3}, {rotation_interval, {1,s}}, {expiration_time, {1, s}}],
    etsdb_backend_manager:start_link(Config),
    mock_read_sequence(),
    catch meck:new(proxy_test_backend, [non_strict]),
    [
        fun() ->
            meck:expect(proxy_test_backend, scan,
                fun(proxy_test_bucket, 2, 4, L, init) ->
                        {async, fun() -> {ok, [1 | L]} end}
                end),
            {ok, R} = init(112, Config),
            RR1 = scan(proxy_test_bucket, 2, 4, [], R),
            ?assertMatch({async, _}, RR1),
            {async, Fun} = RR1,
            ?assert(is_function(Fun, 0)),
            RR2 = Fun(),
            ?assertEqual({ok, [1,1, 1]}, RR2)
        end,
        
        fun() ->
            meck:expect(proxy_test_backend, scan,
                fun(proxy_test_bucket, 2, 4, _L, init) ->
                    {async, fun() -> {error, beadabeda} end}
                end),
            {ok, R} = init(112, Config),
            RR1 = scan(proxy_test_bucket, 2, 4, [], R),
            ?assertMatch({async, _}, RR1),
            {async, Fun} = RR1,
            ?assert(is_function(Fun, 0)),
            RR2 = Fun(),
            ?assertMatch({error, _}, RR2),
            {error, Reason} = RR2,
            ?assertEqual({scan_failed, 112, {2,3}, beadabeda}, Reason)
        end
    ].

calc_scan_intervals_test_() ->
    Config = [{proxy_source, [proxy_test_backend, deeper_backend]}, {data_root, "/home/admin/data"},
        {max_loaded_backends, 3}, {rotation_interval, {1,s}}, {expiration_time, {1, s}}],
    etsdb_backend_manager:start_link(Config),
    mock_read_sequence_100(),
    {ok, _} = init(113, Config),
    [
        ?_assertEqual([{200, 300}, {300, 400}], test_calc_scan_intervals(200, 399, 100)),
        ?_assertEqual([{100, 200}, {200, 300}, {300, 400}], test_calc_scan_intervals(185, 399, 100)),
        ?_assertEqual([{200, 300}, {300, 400}], test_calc_scan_intervals(213, 399, 100)),
        ?_assertEqual([{200, 300}, {300, 400}], test_calc_scan_intervals(200, 386, 100)),
        ?_assertEqual([{200, 300}, {300, 400}, {400, 500}], test_calc_scan_intervals(200, 401, 100)),
        ?_assertEqual([{100, 200}, {200, 300}, {300, 400}, {400, 500}], test_calc_scan_intervals(185, 420, 100))
    ].

test_calc_scan_intervals(From, To, _Incr) ->
    calc_scan_intervals(From, To, 113).

fold_test_() ->
    Config = [{proxy_source, [proxy_test_backend, deeper_backend]}, {data_root, "/home/admin/data"},
        {max_loaded_backends, 3}, {rotation_interval, {1,s}}, {expiration_time, {1, s}}],
    etsdb_backend_manager:start_link(Config),
    mock_read_sequence(),
    catch meck:new(proxy_test_backend, [non_strict]),
    [
        fun() ->
            meck:expect(proxy_test_backend, fold_objects,
                fun(Fun, Acc, init) ->
                    {async, fun() -> {ok, Fun(k, 1, Acc)} end}
                end),
            {ok, R} = init(112, Config),
            RR1 = fold_objects(fun(K, V, Acc) -> [{K, V}|Acc]end, [], R),
            ?assertMatch({async, _}, RR1),
            {async, Fun} = RR1,
            ?assert(is_function(Fun, 0)),
            RR2 = Fun(),
            ?assertEqual({ok, [{k,1},{k,1},{k,1},{k,1},{k,1}]}, RR2)
        end,

        fun() ->
            meck:expect(proxy_test_backend, fold_objects,
                fun(_Fun, _Acc, init) ->
                    {async, fun() -> {error, uuuh} end}
                end),
            {ok, R} = init(112, Config),
            RR1 = fold_objects(fun(K, V, Acc) -> [{K, V}|Acc]end, [], R),
            ?assertMatch({async, _}, RR1),
            {async, Fun} = RR1,
            ?assert(is_function(Fun, 0)),
            RR2 = Fun(),
            ?assertMatch({error, _}, RR2),
            {error, Reason} = RR2,
            ?assertEqual({fold_failed, 112, {0,1}, uuuh}, Reason)
        end
    ].

monitors_test() ->
    %% not exactly test, still checks that manager will not crash
    R = etsdb_backend_manager:acquire(112, {0, 1}),
    ?assertMatch({ok, _}, R).
    %% at this point monitoring should remove broken reference to acquired partition, but we cannot check it.
    %% Still we'll see code being executed in cover analysis results.

tear_down_test() ->
    meck:unload().


%% MOCKS

mock_read_sequence() ->
    meck:expect(etsdb_dbsequence_proxy_fileaccess, read_sequence,
        fun(DataRoot) when is_list(DataRoot) ->
            [filename:join(DataRoot, integer_to_list(X)) ++ "-" ++ integer_to_list(X + 1) || X <- lists:seq(0, 4)]
        end).

mock_read_sequence_100() ->
    meck:expect(etsdb_dbsequence_proxy_fileaccess, read_sequence,
        fun(DataRoot) when is_list(DataRoot) ->
            [filename:join(DataRoot, integer_to_list(X)) ++ "-" ++ integer_to_list(X + 100) || X <- lists:seq(0, 400, 100)]
        end).

-endif.
