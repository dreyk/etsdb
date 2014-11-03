%%%-------------------------------------------------------------------
%%% @author sidorov
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. Oct 2014 3:00 PM
%%%-------------------------------------------------------------------
-module(etsdb_aee_hashtree).
-author("sidorov").

-include("etsdb_request.hrl").

%% API
-export([load_trees/2, insert/3, expire/3, get_xchg_remote/3, exchange/4, merge_trees/2, rehash/2, hash_object/2]).

-type start_options() :: etsdb_aee:start_options().
-type index() :: etsdb_aee:index().
-type bucket() :: etsdb_aee:bucket().
-type kv_list() ::etsdb_aee:kv_list().
-type remote_fun() :: function().
-type tree_group() :: term().
-type tree_update() :: {replace, index(), tree_group()}.
-type update_fun() :: fun( (tree_update())->any() ).
-type date_intervals() :: etsdb_aee_intervals:time_intervals().

-record(state, {trees::tree_group(), date_intervals::date_intervals(), root_path::string(), vnode_index::index(), buckets::[bucket()]}).

%% =====================================================================================================================
%% PUBLIC API FUNCTIONS
%% =====================================================================================================================

-spec load_trees(index(), start_options()) -> #state{}.
load_trees(Index, Opts) ->
    RootPath = zont_data_util:propfind(path, Opts),
    Indices = load_indices(Index),
    DateIntervals = etsdb_aee_intervals:generate_date_intervals(Opts),
    Trees = lists:foldl(
        fun(Indx, Dict) -> %% first level is division by index (partition)
            Params = [{filename:join([RootPath, integer_to_list(Indx), etsdb_aee_intervals:date_interval_to_string(DateInterval)]), DateInterval}
                      || DateInterval <- DateIntervals],
            FinalIndxDict = lists:foldl(
                fun({Path, Interval}, IndxDict) -> %% then divide every partition by time intervals
                    Tree = etsdb_hashtree:new({Indx, Indx}, Path),
                    dict:append(Interval, Tree, IndxDict)
                end, dict:new(), Params),
            dict:append(Indx, FinalIndxDict, Dict)
        end, dict:new(), Indices),
    #state{trees = Trees, date_intervals = DateInterval, root_path = RootPath, vnode_index = Index}.

-spec insert(bucket(), kv_list(), #state{}) -> #state{}.
insert(_Bucket, [], Trees) ->
    Trees;
insert(Bucket, Data = [{Key, _Value}|_], Trees) ->
    process_operation(fun do_insert/5, Key, Bucket, Data, Trees).

-spec expire(bucket(), [binary()], #state{}) -> #state{}.
expire(_Bucket, [], Trees) ->
    Trees;
expire(Bucket, Keys = [Key|_], Trees) ->
    process_operation(fun do_expire/5, Key, Bucket, Keys, Trees).

-spec get_xchg_remote(node(), index(), #state{}) -> remote_fun().
get_xchg_remote(Node, Indx, #state{trees = Trees}) ->
    {ok, TreeGroup} = dict:find(Indx, Trees),
    fun(Interval) ->
        Tree = dict:find(Interval, TreeGroup),
        fun
            (get_bucket, {Lvl, Bucket}) ->
                {ok, R} = rpc:call(Node, etsdb_hashtree, get_bucket, [Lvl, Bucket, Tree]),
                R;
            (key_hashes, Segment) ->
                {ok, R} = rpc:call(Node, etsdb_hashtree, key_hashes, [Segment, Tree]),
                R
        end
    end.

-spec exchange(node(), index(), remote_fun(), #state{}) -> tree_update().
exchange(Node, Indx, Remote, #state{trees = Trees}) ->
    {ok, TreeGroup} = dict:find(Indx, Trees),
    NewTreeGroup = dict:map(
        fun(Interval, Tree) ->
            case etsdb_aee_intervals:should_exchange_interval(Interval) of
                true ->
                    RemoteForInterval = Remote(Interval),
                    {ok, {NewTree, IsUpdated}}
                        = rpc:call(Node, etsdb_hashtree, compare, [Tree, RemoteForInterval, fun converge/2, {Tree, false}]),
                    if
                        (IsUpdated) ->
                            NewTree;
                        true ->
                            'no_exchange'
                    end;
                false ->
                    'no_exchange'
            end
        end, TreeGroup),
    UpdateTreeGroup = dict:filter(
        fun(_, V) ->
            V =/= 'no_exchange'
        end, NewTreeGroup),
    {replace, Indx, UpdateTreeGroup}.

-spec merge_trees(tree_update(), #state{}) -> #state{}.
merge_trees({replace, Indx, UpdTree}, State = #state{trees = Trees}) ->
    {ok, TreeGroup} = dict:find(Indx, Trees),
    NewTreeGroup = dict:merge(
        fun(_K, _Old, New) ->
            New
        end, TreeGroup, UpdTree),
    NewTrees = dict:store(Indx, NewTreeGroup, Trees),
    State#state{trees = NewTrees}.

-spec rehash(update_fun(), #state{}) -> ok.
rehash(UpdFun, State = #state{trees = Trees}) ->
    dict:map(
        fun(Indx, TreeGroup) ->
            NewGroup = rehash_indx(TreeGroup, Indx, State),
            UpdFun({replace, Indx, NewGroup}),
            undefined
        end, Trees),
    ok.


%% =====================================================================================================================
%% INTERNAL FUNCTIONS
%% =====================================================================================================================

-spec load_indices(index())->[index()].
load_indices(Index) ->
    Ring = riak_core_ring_manager:get_my_ring(),
    IndexBin = <<Index:160/integer>>,
    PL = riak_core_ring:preflist(IndexBin, Ring),
    Indices = [Idx || {Idx, _} <- PL],
    lists:sublist(Indices, 3). %% TODO determine actual preflist size


-type process_fun() :: fun( (bucket(), term(), tree_group(), #state{}) -> tree_group() ).
-spec process_operation(process_fun(), binary(), bucket(), term(), #state{}) -> #state{}.
process_operation(Fun, RefKey, Bucket, Data, State = #state{trees = Trees}) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ToTrees = riak_core_ring:preflist(RefKey, Ring),
    NewTrees = lists:foldl(
        fun(Indx, TreesDict) ->
            case dict:find(Indx, TreesDict) of
                {ok, IntervalsDict} ->
                    NewIntervalsDict = Fun(Indx, Bucket, Data, IntervalsDict, State),
                    dict:update(Indx, NewIntervalsDict, TreesDict);
                error ->
                    TreesDict
            end
        end, Trees, ToTrees),
    State#state{trees = NewTrees}.

-spec do_insert(index(), bucket(), kv_list(), tree_group(), #state{}) -> tree_group().
do_insert(Indx, Bucket, Data, TreeGroup, #state{date_intervals = DateIntervals, root_path = RootPath}) ->
    Interval = etsdb_aee_intervals:get_current_interval(DateIntervals),
    dict:update(Interval,
                fun
                    (Tree) ->
                        insert_objects(Bucket, Data, Tree);
                    ('create_new') ->
                        Path = filename:join([RootPath, list_to_integer(Indx), etsdb_aee_intervals:date_interval_to_string(Interval)]),
                        Tree = etsdb_hashtree:new({Indx, Indx}, Path),
                        insert_objects(Bucket, Data, Tree)
                end,
                'crate_new', TreeGroup).

-spec do_expire(index(), bucket(), [binary()], tree_group(), #state{}) -> tree_group().
do_expire(_Indx, Bucket, Keys, TreeGroup, #state{date_intervals = DateIntervals}) ->
    Intervals = etsdb_aee_intervals:get_keys_intervals(Bucket, Keys, DateIntervals),
    lists:foldl(
        fun({Interval, Keys}, WorkTreeGroup) ->
            case dict:is_key(Interval, WorkTreeGroup) of
                true ->
                    dict:update(Interval,
                                fun(Tree) ->
                                    remove_objects(Bucket, Keys, Tree)
                                end, WorkTreeGroup);
                false ->
                    lager:warning("Expiring objects (~p) from nonexisting interval: ~p", [Keys, Interval]),
                    WorkTreeGroup
            end
        end, TreeGroup, Intervals).

-spec insert_objects(bucket(), kv_list(), term()) -> term().
insert_objects(_Bucket, Objects, Tree) ->
    lists:foldl(
        fun({K, V}, WorkTree) ->
            etsdb_hashtree:insert(K, V, WorkTree)
        end, Tree, Objects).

-spec remove_objects(bucket(), [binary()], term()) -> term().
remove_objects(_Bucket, Keys, Tree) ->
    lists:foldl(
        fun(K, WorkTree) ->
            etsdb_hashtree:delete(K, WorkTree)
        end, Tree, Keys).


-record(rehash_state, {tree}).

-spec rehash_indx(tree_group(), index(), #state{}) -> tree_group().
rehash_indx(TreeGroup, Partition, #state{vnode_index = NodeIndex, buckets = Buckets}) ->
    ScanReqs = dict:map(
        fun(Interval, Tree) ->
            Ref = make_ref(),
            generate_rehash_scan_req(Partition, Interval, Tree, Buckets),
            etsdb_vnode:scan(Ref, NodeIndex, ScanReqs),
            TimeOut = zont_pretty_time:to_millisec({1, h}), %% TODO determine rehash timeout
            receive
                {r, _Index,Ref, {ok, #rehash_state{tree = NewTree}}} ->
                    NewTree
                after TimeOut ->
                    lager:error("Rehash timeout or failed. Partition ~p on vnode ~p. Interval ~p", [Partition, NodeIndex, Interval]),
                    Tree
            end
        end, TreeGroup).

-spec generate_rehash_scan_req(index(), etsdb_aee_intervals:time_inerval(), term(), [bucket()]) -> term().
generate_rehash_scan_req(Index, Interval, InitialTree, Buckets) ->
    #pscan_req{
        partition = Index,
        n_val = 1,
        quorum = 1,
        function = fun(_Backend) ->
            KeyRanges = etsdb_aee_intervals:get_key_ranges_for_interval(Interval, Buckets),
            {StartIterate, _} = hd(KeyRanges),
            FoldFun = fun({K, V}, Acc) ->
                WorkTree = case Acc of
                               #rehash_state{tree = T} ->
                                   T;
                               [] ->
                                   InitialTree
                           end,
                #rehash_state{tree = do_rehash_insert(K, V, WorkTree)}
            end,
            BatchSize = 1000, %% TODO determine batch size
            {StartIterate, FoldFun, BatchSize, KeyRanges}
        end,
        catch_end_of_data = false
    }.

-spec do_rehash_insert(binary(), binary(), term()) -> term().
do_rehash_insert(K, V, Tree)->
    etsdb_hashtree:insert(K, hash_object(undefined, V), Tree).

-spec hash_object(bucket(), term()) -> binary().
hash_object(_bucket, Obj) when is_binary(Obj) ->
    crypto:md5(Obj);
hash_object(Bucket, Obj) ->
    Bucket:hash_bject(Obj).


