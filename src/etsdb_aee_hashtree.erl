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
-export([load_trees/2, insert/3, expire/3, get_xchg_remote/3, exchange/6, merge_trees/2, rehash/2, hash_object/2]).

-type start_options() :: etsdb_aee:start_options().
-type index() :: etsdb_aee:index().
-type bucket() :: etsdb_aee:bucket().
-type kv_list() ::etsdb_aee:kv_list().
-type remote_fun() :: function().
-type tree_group() :: term().
-type tree_update() :: {replace, index(), tree_group()}.
-type update_fun() :: fun( (tree_update())->any() ).
-type date_intervals() :: etsdb_aee_intervals:time_intervals().
-type tree() :: term().

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
                    dict:store(Interval, Tree, IndxDict)
                end, dict:new(), Params),
            dict:store(Indx, FinalIndxDict, Dict)
        end, dict:new(), Indices),
    Buckets = app_helper:get_env(etsdb,registered_bucket),
    #state{trees = Trees, date_intervals = DateIntervals, root_path = RootPath, vnode_index = Index, buckets = Buckets}.

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
    fun
        (xchg_fun, Interval) ->
            Tree = dict:find(Interval, TreeGroup),
            fun
                (get_bucket, {Lvl, Bucket}) ->
                    {ok, R} = rpc:call(Node, etsdb_hashtree, get_bucket, [Lvl, Bucket, Tree]),
                    R;
                (key_hashes, Segment) ->
                    {ok, R} = rpc:call(Node, etsdb_hashtree, key_hashes, [Segment, Tree]),
                    R
            end;
        (tree, Interval) ->
            dict:find(Interval, TreeGroup);
        (update_fun, _Interval) ->
            fun(Data, Tree) ->
                {ok, R} = rpc:call(Node, etsdb_aee_hashtree, insert_objects, ['aee_exchange_bucket', Data, Tree]),
                R
            end
    end.


-record(converge_state, {local_tree ::tree(),
                         local_updated = false,
                         local_vnode::etsdb_aee:partition(),
                         remote_vnode::etsdb_aee:partition(),
                         remote_tree::tree(),
                         remote_tree_upd_fun::function(),
                         remote_updated = false}).

-spec exchange(etsdb_aee:partition(), etsdb_aee:partition(), node(), index(), remote_fun(), #state{}) -> tree_update().
exchange(LocalVNode, RemoteVnode, Node, Indx, Remote, #state{trees = Trees}) ->
    {ok, TreeGroup} = dict:find(Indx, Trees),
    NewTreeGroup = dict:map(
        fun(Interval, Tree) ->
            case etsdb_aee_intervals:should_exchange_interval(Interval) of
                true ->
                    RemoteForInterval = Remote(xchg_fun, Interval),
                    {ok, #converge_state{
                        local_tree = NewLocalTree,
                        remote_tree = NewRemoteTree,
                        local_updated = LocalUpdated,
                        remote_updated = RemoteUpdated
                    }} = rpc:call(Node, etsdb_hashtree, compare, [Tree, RemoteForInterval, fun converge/2,
                                                                   #converge_state{local_tree = Tree,
                                                                                   local_vnode = LocalVNode,
                                                                                   remote_vnode = RemoteVnode,
                                                                                   remote_tree = Remote(tree, Interval),
                                                                                   remote_tree_upd_fun = Remote(update_fun, Interval)
                                                                   }]),
                    {return_updated(NewLocalTree, LocalUpdated), return_updated(NewRemoteTree, RemoteUpdated)};
                false ->
                    {'no_exchange', 'no_exchange'}
            end
        end, TreeGroup),
    {LocalUpdate, RemoteUpdate} = dict:fold(
        fun(K, {L, R}, {LDict, RDict}) ->
            {xcg_mb_insert_dict(K, L, LDict), xcg_mb_insert_dict(K, R, RDict)}
        end, {dict:new(), dict:new()}, NewTreeGroup),
    etsdb_vnode:aee_merge(RemoteVnode, {replace, Indx, RemoteUpdate}),
    {replace, Indx, LocalUpdate}.

xcg_mb_insert_dict(_K, 'no_exchange', Dict) ->
    Dict;
xcg_mb_insert_dict(K, V, Dict) ->
    dict:store(K, V, Dict).

return_updated(Tree, true) ->
    Tree;
return_updated(_Tree, false) ->
    'no_exchange'.

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
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    IndexBin = <<Index:160/integer>>,
    PL = riak_core_ring:preflist(IndexBin, Ring),
    Indices = [Idx || {Idx, _} <- PL],
    lists:sublist(Indices, 3). %% TODO determine actual preflist size


-type process_fun() :: fun( (index(), bucket(), term(), tree_group(), #state{}) -> tree_group() ).
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
                    ('create_new') ->
                        Path = filename:join([RootPath, integer_to_list(Indx), etsdb_aee_intervals:date_interval_to_string(Interval)]),
                        Tree = etsdb_hashtree:new({Indx, Indx}, Path),
                        insert_objects(Bucket, Data, Tree);
                    (Tree) ->
                        insert_objects(Bucket, Data, Tree)
                end,
                'crate_new', TreeGroup).

-spec do_expire(index(), bucket(), [binary()], tree_group(), #state{}) -> tree_group().
do_expire(_Indx, Bucket, Keys, TreeGroup, #state{date_intervals = DateIntervals}) ->
    Intervals = etsdb_aee_intervals:get_keys_intervals(Bucket, Keys, DateIntervals),
    lists:foldl(
        fun({Interval, IKeys}, WorkTreeGroup) ->
            case dict:is_key(Interval, WorkTreeGroup) of
                true ->
                    dict:update(Interval,
                                fun(Tree) ->
                                    remove_objects(Bucket, IKeys, Tree)
                                end, WorkTreeGroup);
                false ->
                    lager:warning("Expiring objects (~p) from nonexisting interval: ~p", [Keys, Interval]),
                    WorkTreeGroup
            end
        end, TreeGroup, Intervals).

-spec insert_objects(bucket(), kv_list(), tree()) -> tree().
insert_objects(_Bucket, Objects, Tree) ->
    lists:foldl(
        fun({K, V}, WorkTree) ->
            etsdb_hashtree:insert(K, V, WorkTree)
        end, Tree, Objects).

-spec remove_objects(bucket(), [binary()], tree()) -> tree().
remove_objects(_Bucket, Keys, Tree) ->
    lists:foldl(
        fun(K, WorkTree) ->
            etsdb_hashtree:delete(K, WorkTree)
        end, Tree, Keys).


-record(rehash_state, {tree}).

-spec rehash_indx(tree_group(), index(), #state{}) -> tree_group().
rehash_indx(TreeGroup, Partition, #state{vnode_index = NodeIndex, buckets = Buckets, date_intervals = DateIntervals}) ->
    Ref = make_ref(),
    ScanReqs = lists:map(fun(Bucket) -> generate_rehash_scan_req(Partition, TreeGroup, Bucket, DateIntervals) end, Buckets),
    ok = etsdb_vnode:scan(pure_message_reply, Ref, NodeIndex, ScanReqs),
    TimeOut = zont_pretty_time:to_millisec({1, h}), %% TODO determine rehash timeout
    receive
        {Ref, {r, _Index,Ref, {ok, #rehash_state{tree = NewTree}}}} ->
            NewTree;
        {Ref, {r, _Index,Ref, {ok, []}}} -> %% no items were found while scan
            TreeGroup
        after TimeOut ->
            lager:error("Rehash timeout or failed. Partition ~p on vnode ~p.", [Partition, NodeIndex]),
            TreeGroup
    end.

-spec generate_rehash_scan_req(index(), tree(), bucket(), date_intervals()) -> tree().
generate_rehash_scan_req(Index, InitialTreeGroup, Bucket, DateIntervals) ->
    #pscan_req{
        partition = Index,
        n_val = 1,
        quorum = 1,
        function = fun(_Backend) ->
            Ranges = Bucket:key_ranges(),
            {StartIterate, _} = hd(Ranges),
                FoldFun = fun({K, V}, Acc) ->
                WorkTreeGroup = case Acc of
                               #rehash_state{tree = T} ->
                                   T;
                               [] ->
                                   InitialTreeGroup
                           end,
                #rehash_state{tree = do_rehash_insert(Bucket, K, V, WorkTreeGroup, DateIntervals)}
            end,
            BatchSize = 1000, %% TODO determine batch size
            {StartIterate, FoldFun, BatchSize, Ranges}
        end,
        catch_end_of_data = false
    }.

-spec do_rehash_insert(bucket(), binary(), binary(), tree(), date_intervals()) -> tree().
do_rehash_insert(Bucket, K, V, TreeGroup, DateIntervals)->
    Interval = etsdb_aee_intervals:get_keys_intervals(Bucket, [K], DateIntervals),
    dict:update(
        Interval,
        fun(Tree) ->
            etsdb_hashtree:insert(K, hash_object(Bucket, V), Tree)
        end,
        TreeGroup).

-spec hash_object(bucket(), term()) -> binary().
hash_object(_bucket, Obj) when is_binary(Obj) ->
    crypto:hash(md5, Obj);
hash_object(Bucket, Obj) ->
    Bucket:hash_bject(Obj).

-spec converge([etsdb_hashtree:keydiff()], #converge_state{}) -> #converge_state{}.
converge(KeyDiff, Acc = #converge_state{local_vnode = LocalVNode,
                                        remote_vnode = RemoteVNode,
                                        local_tree = LocalTree,
                                        remote_tree = RemoteTree,
                                        remote_tree_upd_fun = RemoteTreeFun}) ->
    {LocalMissing, RemoteMissing} = lists:partition(
        fun
            ({missing, _}) ->
                true;
            ({remote_missing, _}) ->
                false
        end, KeyDiff),
    LocalTreeFun = fun (Data, Tree) -> insert_objects('aee_exchange_bucket', Data, Tree) end,
    {ok, NewLocalTree} = copy(RemoteVNode, LocalVNode, LocalMissing, LocalTreeFun, LocalTree),
    {ok, NewRemoteTree} = copy(LocalVNode, RemoteVNode, RemoteMissing, RemoteTreeFun, RemoteTree),
    LocalUpdated = length(LocalMissing) > 0,
    RemoteUpdated = length(RemoteMissing) > 0,
    Acc#converge_state{local_updated = LocalUpdated, remote_updated = RemoteUpdated, local_tree = NewLocalTree, remote_tree = NewRemoteTree}.

-record(xcg_copy_state, {current_buffer_size = 0, objects = []::kv_list(), request::{reference(), [binary()]} | undefined, tree_fun, tree}).

-spec copy(etsdb_aee:partition(), etsdb_aee:partition(), [binary()], function(), tree()) -> any().
copy(_from, _to, [], _updFun, Tree) ->
    Tree;
copy(From = {Index, _node}, To, KeysToFetch, UpdateTreeFun, Tree) ->
    SortedKeys = lists:usort(KeysToFetch),
    Range = [{Key, Key} || Key <- SortedKeys],
    BatchSize = if
                    length(SortedKeys) < 1000 -> %% TODO determine batch size
                        length(SortedKeys);
                    true ->
                        1000
                end,
    ScanReq = #pscan_req{
        partition = Index,
        n_val = 1,
        quorum = 1,
        function = fun(_Backend) ->
            {StartIterate, _} = hd(Range),
            FoldFun = fun
                (X, []) ->
                    aee_xchg_fold(X, #xcg_copy_state{tree_fun = UpdateTreeFun, tree = Tree}, To);
                (X, Acc) ->
                    aee_xchg_fold(X, Acc, To)
                end,
            {StartIterate, FoldFun, BatchSize, Range}
        end,
        catch_end_of_data = true
    },
    Ref = make_ref(),
    etsdb_vnode:scan(pure_message_reply, Ref, From, [ScanReq]),
    TimeOut = zont_pretty_time:to_millisec({1, h}), %% TODO determine rehash timeout
    receive
        {Ref, {r, _Index,Ref, {ok, #xcg_copy_state{tree = NewTree}}}} ->
            {ok, NewTree}
    after TimeOut ->
        lager:error("Exchange scan timeout or failed"),
        error
    end.

aee_xchg_fold(KV, State = #xcg_copy_state{current_buffer_size = Size, objects = Objects}, _To) when Size < 1000 -> %% TODO determine buffer size
    State#xcg_copy_state{objects = [KV|Objects], current_buffer_size = Size + 1};
aee_xchg_fold(KV, State = #xcg_copy_state{current_buffer_size = 1000}, To) -> %% Full Buffer
    put_objects([KV], To, State);
aee_xchg_fold('end_of_data', State, To) ->
    put_objects([], To, State).

-spec put_objects(kv_list(), etsdb_aee:partition(), #xcg_copy_state{}) -> #xcg_copy_state{}.
put_objects(Add, To, State = #xcg_copy_state{objects = Objects, request = Req}) ->
    NewObjects = Add + Objects,
    ReqId = make_ref(),
    etsdb_vnode:put_external(pure_message_reply, ReqId, [To], 'aee_exchange_bucket', NewObjects),
    HashedObjects = lists:keymap(fun(Obj) -> etsdb_aee_hashtree:hash_object('aee_exchange_bucket', Obj) end, 2, NewObjects),
    State2 = case Req of
        {PrevReqId, PrevObjects} ->
            receive
                {PrevReqId, {w, _Indx, PrevReqId, ok}} ->
                    insert_to_hashtree(PrevObjects, State)
            end;
        undefined ->
            State
    end,
    State2#xcg_copy_state{objects = [], current_buffer_size = 0, request = [{ReqId, HashedObjects}]}.

-spec insert_to_hashtree(kv_list(), #xcg_copy_state{}) -> #xcg_copy_state{}.
insert_to_hashtree(Objects, State = #xcg_copy_state{tree = Tree, tree_fun = UpdateTreeFun}) ->
    State#xcg_copy_state{tree = UpdateTreeFun(Objects, Tree)}.


