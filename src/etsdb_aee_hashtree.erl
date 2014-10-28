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

%% API
-export([load_trees/2, insert/3, expire/3, get_xchg_remote/3, exchange/4, merge_trees/2, rehash/2]).

-type start_options() :: etsdb_aee:start_options().
-type index() :: etsdb_aee:index().
-type bucket() :: etsdb_aee:bucket().
-type kv_list() ::etsdb_aee:kv_list().
-type remote_fun() :: function().
-type tree_group() :: term().
-type tree_update() :: {replace, index(), tree_group()}.
-type update_fun() :: fun( (tree_update())->any() ).

-record(state, {trees::tree_group(), date_intervals}).

%% =====================================================================================================================
%% PUBLIC API FUNCTIONS
%% =====================================================================================================================

-spec load_trees(index(), start_options()) -> #state{}.
load_trees(Index, Opts) ->
    RootPath = zont_data_util:propfind(path, Opts),
    Indices = lists:map(fun integer_to_list/1, load_indices(Index)),
    DateIntervals = generate_date_intervals(Opts),
    Trees = lists:foldl(
        fun(Indx, Dict) -> %% first level is division by index (partition)
            Params = [{RootPath ++ Indx ++ DateInterval, DateInterval}|| DateInterval <- DateIntervals],
            FinalIndxDict = lists:foldl(
                fun({Path, Interval}, IndxDict) -> %% then divide every partition by time intervals
                    Tree = etsdb_hashtree:new({Indx, Indx}, Path),
                    dict:append(Interval, Tree, IndxDict)
                end, dict:new(), Params),
            dict:append(Indx, FinalIndxDict, Dict)
        end, dict:new(), Indices),
    #state{trees = Trees, date_intervals = DateInterval}.

-spec insert(bucket(), kv_list(), #state{}) -> #state{}.
insert(_Bucket, [], Trees) ->
    Trees;
insert(Bucket, Data = [{Key, _Value}|_], Trees) ->
    process_operation(fun do_insert/3, Key, Bucket, Data, Trees).

-spec expire(bucket(), [binary()], #state{}) -> #state{}.
expire(_Bucket, [], Trees) ->
    Trees;
expire(Bucket, Keys = [Key|_], Trees) ->
    process_operation(fun do_expire/3, Key, Bucket, Keys, Trees).

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
            case should_exchange_inserval(Interval) of
                true ->
                    RemoteForInterval = Remote(Interval),
                    {ok, {NewTree, IsUpdated}} = rpc:call(Node, etsdb_hashtree, compare, [Tree, RemoteForInterval, fun converge/2, {Tree, false}]),
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
rehash(UpdFun, #state{trees = Trees}) ->
    dict:map(
        fun(Indx, TreeGroup) ->
            NewGroup = rehash_indx(TreeGroup),
            UpdFun({replace, Indx, NewGroup}),
            undefined
        end, Trees),
    ok.


%% =====================================================================================================================
%% INTERNAL FUNCTIONS
%% =====================================================================================================================

process_operation(Fun, RefKey, Bucket, Data, State = #state{trees = Trees}) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ToTrees = riak_core_ring:preflist(RefKey, Ring),
    NewTrees = lists:foldl(
        fun(Indx, TreesDict) ->
            case dict:find(Indx, TreesDict) of
                {ok, IntervalsDict} ->
                    NewIntervalsDict = Fun(Bucket, Data, IntervalsDict),
                    dict:update(Indx, NewIntervalsDict, TreesDict);
                error ->
                    TreesDict
            end
        end, Trees, ToTrees),
    State#state{trees = NewTrees}.
