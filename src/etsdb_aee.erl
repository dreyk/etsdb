%%%-------------------------------------------------------------------
%%% @author sidorov
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 08. Oct 2014 6:10 PM
%%%-------------------------------------------------------------------
-module(etsdb_aee).
-author("sidorov").

-behaviour(gen_fsm).

%% API
-export([start_link/2, insert/3, expire/3, lookup/1, get_exchange_remote/2, get_exchange_fun/3]).

%% gen_fsm callbacks
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

%% worker interaction

-export([rehash/1]).

%% states
-export([
    init_hashtree/2,
    wait_cmd/2, wait_cmd/3,
    exchanging/2,
    rehashing/2, rehashing/3]).

-define(SERVER, ?MODULE).

-type index() :: vnode:index().
-type partition() :: {index(), node()}.
-type start_options() :: [start_option()].
-type time_interval() :: zont_pretty_time:time_interval().
-type start_option() :: {granularity, time_interval()} | {reahsh_interval, time_interval()}| {granularity_intervals_to_expire, pos_integer()} | {path, string()}.
-type bucket() :: module().
-type kv_list() :: [kv()].
-type kv():: {binary(), binary()}.
-type remote_fun() :: term().

-record(state, {vnode_index::index(), opts, trees, rehash_timer_ref::ref(), current_xchg_partition::partition(), postpone=[]}).

-record(insert_event, {bucket::bucket(), value::kv_list()}).
-record(expire_event, {bucket::bucket(), keys::[binary()]}).
-record(rehashdone_event, {result::atom()}).
-record(start_exchange_remote_event, {partition::index()}).
-record(start_exchnage_event, {partition::index(), remote_fun::remote_fun()}).
-record(finish_exchange_event, {update}).
-record(merge_event, {trees}).

%%%=====================================================================================================================
%%% API
%%%=====================================================================================================================

%% control
-spec(start_link(start_options(), index()) -> {ok, pid()} | ignore | {error, Reason :: term()}).
start_link(Opts, Index) ->
    gen_fsm:start_link(?MODULE, [Index, Opts], []).

-spec lookup(index()) -> pid().
lookup(Index) ->
    gproc:lookup_local_name(Index).

-spec insert(index(), bucket(), kv_list()) -> ok.
insert(Index, Bucket, Value) ->
    HashedObjects = lists:keymap(fun(Obj) -> etsdb_aee_hashtree:hash_object(Bucket, Obj) end, 2, Value),
    gen_fsm:send_event(lookup(Index), #insert_event{bucket = Bucket, value = HashedObjects}).

-spec expire(index(), bucket(), [binary()]) -> ok.
expire(Index, Bucket, Keys) ->
    gen_fsm:send_event(lookup(Index), #expire_event{bucket = Bucket, keys = Keys}).


%% exchange
-spec get_exchange_fun(partition(), partition(), remote_fun()) -> any().
get_exchange_fun({Index, _node}, {Partition, _master_node}, Remote) ->
    gen_fsm:sync_send_event(lookup(Index), #start_exchnage_event{partition = Partition, remote_fun = Remote}).

-spec get_exchange_remote(partition(), partition()) -> remote_fun().
get_exchange_remote({Index, _node}, {Partition, _master_node}) ->
    gen_fsm:sync_send_event(lookup(Index), #start_exchange_remote_event{partition = Partition}).

%% internal: aee worker interaction


%%%=====================================================================================================================
%%% gen_fsm callbacks
%%%=====================================================================================================================

-spec(init(Args :: term()) ->
    {ok, StateName :: atom(), StateData :: #state{}} |
    {ok, StateName :: atom(), StateData :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([Index, Opts]) ->
    RehashTimeout = zont_data_util:propfind(reahsh_interval, Opts, {1, w}),
    timer:apply_interval(zont_pretty_time:to_millisec(RehashTimeout), ?MODULE, rehash, [self()]),
    {ok, init_hashtree, #state{vnode_index = Index, opts = Opts}, 0}.

%%% ====================================================================================================================
%%% STATES
%%% ====================================================================================================================

%% INIT STATE ----------------------------------------------------------------------------------------------------------
init_hashtree(timeout, State = #state{opts = Opts, vnode_index = Index}) ->
    State2 = State#state{trees = etsdb_aee_hashtree:load_trees(Index, Opts)},
    gproc:add_local_name(Index), %% registering only when we can accept commands
    do_rehash(State2).


%% WAIT FOR COMMAND ----------------------------------------------------------------------------------------------------
wait_cmd(#insert_event{bucket = Bucket, value = Data}, State = #state{trees = Trees}) ->
    State2 = State#state{trees = etsdb_aee_hashtree:insert(Bucket, Data, Trees)},
    {next_state, wait_cmd, State2};

wait_cmd(#expire_event{bucket = Bucket, keys = Keys}, State = #state{trees = Trees}) ->
    State2 = State#state{trees = etsdb_aee_hashtree:expire(Bucket, Keys, Trees)},
    {next_state, wait_cmd, State2};
wait_cmd(rehash, State) ->
    do_rehash(State).

wait_cmd(#start_exchange_remote_event{partition = Partition}, _From, State = #state{trees = Trees}) ->
    Pid = self(),
    Node = node(),
    Remote = fun
        (release) ->
            gen_fsm:send_event(Pid, exchange_finished);
        (start) ->
            etsdb_aee_hashtree:get_xchg_remote(Node, Partition, Trees);
        ({merge, NewTrees}) ->
            gen_fsm:send_event(Pid, #merge_event{trees = NewTrees})
        end,
    {reply, Remote, exchanging, State};
wait_cmd(#start_exchnage_event{partition = Partition, remote_fun = Remote}, _From, State = #state{trees = Trees}) ->
    Pid = self(),
    Node = node(),
    ExchangeFun = fun() ->
        Update = etsdb_aee_hashtree:exchange(Node, Partition, Remote, Trees),
        gen_fsm:send_event(Pid, #finish_exchange_event{update = Update})
    end,
    {reply, ExchangeFun, exchanging, State}.

%% EXCHANGING ----------------------------------------------------------------------------------------------------------
exchanging(#insert_event{bucket = Bucket, value = Data}, State = #state{trees = Trees}) ->
    State2 = State#state{trees = etsdb_aee_hashtree:insert(Bucket, Data, Trees)},
    {next_state, exchanging, State2};

exchanging(rehash, State = #state{postpone = Postpone}) ->
    {next_state, exchanging, State#state{postpone=[rehash|Postpone]}};

exchanging(#expire_event{} = Expire, State = #state{postpone = Postpone}) ->
    {next_state, exchanging, State#state{postpone=[Expire|Postpone]}};

exchanging(#merge_event{trees = ExchangedTrees}, State = #state{trees = RecentTrees}) ->
    {next_state, exchanging, State#state{trees = etsdb_aee_hashtree:merge_trees(ExchangedTrees, RecentTrees)}};

exchanging(#finish_exchange_event{update = Update}, State = #state{trees = Trees, postpone = Reprocess}) ->
    NewTrees = etsdb_aee_hashtree:merge_trees(Update, Trees),
    ToProcess = lists:reverse(lists:usort([rehash|Reprocess])),
    Pid = self(),
    lists:foreach(fun(Event) -> gen_fsm:send_event(Pid, Event) end, ToProcess),
    {next_state, wait_cmd, State#state{trees = NewTrees, postpone = []}}.

%% REHASHING -----------------------------------------------------------------------------------------------------------
rehashing(rehash, State = #state{vnode_index = Index}) ->
    lager:warning("[~p] Rehash request while previous rehash isn't finished yet", [Index]),
    {next_state, rehashing, State};
rehashing(#insert_event{bucket = Bucket, value = Data}, State = #state{trees = Trees}) ->
    State2 = State#state{trees = etsdb_aee_hashtree:insert(Bucket, Data, Trees)},
    {next_state, rehashing, State2};

rehashing(#expire_event{} = Expire, State = #state{postpone = Postpone}) ->
    State2 = State#state{postpone = [Expire|Postpone]},
    {next_state, rehashing, State2};

rehashing(#merge_event{trees = ExchangedTrees}, State = #state{trees = RecentTrees}) ->
    {next_state, rehashing, State#state{trees = etsdb_aee_hashtree:merge_trees(ExchangedTrees, RecentTrees)}};

rehashing(#rehashdone_event{result = ok}, State = #state{postpone = Reprocess}) ->
    ToProcess = lists:reverse(lists:usort(Reprocess)),
    Pid = self(),
    lists:foreach(fun(Event) -> gen_fsm:send_event(Pid, Event) end, ToProcess),
    State2 = State#state{postpone = []},
    {next_state, wait_cmd, State2}.

rehashing(#start_exchnage_event{}, _From, State = #state{vnode_index = Index}) ->
    lager:info("[~p] Can't exchange because busy rehashing just now", [Index]),
    {reply, busy, rehashing, State};
rehashing(#start_exchange_remote_event{}, _From, State = #state{vnode_index = Index}) ->
    lager:info("[~p] Can't exchange because busy rehashing just now", [Index]),
    {reply, busy, rehashing, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_rehash(State = #state{trees = Trees}) ->
    FsmPid = self(),
    {ok, _Pid} = spawn(
        fun() ->
            Result = etsdb_aee_hashtree:rehash(
                fun(Update) ->
                    gen_fsm:send_event(FsmPid, #merge_event{trees = Update})
                end, Trees),
            gen_fsm:send_event(FsmPid, #rehashdone_event{result = Result})
        end),
    {next_state, rehashing, State}.

-spec rehash(pid()) -> ok.
rehash(Pid) ->
    gen_fsm:send_event(Pid, rehash).


%%% ====================================================================================================================
%%% Misc callbacks
%%% ====================================================================================================================
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.


