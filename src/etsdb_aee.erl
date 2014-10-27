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

-export([finalize_rehash/3, rehash/1]).

%% states
-export([
    init_hashtree/2,
    rehash/2,
    wait_cmd/2, wait_cmd/3,
    exchanging/2,
    rehashing/2, rehashing/3]).

-define(SERVER, ?MODULE).

-type index() :: vnode:index().
-type partition() :: {index(), node()}.
-type start_options() :: [start_option()].
-type time_interval() :: zont_pretty_time:time_interval().
-type start_option() :: {granularity, time_interval()} | {reahsh_interval, time_interval()}|{path, string()}.
-type bucket() :: module().
-type kv_list() :: [kv()].
-type kv():: {binary(), binary()}.
-type remote_fun() :: term().

-record(state, {vnode_index::index(), opts, trees = [], rehash_timer_ref::ref(), current_xchg_partition::partition(), postpone=[]}).

-record(insert_event, {bucket::bucket(), value::kv_list()}).
-record(expire_event, {bucket::bucket(), keys::[binary()]}).
-record(rehashdone_event, {result::atom(), trees = []}).
-record(start_exchange_remote_event, {partition::index()}).
-record(start_exchnage_event, {partition::index(), remote_fun::remote_fun()}).

%%%===================================================================
%%% API
%%%===================================================================

%% control
-spec(start_link(start_options(), index()) -> {ok, pid()} | ignore | {error, Reason :: term()}).
start_link(Opts, Index) ->
    gen_fsm:start_link(?MODULE, [Index, Opts], []).

-spec lookup(index()) -> pid().
lookup(Index) ->
    gproc:lookup_local_name(Index).

-spec insert(index(), bucket(), kv_list()) -> ok.
insert(Index, Bucket, Value) ->
    gen_fsm:send_event(lookup(Index), #insert_event{bucket = Bucket, value = Value}).

-spec expire(index(), bucket(), [binary()]) -> ok.
expire(Index, Bucket, Keys) ->
    gen_fsm:send_event(lookup(Index), #expire_event{bucket = Bucket, keys = Keys}).


%% exchange
get_exchange_fun(Index, Partition, Remote) ->
    gen_fsm:sync_send_event(lookup(Index), #start_exchnage_event{partition = Partition, remote_fun = Remote}).

-spec get_exchange_remote(partition(), partition()) -> remote_fun().
get_exchange_remote({Index, _node}, {Partition, _master_node}) ->
    gen_fsm:sync_send_event(lookup(Index), #start_exchange_remote_event{partition = Partition}).

%% internal: aee worker interaction
-spec finalize_rehash(pid(), atom(), [term()]) -> ok.
finalize_rehash(Pid, Result, Trees) ->
    gen_fsm:send_event(Pid, #rehashdone_event{result = Result, trees = Trees}).

-spec rehash(pid()) -> ok.
rehash(Pid) ->
    gen_fsm:send_event(Pid, rehash).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, StateName :: atom(), StateData :: #state{}} |
    {ok, StateName :: atom(), StateData :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([Index, Opts]) ->
    gproc:add_local_name(Index),
    RehashTimeout = zont_data_util:propfind(reahsh_interval, Opts, {1, w}),
    timer:apply_interval(zont_pretty_time:to_millisec(RehashTimeout), ?MODULE, rehash, [self()]),
    {ok, init_hashtree, #state{vnode_index = Index, opts = Opts}, 0}.


init_hashtree(timeout, State = #state{opts = Opts}) ->
    State2 = State#state{trees = etsdb_aee_hashtree:load_trees(Opts)},
    {next_state, rehash, State2, 0}.

rehash(timeout, State = #state{trees = Trees}) ->
    etsdb_aee_hashtree:rehash(self(), Trees),
    {next_state, rehashing, State}.

wait_cmd(#insert_event{bucket = Bucket, value = Data}, State = #state{trees = Trees}) ->
    State2 = State#state{trees = etsdb_aee_hashtree:insert(Bucket, Data, Trees)},
    {next_state, wait_cmd, State2};

wait_cmd(#expire_event{bucket = Bucket, keys = Keys}, State = #state{trees = Trees}) ->
    State2 = State#state{trees = etsdb_aee_hashtree:expire(Bucket, Keys, Trees)},
    {next_state, wait_cmd, State2}.

wait_cmd(#start_exchange_remote_event{partition = Partition}, _From, State = #state{trees = Trees}) ->
    Pid = self(),
    Remote = fun
        (release) ->
            gen_fsm:send_event(Pid, exchange_finished);
        (_) ->
            error('not implemented')
        end,
    {reply, Remote, exchanging, State};
wait_cmd(#start_exchnage_event{partition = Partition, remote_fun = Remote}, _From, State) ->
    Pid = self(),
    ExchangeFun = fun() ->
        etsdb_aee_hashtree:exchange(Remote),
        gen_fsm:send_event(Pid, exchange_finished)
    end,
    {reply, ExchangeFun, exchanging, State}.

exchanging(#insert_event{bucket = Bucket, value = Data}, State = #state{trees = Trees}) ->
    State2 = State#state{trees = etsdb_aee_hashtree:insert(Bucket, Data, Trees)},
    {next_state, exchanging, State2};

exchanging(rehash, State = #state{postpone = Postpone}) ->
    {next_state, exchanging, State#state{postpone=[rehash|Postpone]}};

exchanging(#expire_event{} = Expire, State = #state{postpone = Postpone}) ->
    {next_state, exchanging, State#state{postpone=[Expire|Postpone]}};

exchanging(exchange_finished, State = #state{postpone = Reprocess}) ->
    ToProcess = lists:reverse(lists:usort([rehash|Reprocess])),
    Pid = self(),
    lists:foreach(fun(Event) -> gen_fsm:send_event(Pid, Event) end, ToProcess),
    {next_state, wait_cmd, State#state{postpone = []}}.


rehashing(#insert_event{bucket = Bucket, value = Data}, State = #state{trees = Trees}) ->
    State2 = State#state{trees = etsdb_aee_hashtree:insert(Bucket, Data, Trees)},
    {next_state, rehashing, State2};

rehashing(#expire_event{} = Expire, State = #state{postpone = Postpone}) ->
    State2 = State#state{postpone = [Expire]},
    {next_state, rehashing, State2};

rehashing(#rehashdone_event{result = ok, trees = RehashedTrees}, State = #state{trees = RecentTrees, postpone = Reprocess}) ->
    ToProcess = lists:reverse(lists:usort(Reprocess)),
    Pid = self(),
    lists:foreach(fun(Event) -> gen_fsm:send_event(Pid, Event) end, ToProcess),
    State2 = State#state{trees = etsdb_aee_hashtree:merge_trees(RehashedTrees, RecentTrees), postpone = []},
    {next_state, wait_cmd, State2}.

rehashing(#start_exchnage_event{}, _From, State) ->
    {reply, busy, rehashing, State};
rehashing(#start_exchange_remote_event{}, _From, State) ->
    {reply, busy, rehashing, State}.


-spec(handle_event(Event :: term(), StateName :: atom(),
                   StateData :: #state{}) ->
                      {next_state, NextStateName :: atom(), NewStateData :: #state{}} |
                      {next_state, NextStateName :: atom(), NewStateData :: #state{},
                       timeout() | hibernate} |
                      {stop, Reason :: term(), NewStateData :: #state{}}).
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.


-spec(handle_sync_event(Event :: term(), From :: {pid(), Tag :: term()},
                        StateName :: atom(), StateData :: term()) ->
                           {reply, Reply :: term(), NextStateName :: atom(), NewStateData :: term()} |
                           {reply, Reply :: term(), NextStateName :: atom(), NewStateData :: term(),
                            timeout() | hibernate} |
                           {next_state, NextStateName :: atom(), NewStateData :: term()} |
                           {next_state, NextStateName :: atom(), NewStateData :: term(),
                            timeout() | hibernate} |
                           {stop, Reason :: term(), Reply :: term(), NewStateData :: term()} |
                           {stop, Reason :: term(), NewStateData :: term()}).
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.


-spec(handle_info(Info :: term(), StateName :: atom(),
                  StateData :: term()) ->
                     {next_state, NextStateName :: atom(), NewStateData :: term()} |
                     {next_state, NextStateName :: atom(), NewStateData :: term(),
                      timeout() | hibernate} |
                     {stop, Reason :: normal | term(), NewStateData :: term()}).
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.


-spec(terminate(Reason :: normal | shutdown | {shutdown, term()}
| term(),       StateName :: atom(), StateData :: term()) -> term()).
terminate(_Reason, _StateName, _State) ->
    ok.


-spec(code_change(OldVsn :: term() | {down, term()}, StateName :: atom(),
                  StateData :: #state{}, Extra :: term()) ->
                     {ok, NextStateName :: atom(), NewStateData :: #state{}}).
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
