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
-export([start_link/2, insert/3, expire/3, lookup/1, start_exchange/2, wait_cmd/3, get_segment_hashes/3]).

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
-export([init_hashtree/2, rehash/2, wait_cmd/2]).

-define(SERVER, ?MODULE).

-type index() :: non_neg_integer().
-type start_options() :: [start_option()].
-type time_interval() :: zont_pretty_time:time_interval().
-type start_option() :: {granularity, time_interval()} | {reahsh_interval, time_interval()}|{path, string()}.
-type bucket() :: module().
-type kv_list() :: [kv()].
-type kv():: {binary(), binary()}.

-record(state, {vnode_index::index(), opts, trees = [], rehash_timer_ref::ref()}).

-record(insert_event, {bucket::bucket(), value::kv_list()}).
-record(expire_event, {bucket::bucket(), keys::[binary()]}).
-record(rehashdone_event, {result::atom(), trees = []}).

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
start_exchange(Index, Remote) ->
    gen_fsm:send_event(lookup(Index), {start_exchange_event, Remote}).

get_segment_hashes(Index, Tree, Segment) ->
    gen_fsm:sync_send_event(lookup(Index), {get_hashes_event, Tree, Segment}).

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
    {next_state, wait_cmd, State}.

wait_cmd(#insert_event{bucket = Bucket, value = Data}, State = #state{trees = Trees}) ->
    State2 = State#state{trees = etsdb_aee_hashtree:insert(Bucket, Data, Trees)},
    {next_state, wait_cmd, State2};

wait_cmd(#expire_event{bucket = Bucket, keys = Keys}, State = #state{trees = Trees}) ->
    State2 = State#state{trees = etsdb_aee_hashtree:expire(Bucket, Keys, Trees)},
    {next_state, wait_cmd, State2};

wait_cmd(#rehashdone_event{result = ok, trees = RehashedTrees}, State = #state{trees = RecentTrees}) ->
    State2 = State#state{trees = etsdb_aee_hashtree:merge_trees(RehashedTrees, RecentTrees)},
    {next_state, wait_cmd, State2};

wait_cmd({start_exchange_event, Remote}, State = #state{trees = Trees}) ->
    etsdb_aee_hashtree:start_exchange(Remote, Trees),
    {next_state, wait_cmd, State}.

wait_cmd({get_hashes_event, Tree, Segment}, _From, State = #state{trees = Trees}) ->
    R = etsdb_aee_hashtree:get_hashes(Segment, Tree, Trees),
    {reply, R, wait_cmd, State}.


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
