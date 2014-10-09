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
-export([start_link/2, insert/3, expire/3, lookup/1]).

%% gen_fsm callbacks
-export([init/1,
         state_name/2,
         state_name/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-define(SERVER, ?MODULE).

-type index() :: non_neg_integer().
-type start_options() :: [start_option()].
-type time_interval() :: {non_neg_integer(),d} | {non_neg_integer(), w} | {non_neg_integer(), y}.
-type start_option() :: {granularity, time_interval()}|{expiration, time_interval()}|{path, string()}.
-type bucket() :: module().
-type kv_list() :: [kv()].
-type kv():: {binary(), binary()}.

-record(state, {}).

-record(insert_event, {bucket::bucket(), value::kv_list()}).
-record(expire_event, {bucket::bucket(), keys::[binary()]}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @end
%%--------------------------------------------------------------------
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
init([Index, _Opts]) ->
    gproc:add_local_name(Index),
    {ok, state_name, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same
%% name as the current state name StateName is called to handle
%% the event. It is also called if a timeout occurs.
%%
%% @end
%%--------------------------------------------------------------------
-spec(state_name(Event, State) ->
    {next_state, NextStateName :: atom(), NextState :: #state{}} |
    {next_state, NextStateName :: atom(), NextState :: #state{},
     timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
state_name(_Event, State) ->
    {next_state, state_name, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_event/[2,3], the instance of this function with
%% the same name as the current state name StateName is called to
%% handle the event.
%%
%% @end
%%--------------------------------------------------------------------
-spec(state_name(Event :: term(), From :: {pid(), term()},
                 State :: #state{}) ->
                    {next_state, NextStateName :: atom(), NextState :: #state{}} |
                    {next_state, NextStateName :: atom(), NextState :: #state{},
                     timeout() | hibernate} |
                    {reply, Reply, NextStateName :: atom(), NextState :: #state{}} |
                    {reply, Reply, NextStateName :: atom(), NextState :: #state{},
                     timeout() | hibernate} |
                    {stop, Reason :: normal | term(), NewState :: #state{}} |
                    {stop, Reason :: normal | term(), Reply :: term(),
                     NewState :: #state{}}).
state_name(_Event, _From, State) ->
    Reply = ok,
    {reply, Reply, state_name, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_event(Event :: term(), StateName :: atom(),
                   StateData :: #state{}) ->
                      {next_state, NextStateName :: atom(), NewStateData :: #state{}} |
                      {next_state, NextStateName :: atom(), NewStateData :: #state{},
                       timeout() | hibernate} |
                      {stop, Reason :: term(), NewStateData :: #state{}}).
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @end
%%--------------------------------------------------------------------
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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: term(), StateName :: atom(),
                  StateData :: term()) ->
                     {next_state, NextStateName :: atom(), NewStateData :: term()} |
                     {next_state, NextStateName :: atom(), NewStateData :: term(),
                      timeout() | hibernate} |
                     {stop, Reason :: normal | term(), NewStateData :: term()}).
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: normal | shutdown | {shutdown, term()}
| term(),       StateName :: atom(), StateData :: term()) -> term()).
terminate(_Reason, _StateName, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, StateName :: atom(),
                  StateData :: #state{}, Extra :: term()) ->
                     {ok, NextStateName :: atom(), NewStateData :: #state{}}).
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
