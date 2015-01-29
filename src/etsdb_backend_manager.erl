%%%-------------------------------------------------------------------
%%% @author lol4t0
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Jan 2015 9:08
%%%-------------------------------------------------------------------
-module(etsdb_backend_manager).
-author("lol4t0").

-behaviour(gen_server).

%% API
-export([start_link/1, add/6, delete/2, acquire/2, release/3, list_backends/1, drop_partition/1]).

%% Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-type backend_key() :: {Start::non_neg_integer(), End::non_neg_integer()}.
-type backend_description() :: {BackendRef :: term() | undefined , Key :: backend_key()}.

-record(backend_info, {
    key :: {StartTimestamp :: non_neg_integer(), Partition :: term()},
    partition :: term(),
    start_timestamp :: non_neg_integer(),
    end_timestamp :: non_neg_integer(),
    path :: string(),
    backend_state = undefined,
    last_accessed = undefined,
    ref_count = 0,
    opts :: proplists:proplist(),
    module :: module()
}).
-record(state, {
    backends_table,
    max_loaded_backends::pos_integer(),
    current_loaded_backends = 0::non_neg_integer(),
    wait_queue = [] :: [{Key ::{From :: non_neg_integer(), Partition :: non_neg_integer()}, Ref :: term(), RequesterPid :: pid()}]
}).


%% API IMPLEMENTATION

-spec start_link(proplists:proplist()) -> {'ok', pid()} | 'ignore' | {'error', term()}.
start_link(Config) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Config], []).

-spec add(term(), string(), non_neg_integer(), non_neg_integer(), proplists:proplist(), module()) -> ok.
add(Partition, Path, From, To, Opts, Module) ->
    gen_server:call(?MODULE, {add, mk_key(#backend_info{partition = Partition, path = Path,
        start_timestamp = From, end_timestamp = To, opts = Opts, module = Module})}).

-spec delete(term(), backend_key()) -> ok.
delete(Partition, Key) ->
    gen_server:call(?MODULE, {delete, Partition, Key}).

-spec acquire(term(), backend_key()) ->
    {ok, BackendRef :: term()} | {busy, WaitObject :: term()} | {error, Reason :: term()}.
acquire(Partition, Key) ->
    gen_server:call(?MODULE, {acquire, Partition, Key}).

-spec release(term(), backend_key(), term()) -> ok.
release(Partition, Key, NewState) ->
    gen_server:cast(?MODULE, {release, Partition, Key, NewState}),
    ok.

-spec list_backends(term()) -> {ok, Backends :: backend_description()} | {error, Reason :: term()}.
list_backends(Partition) ->
    gen_server:call(?MODULE, {list_backends, Partition}).

drop_partition(Partition) ->
    gen_server:call(?MODULE, {drop_partition, Partition}).


%% CALLBACKS IMPLEMENTATION

init(Config)->
    MaxLoadedBackends = etsdb_util:propfind(max_loaded_backends, Config, 2),
    {ok, #state{
        max_loaded_backends = MaxLoadedBackends,
        backends_table = ets:new(undefined, [set, private, {keypos, #backend_info.key}])
    }}.

handle_call({add, BackendInfo}, _From, State = #state{backends_table = Tab}) ->
    Created = ets:insert_new(Tab, BackendInfo),
    {reply, Created, State};
handle_call({delete, Partition, {From, _To}}, _From, State = #state{backends_table = Tab}) ->
    ets:delete(Tab, {Partition, From}),
    {reply, ok, State};
handle_call({acquire, Partition, Key}, From, State) ->
    process_acquire_results(load_backend(Partition, Key, State, From));
handle_call({list_backends, Partition}, _Form, State = #state{backends_table = Tab}) ->
    do_list_backends(Partition, Tab, State);
handle_call({drop_partition, Partition}, _From, State = #state{backends_table = Tab}) ->
    Reply = do_list_backends(Partition, Tab, State),
    ets:match_delete(Tab, #backend_info{_='_', partition = Partition}),
    Reply.

do_list_backends(Partition, Tab, State) ->
    Spec = [{
        [#backend_info{_ = '_', backend_state = '$1', partition = '$2', start_timestamp = '$3', end_timestamp = '$4'}],
        [{'=:=', '$2', Partition}],
        [{'$1', {'$3', '$4'}}]
    }],
    R = ets:select(Tab, Spec),
    {reply, R, State}.


handle_cast({release, Partition, {From, _To}, NewBackendRef}, State = #state{backends_table = Tab, wait_queue = WaitQueue}) ->
    Key = {From, Partition},
    I = ets:lookup(Tab, Key),
    [#backend_info{ref_count = Cnt}] = I,
    Upd0 = [{#backend_info.ref_count, Cnt - 1}],
    Upd = if 
              NewBackendRef =/= undefined ->
                  [{#backend_info.backend_state, NewBackendRef} | Upd0];
              true ->
                  Upd0
          end,
    ets:update_element(Tab, Key, Upd),
    {UpdState, UpdWaitQueue} = lists:foldl(
        fun(R = {{Partition, From}, Ref, Pid}, {CurrState, CurrWaitQueue}) ->
            case load_backend(Partition, {From, undefined}, CurrState, undefined) of
                {ok, Loaded, NewState} ->
                    Pid ! {Ref, ok, Loaded},
                    {NewState, CurrWaitQueue};
                {busy, undefined, NewState} ->
                    {NewState, [R | CurrWaitQueue]};
                {error, Reason, NewState} ->
                    Pid ! {Ref, error, Reason},
                    {NewState, CurrWaitQueue}
            end
        end, {State, []}, WaitQueue),
    {noreply, UpdState#state{wait_queue = UpdWaitQueue}}.

handle_info(_Info, _State) ->
    erlang:error(not_implemented).

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% PRIVATE FUNCTIONS

mk_key(#backend_info{partition = P, start_timestamp = S} = I) ->
    I#backend_info{key = {S, P}}.

load_backend(Partition, {From, _To}, State = #state{backends_table = Tab}, Pid) ->
    Key = {From, Partition},
    case ets:lookup(Tab, Key) of
        [I] ->
            load_existing_backend(I, Key, Partition, State, Pid);
        [] ->
            {error, does_not_exist, State}
    end.

load_existing_backend(I, Key, Partition, State = #state{backends_table = Tab, current_loaded_backends = CurrLoaded,
    max_loaded_backends = MaxLoaded, wait_queue = WaitQueue}, Pid) ->
    #backend_info{backend_state = BackendState, path = Path, opts = Config, module = Mod, ref_count = RefCnt} = I,
    if
        BackendState == undefined ->
            SupersedeRes = if
                               CurrLoaded >= MaxLoaded ->
                                   {supersede_backends(1, Tab, Mod), MaxLoaded};
                               true ->
                                   {ok, CurrLoaded + 1}
                           end,
            case SupersedeRes of
                {ok, NewLoadedCnt} ->
                    BackendConfig = lists:keyreplace(data_root, 1, Config, {data_root, Path}),
                    case Mod:init(Partition, BackendConfig) of
                        {ok, S} ->
                            Loaded = I#backend_info{backend_state = S, last_accessed = erlang:now(), ref_count = 1},
                            ets:insert(Tab, Loaded),
                            {ok, Loaded, State#state{current_loaded_backends = NewLoadedCnt}};
                        {error, Reason} ->
                            {error, {backend_load_failed, Reason}, State}
                    end;
                {not_avaliable, LoadedCnt} when is_pid(Pid)  ->
                    Ref = make_ref(),
                    NewState = State#state{wait_queue = WaitQueue ++ [{Key, Ref, Pid}], current_loaded_backends = LoadedCnt},
                    {busy, Ref, NewState};
                {not_avaliable, LoadedCnt} when not is_pid(Pid)  ->
                    {busy, undefined, LoadedCnt};
                Else ->
                    {error, Else, State}
            end;
        true ->
            Ts = erlang:now(),
            ets:update_element(Tab, Key, [{#backend_info.last_accessed, Ts}, {#backend_info.ref_count = RefCnt + 1}]),
            {I#backend_info{last_accessed = Ts}, State}
    end.

supersede_backends(1, Backends, BackendModule) ->
    InfTs = {inf, inf, inf},
    Oldest = ets:foldl(
        fun
            (#backend_info{last_accessed = undefined}, I) ->
                I;
            (I = #backend_info{last_accessed = A, ref_count = 0}, #backend_info{last_accessed = B}) when A < B ->
                I;
            (_, I) ->
                I
        end,
        #backend_info{last_accessed = InfTs}, Backends),
    case Oldest of
        #backend_info{last_accessed = InfTs} ->
            not_avalibale;
        #backend_info{backend_state = State} ->
            ok = BackendModule:stop(State),
            Updated = Oldest#backend_info{backend_state = undefined, last_accessed = undefined, ref_count = 0},
            ets:insert(Backends, Updated),
            ok
    end.

process_acquire_results({ok, BackendState, NewState}) ->
    {reply, {ok, BackendState}, NewState};
process_acquire_results({busy, WaitObj, NewState}) ->
    {reply, {busy, WaitObj}, NewState};
process_acquire_results({error, Reason, NewState}) ->
    {reply, {error, Reason}, NewState}.

