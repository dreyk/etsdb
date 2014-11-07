%%%-------------------------------------------------------------------
%%% @author sidorov
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 08. Oct 2014 4:55 PM
%%%-------------------------------------------------------------------
-module(etsdb_aee_sup).
-author("sidorov").

-behaviour(supervisor).

%% API
-export([start_link/0, start_aee/1, stop_aee/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

-type index() :: non_neg_integer().

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the antientropy exchange supervisor
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).


%% @doc
%% Starts new aee worker for the specific vnode
%%
-spec start_aee(index()) -> supervisor:startchild_ret().
start_aee(Index) ->
    supervisor:start_child(?SERVER, [Index]).


%% @doc
%% Stops specified aee worker
-spec stop_aee(index()) -> ok | {error, term()}.
stop_aee(Index) ->
    case catch etsdb_aee:lookup(Index) of
        Pid when is_pid(Pid) ->
            supervisor:terminate_child(?SERVER, Pid);
        Else ->
            {error, Else}
    end.


%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(),
                       MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
          [ChildSpec :: supervisor:child_spec()]
    }} |
    ignore |
    {error, Reason :: term()}).
init([]) ->
    {ok, Opts} = application:get_env(etsdb, aee),
    {ok,
        {
            {simple_one_for_one, 10, 10},
            [{etsdb_aee,
                {etsdb_aee, start_link, [Opts]},
                permanent, 5000, supervisor, [etsdb_aee]}
            ]
        }
    }.

%%%===================================================================
%%% Internal functions
%%%===================================================================
