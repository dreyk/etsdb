%%%-------------------------------------------------------------------
%%% @author lol4t0
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%% Core node services supervisor
%%% @end
%%% Created : 05. Feb 2015 11:45
%%%-------------------------------------------------------------------
-module(etsdb_node_core_sup).
-author("lol4t0").

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

-spec start_link() -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    RestartStrategy = rest_for_one,
    MaxRestarts = 5,
    MaxSecondsBetweenRestarts = 10,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},
    
    BackendManagerConfig = app_helper:get_env(etsdb, backend_manager, []),
    
    BackendManager = {
        etsdb_backend_manager,
        {etsdb_backend_manager, start_link, [BackendManagerConfig]},
        permanent, 5000, worker, [etsdb_backend_manager]
    },

    %% Start riak_core vnode master.See docimentation on riak_core.
    VMaster = {
        etsdb_vnode_master,
        {riak_core_vnode_master, start_link, [etsdb_vnode]},
        permanent, 5000, worker, [riak_core_vnode_master]
    },


    {ok, {SupFlags, [BackendManager, VMaster]}}.

