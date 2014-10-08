%% -------------------------------------------------------------------
%%
%%
%% Copyright (c) Dreyk.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.

%% @doc Etsdb main supervisour.
-module(etsdb_sup).
-author('Alex G. <gunin@mail.mipt.ru>').


-behaviour(supervisor).


-export([start_link/0]).


-export([init/1]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    %%Start riak_core vnode master.See docimentation on riak_core.
    VMaster = {etsdb_vnode_master,
               {riak_core_vnode_master, start_link, [etsdb_vnode]},
               permanent, 5000, worker, [riak_core_vnode_master]},
    ClientWorkerPoolArgs = [{name, {local,etsdb_client_worker}},
                            {worker_module,etsdb_client_worker},
                            {size, 100},
                            {max_overflow,0}
                           ],
    ClirntWorkerPool = {etsdb_client_worker, {poolboy, start_link, [ClientWorkerPoolArgs]},
                        permanent, 5000, worker, [poolboy]},
    ProxySupervisor = {etsdb_vnode_put_proxy_sup,
        {etsdb_vnode_put_proxy_sup, start_link, []},
        permanent, 5000, supervisor, [etsdb_vnode_put_proxy_sup]},
    AeeSupervisor = {etsdb_aee_sup,
                       {etsdb_aee_sup, start_link, []},
                       permanent, 5000, supervisor, [etsdb_aee_sup]},
    All = [VMaster,ClirntWorkerPool,ProxySupervisor, AeeSupervisor],
    { ok,
        { {one_for_one, 5, 10},
          All}}.
