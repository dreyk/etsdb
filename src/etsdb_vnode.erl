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

%% @doc etsdb riak_core_vnode implementation
-module(etsdb_vnode).
-author('Alex G. <gunin@mail.mipt.ru>').

-export([start_vnode/1,
		 init/1]).

-behaviour(riak_core_vnode).

-record(state,{delete_mod,vnode_index}).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, etsdb_vnode).

init([Index]) ->
    DeleteMode = app_helper:get_env(etsdb, delete_mode, 3000),
    {ok,#state{vnode_index=Index,delete_mod=DeleteMode}}.
