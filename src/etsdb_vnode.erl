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
		 init/1,
		 handle_command/3]).

-behaviour(riak_core_vnode).

-include("etsdb_request.hrl").

-record(state,{delete_mod,vnode_index,backend,backend_ref}).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, etsdb_vnode).

init([Index]) ->
    DeleteMode = app_helper:get_env(etsdb, delete_mode, 3000),
	{BackEndModule,BackEndProps} = app_helper:get_env(etsdb, backend,{etsdb_ets_backend,[]}),
	case BackEndModule:init(Index,BackEndProps) of
		{ok,Ref}->
    		{ok,#state{vnode_index=Index,delete_mod=DeleteMode,backend=BackEndModule,backend_ref=Ref}};
		{error,Else}->
			{error,Else}
	end.

handle_command(?ETSDB_INNERSTORE_REQ{value=Value,req_id=ReqID}, Sender,State)->
	 save_internal(Sender,Value,ReqID,State);
handle_command(?ETSDB_STORE_REQ{bucket=Bucket,value=Value,timestamp=Time,req_id=ReqID}, Sender,State)->
	ObjectToStore = etsdb_object:make_object(Bucket,Value, Time),
	save_internal(Sender,ObjectToStore,ReqID,State).

save_internal(Sender,Value,ReqID,#state{backend=BackEndModule,backend_ref=BackEndRef,vnode_index=Index}=State)->
	{Result,NewBackEndRef} = BackEndModule:save(Value,BackEndRef),
	riak_core_vnode:reply(Sender, {w,Index,ReqID,Result}),
	{ok,State#state{backend_ref=NewBackEndRef}}.
