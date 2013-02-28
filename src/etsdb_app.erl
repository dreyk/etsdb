%% -------------------------------------------------------------------
%%
%% etsdb: application startup for tsdb extension to Riak.
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

%% @doc Bootstrapping the etsdb extension application.
-module(etsdb_app).
-author('Alex G. <gunin@mail.mipt.ru>').

-behaviour(application).

-export([
	 start/2,
	 stop/1,
	 start_test/0]).


-spec start(Type::term(), StartArgs::term())-> {ok,pid()} | ignore | {error,Error::term()}.
%% @doc The application:start callback for etsdb.
%%      Arguments are ignored as all configuration is done via the erlenv file.
start(_Type, _StartArgs) ->
	%% Add user-provided code paths to vm
	case app_helper:get_env(etsdb, add_paths) of
		List when is_list(List) ->
			ok = code:add_paths(List);
		_ ->
			ok
	end,
	%%Start main supervisor
	case etsdb_sup:start_link() of
		{ok, Pid} ->
			init_riak_core_services(),
			%%ok = riak_api_pb_service:register([{etsdb_pb_client,10,13}]),
			{ok, Pid};
		Error ->
			Error
	end.

stop(_State) ->
	lager:info("Stopped  application etsdb.\n", []),
	ok.

%%Init riak_core, rigister etsdb in ring.
init_riak_core_services()->
	ok = riak_core:register(etsdb,[{vnode_module,etsdb_vnode}]).

%%Start app in command line.
%%You may use this fun for debug.
start_test()->
	erlang:spawn(fun()->start_test_inner() end).
start_test_inner() ->
	ok = application:start(sasl),
	ok = application:start(os_mon),
    ok = application:start(crypto),
	ok = application:start(compiler),
	ok = application:start(syntax_tools),
	ok = application:start(lager),
    ok = application:start(riak_sysmon),
	ok = application:start(inets),
	ok = application:start(public_key),
	ok = application:start(ssl),
	ok = application:start(xmerl),
	ok = application:start(mochiweb),
    ok = application:start(webmachine),
	ok = application:start(riak_core),
	ok = application:start(riak_api),
	ok = application:start(etsdb).
