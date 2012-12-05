%% -------------------------------------------------------------------
%%
%% etsdb_ets_backend: ets table backend for etsdb. 
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

%% @doc etsdb_ets_backend it's ets table backend.
-module(etsdb_ets_backend).


-export([init/2,
		 save/2]).

-define(ETS(I), list_to_atom("ets_tsdb_"++integer_to_list(I))).

-record(state,{table}).

init(Index,_Conf)->
	Tid = ets:new(?ETS(Index),[ordered_set,protected,named_table,{read_concurrency,true}]),
	{ok,#state{table=Tid}}.


save(O,#state{table=Tab}=State)->
	Res = case catch ets:insert(Tab,O) of
			  true->
				  ok;
			  Else->
				  {error,Else}
		  end,
	{Res,State}.

