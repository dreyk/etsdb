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

%% @doc it's ets table backend.
-module(etsdb_ets_backend).


-export([init/2,
		 save/3,cell/4]).

-define(ETS(I), list_to_atom("ets_tsdb_"++integer_to_list(I))).

-record(state,{table}).

init(Index,_Conf)->
	Tid = ets:new(?ETS(Index),[ordered_set,protected,named_table,{read_concurrency,true}]),
	{ok,#state{table=Tid}}.


save(B,{batch,UserObjects},State)->
	Objects = [{{B,K},V}||{K,V}<-UserObjects],
	put_obj(Objects, State);
	
save(B,{K,V},State)->
	put_obj({{B,K},V}, State).
	

cell(Bucket,Filter,Key,#state{table=Tab})->
	find_cell({Bucket,Key},Filter,Tab).
				
find_cell({Bucket,_}=Key,Filter,Tab)->
	case get_obj(Key,Tab) of
		{ok,not_found}->
			case ets:next(Tab,Key) of
				'$end_of_table'->
					{ok,not_found};
				{Bucket,UserKey}->
					find_cell({Bucket,UserKey},Filter,Tab)
			end;
		{ok,{{_,UserKey},V}}->
			case Bucket:fold_cell(UserKey,V,Filter) of
				ok->
					{ok,{UserKey,V}};
				stop->
					{ok,not_found};
				_->
					find_cell({Bucket,UserKey},Filter,Tab)
			end
	end.

get_obj(Key,Tab)->
	case ets:lookup(Tab,Key) of
		[Object]->
			{ok,Object};
		_->			
			{ok,not_found}
	end.

put_obj(O,#state{table=Tab}=State)->
	Res = case catch ets:insert(Tab,O) of
			  true->
				  ok;
			  Else->
				  {error,Else}
		  end,
	{Res,State}.