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

%% @doc Simple implementation tsdb bucket for storing any data in {id,time} order
-module(etsdb_tkb).



-export([serialize/1,
		 unserialize/2,
		 api_version/0,
		 w_val/0,
		 r_val/0,
		 quorum/0,
		 merge_conflict/3,
		 fold/2,partition/1,serialize_key/1]).

-behaviour(etsdb_bucket).
-author('Alex G. <gunin@mail.mipt.ru>').

api_version()->
	"0.1".

serialize_key({ID,Time})->
	<<ID:64/integer,Time:64/integer>>.
serialize({{ID,Time},Value})->
	Key = <<ID:64/integer,Time:64/integer>>,
	{Key,term_to_binary(Value)}.
unserialize(<<ID:64/integer,Time:64/integer>>,Value)->
	{{ID,Time},binary_to_term(Value)}.

w_val()->
	3.
r_val()->
	3.
quorum()->
	2.
partition({{_ID,Time},_Value})->
	Time div 36000000.

merge_conflict(_K,V1,_V2)->
	V1.

fold(_K,_V)->
	ok.