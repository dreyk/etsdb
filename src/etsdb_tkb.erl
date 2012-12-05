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

%% @doc Simple implementation tsdb bucket for storing some data in {id,time} order
-module(etsdb_tkb).



-export([]).

-behaviour(etsdb_bucket).

api_version()->
	"0.1".
serialize({{ID,Time},Value})->
	Key = <<ID:64/integer,Time:64/integer>>,
	{Key,term_to_binary(Value)}.
unserialize(<<ID:64/integer,Time:64/integer>>,Value)->
	{{ID,Time},binary_to_term(Value)}.

key({K,_Value})->
	K.
value({_K,Value})->
	Value.
w_val()->
	3.
r_val()->
	3.
quorum()->
	2.
partition({{_ID,Time},_Value},Count)->
	Interval = round(60*60*1000/Count),
	(Time rem Interval) +1.
merge_conflict(K,V1,_V2)->
	V1.

fold(K,V)->
	ok.