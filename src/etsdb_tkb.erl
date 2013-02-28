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



-export([
		 api_version/0,
		 w_val/0,
		 r_val/0,
		 quorum/0,
		 make_partitions/1,
		 serialize/2,
		 scan_partiotions/2]).

-behaviour(etsdb_bucket).

-author('Alex G. <gunin@mail.mipt.ru>').

-define(REGION_SIZE,36000000). %%One hour

api_version()->
	"0.1".

w_val()->
	3.
r_val()->
	3.
quorum()->
	2.

make_partitions(Datas) when is_list(Datas)->
	[{make_partition(Data),Data}||Data<-Datas];
make_partitions(Data)->
	[{make_partition(Data),Data}].

make_partition({{ID,Time},_Value})->
	TimeRegion = Time div ?REGION_SIZE,
	partiotion_by_region(ID,TimeRegion).

scan_partiotions({ID,Time1},{ID,Time2})->
	FromTimeRegion = Time1 div ?REGION_SIZE,
	ToTimeRegion = Time2 div ?REGION_SIZE,
	lists:usort([partiotion_by_region(ID,TimeRegion)||TimeRegion<-lists:seq(FromTimeRegion,ToTimeRegion)]).

serialize(Data,ForBackEnd) when is_list(Data)->
	[serialize_internal(Data,ForBackEnd)||Data];
serialize(Data,ForBackEnd)->
	serialize_internal(Data,ForBackEnd).

serialize_internal({{ID,Time},Value},etsdb_leveldb_backend)->
	Value1 = term_to_binary(Value),
	{<<ID:64/integer,Time:64/integer>>,<<1,Value1/binary>>};
serialize_internal({{ID,Time},Value},_ForBackEnd)->
	{{ID,Time},{1,Value}}.
partiotion_by_region(ID,TimeRegion)->
	<<ID:64/integer,TimeRegion:64/integer>>.
