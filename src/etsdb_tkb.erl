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
		 sort/1,
		 api_version/0,
		 w_val/0,
		 r_val/0,
		 w_quorum/0,
		 r_quorum/0,
		 make_partitions/1,
		 serialize/2,
		 scan_partiotions/2,
		 scan_spec/3,
		 join_scan/2,test_fun/0,unserialize_result/1]).

-behaviour(etsdb_bucket).

-author('Alex G. <gunin@mail.mipt.ru>').

-define(REGION_SIZE,86400000). %%One Day

-define(PREFIX,"etsdb_tkb"). %%One Day

api_version()->
	"0.1".

w_val()->
	3.
r_val()->
	3.
w_quorum()->
	2.
r_quorum()->
	2.

sort(Data)->
	lists:keysort(1,Data).
make_partitions(Datas) when is_list(Datas)->
	[{make_partition(Data),Data}||Data<-Datas];
make_partitions(Data)->
	[{make_partition(Data),Data}].

make_partition({{ID,Time},_Value})->
	TimeRegion = Time div ?REGION_SIZE,
	partiotion_by_region(ID,TimeRegion).

scan_partiotions({ID,Time1},{ID,Time2}) when Time1>Time2->
	{{ID,Time1},{ID,Time1},[]};
scan_partiotions({ID,OriginalTime1},{ID,OriginalTime2})->
	Now = etsdb_util:system_time(),
	Time1 = max(OriginalTime1,Now-?REGION_SIZE*365),
	Time2 = min(OriginalTime2,Now+?REGION_SIZE*3),
	FromTimeRegion = Time1 div ?REGION_SIZE,
	ToTimeRegion = Time2 div ?REGION_SIZE,
	{{ID,Time1},
	 {ID,Time2},
	 lists:usort([partiotion_by_region(ID,TimeRegion)||TimeRegion<-lists:seq(FromTimeRegion,ToTimeRegion)])};
scan_partiotions(From,To)->
	{From,To,[]}.

serialize(Datas,ForBackEnd) when is_list(Datas)->
	[serialize_internal(Data,ForBackEnd)||Data<-Datas];
serialize(Data,ForBackEnd)->
	serialize_internal(Data,ForBackEnd).

serialize_internal({{ID,Time},Value},etsdb_leveldb_backend)->
	{<<?PREFIX,ID:64/integer,Time:64/integer>>,<<Value/binary>>};
serialize_internal({{ID,Time},Value},_ForBackEnd)->
	{{ID,Time},Value}.
partiotion_by_region(ID,TimeRegion)->
	<<ID:64/integer,TimeRegion:64/integer>>.



scan_spec({ID,From},{ID,To},_BackEnd)->
	StartKey = <<?PREFIX,ID:64/integer,From:64/integer>>,
	StopKey = <<?PREFIX,ID:64/integer,To:64/integer>>,
	Fun = fun
			 ({K,_}=V, Acc) when K >= StartKey andalso K =< StopKey ->	  
				[V|Acc];
			 (_V, Acc)->
				  throw({break,Acc})
		  end,
	{StartKey,Fun}.

unserialize_result(R)->
	lists:foldr(fun(V,Acc)->
						case unserialize_internal(V) of
							skip->
								Acc;
							{error,not_object}->
								Acc;
							KV->
								[KV|Acc]
						end end,[],R).
unserialize_internal({_,<<"deleted">>})->
	skip;
unserialize_internal({<<?PREFIX,ID:64/integer,Time:64/integer>>,<<Value/binary>>})->
	{{ID,Time},Value};
unserialize_internal(_)->
	{error,not_object}.

join_scan(A1,A2)->
	orddict:merge(fun(_,V1,_)->V1 end,A1,A2).
test_fun()->
	fun
	   (K) when K<1->
			ok;
	   (_)->
		 error
	end.