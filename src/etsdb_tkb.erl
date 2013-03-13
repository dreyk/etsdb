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
		 join_scan/2,
		 unserialize_result/1,
		 expire_spec/1,
		 serialize/1,
		 clear_period/0]).

-behaviour(etsdb_bucket).

-author('Alex G. <gunin@mail.mipt.ru>').

-define(REGION_SIZE,86400000). %%One Day

-define(LIFE_TIME,60*60*1000). %%One hour

-define(MAX_EXPIRED_COUNT,10000).

-define(CLEAR_PERIOD,60*1000). %%One minutes

-define(PREFIX,"etsdb_tkb").
-define(PREFIX_REV,"etsdb_tkb_rev").

-define(USE_BACKEND,etsdb_leveldb_backend). %%One Day

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

clear_period()->
	?CLEAR_PERIOD.

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

serialize(Datas)->
	serialize(Datas,?USE_BACKEND).

serialize(Datas,ForBackEnd) when is_list(Datas)->
	Batch = lists:foldl(fun(Data,Acc)->
						Record = serialize_internal(Data,ForBackEnd),
						RevRecord = serialize_internal_rev(Data,ForBackEnd),
						[Record,RevRecord|Acc] end,[],Datas),
	lists:keysort(1,Batch);
	
serialize(Data,ForBackEnd)->
	[serialize_internal(Data,ForBackEnd),serialize_internal_rev(Data,ForBackEnd)].

serialize_internal({{ID,Time},Value},etsdb_leveldb_backend)->
	{<<?PREFIX,ID:64/integer,Time:64/integer>>,<<Value/binary>>};
serialize_internal({{ID,Time},Value},_ForBackEnd)->
	{{?PREFIX,ID,Time},Value}.
serialize_internal_rev({{ID,Time},_Value},etsdb_leveldb_backend)->
	{<<?PREFIX_REV,Time:64/integer,ID:64/integer>>,<<?PREFIX,ID:64/integer,Time:64/integer>>};
serialize_internal_rev({{ID,Time},_Value},_ForBackEnd)->
	{{?PREFIX_REV,Time,ID},{?PREFIX,ID,Time}}.
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

expire_spec(_BackEnd)->
	ExparationTime = etsdb_util:system_time()-?LIFE_TIME,
	StartKey = <<?PREFIX_REV,0:64/integer,0:64/integer>>,
	StopKey = <<?PREFIX_REV,ExparationTime:64/integer,0:64/integer>>,
	Fun = fun
			 ({K,V}, {Count,Acc}) when K >= StartKey andalso K =< StopKey->
				  if
					  Count<?MAX_EXPIRED_COUNT->
							{Count+1,[K,V|Acc]};
					  true->
						  throw({break,{{continue,Count},Acc}})
				  end;
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