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
         clear_period/0,
         scan_partiotions/1,
         key_ranges/0,
         timestamp_for_keys/1]).

-behaviour(etsdb_bucket).

-author('Alex G. <gunin@mail.mipt.ru>').

-define(REGION_SIZE,86400000). %%One Day

-define(LIFE_TIME,86400000*7). %%One week

-define(MAX_EXPIRED_COUNT,10000).

-define(CLEAR_PERIOD,300*1000). %%Five minutes

-define(PREFIX,"pv").
-define(PREFIX_REV,"pv_rev").

-define(USE_BACKEND,etsdb_leveldb_backend).

-include("etsdb_request.hrl").

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

scan_partiotions(#scan_it{rgn = TimeRegion,end_rgn = TimeRegion})->
    empty;
scan_partiotions(#scan_it{rgn = TimeRegion,from={ID,_}}=It)->
    Rgn = TimeRegion+1,
    It#scan_it{rgn=Rgn,partition=partiotion_by_region(ID,Rgn)}.
scan_partiotions({ID,Time1},{ID,Time2}) when Time1>Time2->
    empty;
scan_partiotions({ID,Time1},{ID,Time2})->
    Now = etsdb_util:system_time(),
    From = max(Now-?LIFE_TIME,Time1),
    FromTimeRegion = From div ?REGION_SIZE,
    ToTimeRegion = Time2 div ?REGION_SIZE,
    #scan_it{rgn_count=ToTimeRegion-FromTimeRegion+1,
             rgn = FromTimeRegion,
             partition=partiotion_by_region(ID,FromTimeRegion),
             from={ID,From},to={ID,Time2},start_rgn=FromTimeRegion,end_rgn=ToTimeRegion};
scan_partiotions(_From,_To)->
        empty.

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
    Key = sext:encode({?PREFIX,ID,Time}),
    {Key,<<Value/binary>>};
serialize_internal({{ID,Time},Value},_ForBackEnd)->
    {{?PREFIX,ID,Time},Value}.
serialize_internal_rev({{ID,Time},_Value},etsdb_leveldb_backend)->
    Key = sext:encode({?PREFIX,ID,Time}),
    IKey = sext:encode({?PREFIX_REV,Time,ID}),
    {IKey,Key};
serialize_internal_rev({{ID,Time},_Value},_ForBackEnd)->
    {{?PREFIX_REV,Time,ID},{?PREFIX,ID,Time}}.
partiotion_by_region(ID,TimeRegion)->
    <<ID/binary,TimeRegion:64/integer>>.


scan_spec({ID,From},{ID,To},_BackEnd)->
    StartKey = sext:encode({?PREFIX,ID,From}),
    StopKey = sext:encode({?PREFIX,ID,To}),
    Fun = fun
             ({K,_}=V, Acc) when K >= StartKey andalso K =< StopKey ->      
                [V|Acc];
             (_V, Acc)->
                  throw({break,Acc})
          end,
    {StartKey,Fun}.

expire_spec(_BackEnd)->
    ExparationTime = etsdb_util:system_time()-?LIFE_TIME,
    StartKey = sext:encode({?PREFIX_REV,0,<<>>}),
    StopKey = sext:encode({?PREFIX_REV,ExparationTime,<<>>}),
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
unserialize_internal({Key,Value})->
    case sext:decode(Key) of
        {?PREFIX,ID,Time}->
            {{ID,Time},Value};
        _->
            {error,not_object}
    end;
unserialize_internal(_)->
    {error,not_object}.

join_scan(A1,A2)->
    A1++A2.

key_ranges() ->
    Now = etsdb_util:system_time(),
    MinKey = sext:encode({?PREFIX, <<>>, Now - ?LIFE_TIME}),
    MaxKey = sext:encode({"pw", <<>>, Now + 1}),

    MinIKey = sext:encode({?PREFIX_REV,Now - ?LIFE_TIME, <<>>}),
    MaxIKey = sext:encode({?PREFIX_REV,Now + 1, <<>>}),
    Keys = {MinKey, MaxKey},
    IKeys = {MinIKey, MaxIKey},
    lists:sort([Keys, IKeys]).

timestamp_for_keys(Keys) ->
    lists:map(
        fun(Key) ->
            Time = case sext:decode(Key) of
                {?PREFIX, _Id, TS} ->
                    TS;
                {?PREFIX_REV, TS, _Id} ->
                    TS
            end,
            {Key, Time}
        end, Keys).
