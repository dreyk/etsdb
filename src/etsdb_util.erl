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
-module(etsdb_util).


-export([num_partiotions/0]).

-export([reduce_orddict/2,
         make_error_response/1,
         hash_for_partition/1,
         system_time/0,
         system_time/1,
         system_time/2,
         random_int/1]).

num_partiotions()->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:num_partitions(Ring).


reduce_orddict(_ReduceFun,[])->
    [];
reduce_orddict(ReduceFun,OrdDict)->
    R = lists:foldl(fun({K,V},Acc)->
                            case Acc of
                                {K,ValAcc,Acc1}->
                                    {K,ReduceFun(V,ValAcc),Acc1};
                                {M,ValAcc,Acc1}->
                                    {K,ReduceFun(V,'$start'),[{M,ReduceFun('$end',ValAcc)}|Acc1]};
                                undefined->
                                    {K,ReduceFun(V,'$start'),[]}
                            end end,undefined,OrdDict),
    case R of
        undefined->
            [];
        {K,ValAcc,Acc}->
            lists:reverse([{K,ReduceFun('$end',ValAcc)}|Acc]);
        _->
            []
    end.

make_error_response({error,_}=E)->
    E;
make_error_response(E)->
    {error,E}.

hash_for_partition(0) ->
    <<(trunc(math:pow(2,160))-1):160/integer>>;
hash_for_partition(I) ->
    <<(I-1):160/integer>>.
    
system_time()->
    system_time(millisec).
system_time(millisec)->
    system_time(millisec,os:timestamp());
system_time(sec)->
    system_time(sec,os:timestamp()).
system_time(millisec,{Mega,S,Micro})->
    (Mega*1000000+S)*1000+(Micro div 1000);
system_time(sec,{Mega,S,_})->
    Mega*1000000+S.

random_int(Limit) when is_integer(Limit) andalso Limit > 0 -> 
        {_, Sec, Micro} = os:timestamp(), 
        {Random, _} = random:uniform_s(Limit, {Micro rem 32768, Sec rem 32768, 0}), 
        Random; 

random_int(_) -> 
        0.