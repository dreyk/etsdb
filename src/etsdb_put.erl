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
-module(etsdb_put).


-define(DEFAULT_TIMEOUT,60000).

-export([put/2,put/3,prepare_data/2]).

-include("etsdb_request.hrl").

put(Bucket,Data)->
    put(Bucket,Data,?DEFAULT_TIMEOUT).

put(_Bucket,[],_Timeout)->
    ok;
put(Bucket,Data,Timeout)->
    PartitionedData = prepare_data(Bucket,Data),
    ReqRef = make_ref(),
    Me = self(),
    etsdb_mput_fsm:start_link({raw,ReqRef,Me}, Bucket, PartitionedData, Timeout),
    wait_for_results(ReqRef,client_wait_timeout(Timeout)).


prepare_data(Bucket,Data)->
    Partitioned = Bucket:make_partitions(Data),
    {ok,Ring} = riak_core_ring_manager:get_my_ring(),
    batch_partitions(Bucket,Ring,Partitioned,[]).

batch_partitions(Bucket,_,[],Acc)->
    join_partiotions(Bucket,Acc);
batch_partitions(Bucket,Ring,[{{vidx,VnodeIdx},Data}|T],Acc)->
    batch_partitions(Bucket,Ring,T,[{VnodeIdx,Data}|Acc]);
batch_partitions(Bucket,Ring,[{Partition,Data}|T],Acc)->
    Idx = crypto:hash(sha,Partition),
    VnodeIdx=riak_core_ring:responsible_index(Idx,Ring),
    VNodeHash = etsdb_util:hash_for_partition(VnodeIdx),
    batch_partitions(Bucket,Ring,T,[{VNodeHash,Data}|Acc]).

join_partiotions(Bucket,Partitioned)->
    SortByPartition = lists:keysort(1,Partitioned),
    etsdb_util:reduce_orddict(fun(A1,A2)->merge_user_data(Bucket,A1,A2) end,SortByPartition).

merge_user_data(_Bucket,Data,'$start')->
    [Data];
merge_user_data(Bucket,'$end',Acc)->
    Bucket:serialize(Acc);
merge_user_data(_Bucket,Data,Acc)->
    [Data|Acc].

wait_for_results(ReqRef,Timeout)->
    receive 
        {ReqRef,Res}->
            Res;
        {_OldReqRef,_OldRes}->
            wait_for_results(ReqRef,Timeout)
    after Timeout->
            {error,timeout}
    end.

%%Add 50ms to operation timeout
client_wait_timeout(Timeout)->
    Timeout + 50.