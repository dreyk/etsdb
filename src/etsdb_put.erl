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

-export([put/2,put/3,prepare_data/2,test_console/1]).

-include("etsdb_request.hrl").

put(Bucket,Data)->
    put(Bucket,Data,?DEFAULT_TIMEOUT).

put(_Bucket,[],_Timeout)->
    ok;
put(Bucket,Data,Timeout)->
    dyntrace:p(0,0, "etsdb_put:put"),
    dyntrace:p(0,0, "etsdb_put:prepare_data"),
    PartitionedData = prepare_data(Bucket,Data),
    dyntrace:p(1,0, "etsdb_put:prepare_data"),
    ReqRef = make_ref(),
    Me = self(),
    etsdb_mput_fsm:start_link({raw,ReqRef,Me}, Bucket, PartitionedData, Timeout),
    Res = wait_for_results(ReqRef,client_wait_timeout(Timeout)),
    dyntrace:p(1,0, "etsdb_put:put"),
    Res.


prepare_data(Bucket,Data)->
    Partitioned = Bucket:make_partitions(Data),
    %%DatasByUserPartition = join_partiotions(Partitioned),
    {ok,Ring} = riak_core_ring_manager:get_my_ring(),
    batch_partitions(Bucket,Ring,Partitioned,[]).

batch_partitions(Bucket,_,[],Acc)->
    dyntrace:p(0,0, "etsdb_put:join part"),
    Acc1 = join_partiotions(Bucket,Acc),
    dyntrace:p(1,0, "etsdb_put:join part"),
    Acc1;
batch_partitions(Bucket,Ring,[{{vidx,VnodeIdx},Data}|T],Acc)->
    batch_partitions(Bucket,Ring,T,[{VnodeIdx,Data}|Acc]);
batch_partitions(Bucket,Ring,[{Partition,Data}|T],Acc)->
    dyntrace:p(0,0, "etsdb_put:hash"),
    Idx = crypto:hash(sha,Partition),
    dyntrace:p(1,0, "etsdb_put:hash"),
    dyntrace:p(0,0, "etsdb_put:responsible_index"),
    VnodeIdx=riak_core_ring:responsible_index(Idx,Ring),
    dyntrace:p(1,0, "etsdb_put:responsible_index"),
    dyntrace:p(0,0, "etsdb_put:partition_hash"),
    VNodeHash = etsdb_util:hash_for_partition(VnodeIdx),
    dyntrace:p(1,0, "etsdb_put:partition_hash"),
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

test_console(Node)->
    AF1 = fun(J,C)-> [{demo_data,I,I+1,10,10,I}||I<-lists:seq(J*C+1,J*C+C)] end,
    [rpc:call(Node,etsdb_put,put,[demo_geohash,AF1(J,100),6000])||J<-lists:seq(1,10)].