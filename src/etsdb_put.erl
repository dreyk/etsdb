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

put(Bucket,Data,Timeout)->
	PartitionedData = prepare_data(Bucket,Data),
	do_put(Bucket,PartitionedData,Timeout,#etsdb_store_res_v1{count=0,error_count=0,errors=[]}).

do_put(_Bucket,[],_Timeout,Results)->
	Results;
do_put(Bucket,[{Partition,Data}|T],Timeout,Results)->
	ReqRef = make_ref(),
	Me = self(),
	PartionIdx  = etsdb_util:hash_for_partition(Partition),
	etsdb_put_fsm:start_link({raw,ReqRef,Me},PartionIdx, Bucket, Data, Timeout),
	ResultsNew = case wait_for_results(ReqRef,client_wait_timeout(Timeout)) of
		ok->
			merge_store_result(length(Data),Results);
		Else->
			merge_store_result(length(Data),etsdb_util:make_error_response(Else),Results)
	end,
	do_put(Bucket,T, Timeout,ResultsNew).

prepare_data(Bucket,Data)->
	Partitioned = Bucket:make_partitions(Data),
	%%DatasByUserPartition = join_partiotions(Partitioned),
	{ok,Ring} = riak_core_ring_manager:get_my_ring(),
	batch_partitions(Ring,Partitioned,[]).

batch_partitions(_,[],Acc)->
	join_partiotions(Acc);
batch_partitions(Ring,[{Partition,Data}|T],Acc)->
	Idx = crypto:sha(Partition),
	VnodeIdx=riak_core_ring:responsible_index(Idx,Ring),
	batch_partitions(Ring,T,[{VnodeIdx,Data}|Acc]).

join_partiotions(Partitioned)->
	SortByPartition = lists:keysort(1,Partitioned),
	etsdb_util:reduce_orddict(fun merge_user_data/2,SortByPartition).

merge_user_data(Data,'$start')->
	[Data];
merge_user_data('$end',Acc)->
	lists:reverse(Acc);
merge_user_data(Data,Acc)->
	[Data|Acc].

wait_for_results(ReqRef,Timeout)->
	receive 
		{ReqRef,Res}->
			Res
	after Timeout->
			{error,timeout}
	end.

merge_store_result(ErrCount,Error,#etsdb_store_res_v1{error_count=AE,errors=AEs}=Acc)->
	Acc#etsdb_store_res_v1{error_count=ErrCount+AE,errors=[Error|AEs]}.
merge_store_result(C,#etsdb_store_res_v1{count=AC}=Acc) when is_integer(C)->
	Acc#etsdb_store_res_v1{count=C+AC};
merge_store_result(#etsdb_store_res_v1{count=C,error_count=E,errors=Es},#etsdb_store_res_v1{count=AC,error_count=AE,errors=AEs}=Acc)->
	Acc#etsdb_store_res_v1{count=C+AC,error_count=E+AE,errors=AEs++Es}.


%%Add 50ms to operation timeout
client_wait_timeout(Timeout)->
	Timeout + 50.