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

-export([put/2,put/3]).


put(Bucket,Data)->
	put(Bucket,Data,?DEFAULT_TIMEOUT).

put(Bucket,Data,Timeout)->
	PartitionedData = prepare_data(Bucket,Data),
	do_put(Bucket,PartitionedData,Timeout,[]).

do_put(_Bucket,[],_Timeout,Results)->
	case Results of
		[]->
			ok;
		_->
			{errors,Results}
	end;
do_put(Bucket,[{Partition,Data}|T],Timeout,Results)->
	ReqRef = make_ref(),
	Me = self(),
	etsdb_put_fsm:start_link({raw,ReqRef,Me},Partition, Bucket, Data, Timeout),
	ResultsNew = case wait_for_results(ReqRef,Timeout) of
		ok->
			Results;
		Else->
			[{Else,Data}|Results]
	end,
	do_put(Bucket,T, Timeout,ResultsNew).

prepare_data(Bucket,Data)->
	Partitioned = Bucket:make_partitions(Data),
	SortByPartition = lists:keysort(1,Partitioned),
	etsdb_util:reduce_orddict(fun merge_user_data/2,SortByPartition).
	

merge_user_data(Data1,[Data2])->
	[Data2|Data1].

wait_for_results(ReqRef,Timeout)->
	receive 
		{ReqRef,Res}->
			Res
	after Timeout->
			{error,timeot}
	end.