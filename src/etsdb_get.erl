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
-module(etsdb_get).

-export([scan/3,scan/4]).

-define(DEFAULT_TIMEOUT,60000).

scan(Bucket,From,To)->
	scan(Bucket,From,To,?DEFAULT_TIMEOUT).
scan(Bucket,From,To,Timeout)->
	Partitions = Bucket:scan_partiotions(From,To),
	scan_partiotions(Bucket,From,To,Partitions,[],Timeout).

scan_partiotions(_Bucket,_From,_To,[],Acc,_Timeout)->
	{ok,Acc};
scan_partiotions(Bucket,From,To,[Partition|T],Acc,Timeout)->
	ReqRef = make_ref(),
	Me = self(),
	PartionIdx = crypto:sha(Partition),
	etsdb_get_fsm:start_link({raw,ReqRef,Me},PartionIdx, Bucket, {scan,From,To},Timeout),
	case wait_for_results(ReqRef,Timeout) of
		{ok,Res} when is_list(Res)->
			lager:debug("scan return ~p for ~p ~p",[Res,From,To]),
			scan_partiotions(Bucket,From,To,T,Res++Acc,Timeout);
		Else->
			etsdb_util:make_error_response(Else)
	end.

wait_for_results(ReqRef,Timeout)->
	receive 
		{ReqRef,Res}->
			Res
	after Timeout->
			{error,timeot}
	end.