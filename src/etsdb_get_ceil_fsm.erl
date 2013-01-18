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
-module(etsdb_get_ceil_fsm).

-behaviour(riak_core_coverage_fsm).

-include_lib("etsdb_request.hrl").

-export([init/2,
         process_results/2,
         finish/2]).


-type from() :: {atom(), req_id(), pid()}.
-type req_id() :: non_neg_integer().

-record(state, {from :: from()}).

init(From, [Bucket,Value,Filter,Timeout]) ->
    NVal = Bucket:w_val(),
    Req = ?ETSDB_GET_CELL_REQ{bucket=Bucket,value=Value,filter=Filter},
    {Req, all, NVal, 1,etsdb,etsdb_vnode_master, Timeout,
     #state{from=From}}.
process_results({error, Reason}, _State) ->
    {error, Reason};
process_results(Results,
                StateData=#state{from={raw, ReqId, ClientPid}}) ->
	ClientPid ! {ReqId, {results, Results}},
    {ok, StateData};
process_results(done, StateData) ->
    {done, StateData}.

finish({error, Error},
       StateData=#state{from={raw, ReqId, ClientPid}}) ->
    ClientPid ! {ReqId, {error, Error}},
    {stop, normal, StateData};
finish(clean,StateData=#state{from={raw, ReqId, ClientPid}}) ->
    ClientPid ! {ReqId, done},
    {stop, normal, StateData}.



