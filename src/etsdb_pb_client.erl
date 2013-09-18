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
-module(etsdb_pb_client).

-include_lib("etsdb_tkb_pb.hrl").

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-import(riak_pb_kv_codec, [decode_quorum/1]).

-record(state, {}). % emulate legacy API when vnode_vclocks is true

-define(DEFAULT_TIMEOUT, 60000).

init() ->
    #state{}.

%% @doc decode/2 callback. Decodes an incoming message.
decode(_Code, Bin) ->
    {ok, etsdb_tkb_pb:decode_tkvbatch(Bin)}.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, etsdb_tkb_pb:encode_tkvresp(#tkvresp{status=Message})}.


process(#tkvbatch{records=Records}, #state{} = State) ->
    Records1 = lists:sort([{{ID,Time},Value}||#tkvrecord{id=ID,time=Time,value=Value}<-Records]),
    case etsdb_put:put(etsdb_tkb,Records1,?DEFAULT_TIMEOUT) of
        ok->
            {reply,ok, State};
        {errors, Reason} ->
            io:format("error ~p~n",[Reason]),
            {reply,error, State}
    end.


process_stream(_,_,State) ->
    {ignore, State}.


