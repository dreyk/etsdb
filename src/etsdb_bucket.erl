%% -------------------------------------------------------------------
%%
%% etsdb_bucket
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

%% @doc etsdb_bucket behaviour
-module(etsdb_bucket).
-author('Alex G. <gunin@mail.mipt.ru>').


-export([behaviour_info/1]).


-spec behaviour_info(atom()) -> 'undefined' | [{atom(), arity()}].
behaviour_info(callbacks) ->
    [
     {api_version,0},
     {w_val,0},
     {r_val,0},
     {w_quorum,0},
     {r_quorum,0},
     {make_partitions,1},
     {partition_by_time,2}
    ];
behaviour_info(_Other) ->
    undefined.