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

-export([scan/3]).

scan(Bucket,From,To)->
	Partitions = Bucket:scan_partiotions(From,To),
	{ok, Ring} = riak_core_ring_manager:get_my_ring(),
	UpNodes = riak_core_node_watcher:nodes(etsdb),
	RVal = Bucket:r_val(),
	%%[etsdb_apl:get_apl(Partitions,RVal,Ring,UpNodes)].
	ok.
