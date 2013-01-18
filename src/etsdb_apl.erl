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


-module(etsdb_apl).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([get_apl_ann/2,get_apl_ann/4]).

get_apl_ann(Partition, N)->
	{ok, Ring} = riak_core_ring_manager:get_my_ring(),
	UpNodes = riak_core_node_watcher:nodes(etsdb),
	get_apl_ann(Partition, N,Ring,UpNodes).

get_apl_ann(Partition, N,Ring,UpNodes)->
	AllOwners = riak_core_ring:all_owners(Ring),
	{P1,P2} = lists:split(Partition,AllOwners),
	{Primaries, Fallbacks} = lists:split(N, P2++P1),
    {Up, Pangs} = check_up(Primaries, UpNodes, [], []),
    Up ++ find_fallbacks(Pangs, Fallbacks, UpNodes, []).

check_up([], _UpNodes, Up, Pangs) ->
    {lists:reverse(Up), lists:reverse(Pangs)};
check_up([{Partition,Node}|Rest], UpNodes, Up, Pangs) ->
    case is_up(Node, UpNodes) of
        true ->
            check_up(Rest, UpNodes, [{{Partition, Node}, primary} | Up], Pangs);
        false ->
            check_up(Rest, UpNodes, Up, [{Partition, Node} | Pangs])
    end.

find_fallbacks(_Pangs, [], _UpNodes, Secondaries) ->
    lists:reverse(Secondaries);
find_fallbacks([], _Fallbacks, _UpNodes, Secondaries) ->
    lists:reverse(Secondaries);
find_fallbacks([{Partition, _Node}|Rest]=Pangs, [{_,FN}|Fallbacks], UpNodes, Secondaries) ->
    case is_up(FN, UpNodes) of
        true ->
            find_fallbacks(Rest, Fallbacks, UpNodes,
                           [{{Partition, FN}, fallback} | Secondaries]);
        false ->
            find_fallbacks(Pangs, Fallbacks, UpNodes, Secondaries)
    end.

%% Return true if a node is up
is_up(Node, UpNodes) ->
	lists:member(Node,UpNodes).
