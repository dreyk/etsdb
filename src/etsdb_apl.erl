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
-export([get_apl/2, get_apl/4, responsible_preflists/1]).

-spec get_apl(binary(), riak_core_apl:n_val()) -> riak_core_apl:preflist().
get_apl(Partition, N)->
    riak_core_apl:get_apl(Partition,N,etsdb).

-spec get_apl(binary(), riak_core_apl:n_val(), riak_core_apl:ring(), [node()]) -> riak_core_apl:preflist().
get_apl(Partition, N,Ring,UpNodes)->
    riak_core_apl:get_apl(Partition,N,Ring,UpNodes).

-spec responsible_preflists(riak_core_apl:index()) -> [riak_core_apl:index_n()].
responsible_preflists(Index) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    responsible_preflists(Index, Ring).

-spec responsible_preflists(riak_core_apl:index(), riak_core_apl:riak_core_ring()) -> [riak_core_apl:index_n()].
responsible_preflists(Index, Ring) ->
    AllN = determine_all_n(Ring),
    responsible_preflists(Index, AllN, Ring).

-spec responsible_preflists(riak_core_apl:index(), [pos_integer(),...], riak_core_apl:riak_core_ring())
                           -> [riak_core_apl:index_n()].
responsible_preflists(Index, AllN, Ring) ->
    IndexBin = <<Index:160/integer>>,
    PL = riak_core_ring:preflist(IndexBin, Ring),
    Indices = [Idx || {Idx, _} <- PL],
    RevIndices = lists:reverse(Indices),
    lists:flatmap(fun(N) ->
        responsible_preflists_n(RevIndices, N)
                  end, AllN).

-spec responsible_preflists_n([riak_core_apl:index()], pos_integer()) -> [riack_core_apl:index_n()].
responsible_preflists_n(RevIndices, N) ->
    {Pred, _} = lists:split(N, RevIndices),
    [{Idx, N} || Idx <- lists:reverse(Pred)].

-spec determine_all_n(riak_core_apl:riak_core_ring()) -> [pos_integer(),...].
determine_all_n(Ring) ->
    Buckets = riak_core_ring:get_buckets(Ring),
    BucketProps = [riak_core_bucket:get_bucket(Bucket, Ring) || Bucket <- Buckets],
    Default = app_helper:get_env(riak_core, default_bucket_props),
    DefaultN = proplists:get_value(n_val, Default),
    AllN = lists:foldl(fun(Props, AllN) ->
        N = proplists:get_value(n_val, Props),
        ordsets:add_element(N, AllN)
                       end, [DefaultN], BucketProps),
    AllN.
