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
-module(etsdb_console).


-export([join/1,
         leave/1,
         remove/1,
         ringready/1,
		 remove_node/1,
		 ring_status/0,reip/1]).

join([NodeStr]) ->
    case do_join(NodeStr) of
        ok ->
            io:format("Sent join request to ~s\n", [NodeStr]),
            ok;
        {error, not_reachable} ->
            io:format("Node ~s is not reachable!\n", [NodeStr]),
            error;
        {error, different_ring_sizes} ->
            io:format("Failed: ~s has a different ring_creation_size~n",
                      [NodeStr]),
            error
    end;
join(_) ->
    io:format("Join requires a node to join with.\n"),
    error.

do_join(NodeStr) when is_list(NodeStr) ->
    do_join(riak_core_util:str_to_node(NodeStr));
do_join(Node) when is_atom(Node) ->
    {ok, OurRingSize} = application:get_env(riak_core, ring_creation_size),
    case net_adm:ping(Node) of
        pong ->
            case rpc:call(Node,
                          application,
                          get_env, 
                          [riak_core, ring_creation_size]) of
                {ok, OurRingSize} ->
                    riak_core_gossip:send_ring(Node, node());
                _ -> 
                    {error, different_ring_sizes}
            end;
        pang ->
            {error, not_reachable}
    end.

leave([]) ->
    remove_node(node()).

remove([Node]) ->
    remove_node(list_to_atom(Node)).

remove_node(Node) when is_atom(Node) ->
    Res = riak_core_gossip:remove_from_cluster(Node),
    io:format("~p\n", [Res]).

ring_status() ->
    case ringready() of
        {ok, Nodes} ->
            io_lib:format("TRUE All nodes agree on the ring ~p", [Nodes]);
        {error, {different_owners, N1, N2}} ->
            io_lib:format("FALSE Node ~p and ~p list different partition owners", [N1, N2]);
        {error, {nodes_down, Down}} ->
            io_lib:format("FALSE ~p down.  All nodes need to be up to check.", [Down])
    end.
%% @doc
%% @spec ringready([]) -> ok | error
ringready([]) ->
    case ringready() of
        {ok, Nodes} ->
            io:format("TRUE All nodes agree on the ring ~p\n", [Nodes]);
        {error, {different_owners, N1, N2}} ->
            io:format("FALSE Node ~p and ~p list different partition owners\n", [N1, N2]),
            error;
        {error, {nodes_down, Down}} ->
            io:format("FALSE ~p down.  All nodes need to be up to check.\n", [Down]),
            error
    end.

%% @spec ringready() -> {ok, [atom()]} | {error, any()}
ringready() ->
    case get_rings() of
        {[], Rings} ->
            {N1,R1}=hd(Rings),
            case rings_match(hash_ring(R1), tl(Rings)) of
                true ->
                    Nodes = [N || {N,_} <- Rings],
                    {ok, Nodes};

                {false, N2} ->
                    {error, {different_owners, N1, N2}}
            end;

        {Down, _Rings} ->
            {error, {nodes_down, Down}}
    end.

%% Retrieve the rings for all other nodes by RPC
get_rings() ->
    {RawRings, Down} = riak_core_util:rpc_every_member(
                         riak_core_ring_manager, get_my_ring, [], 30000),
    Rings = orddict:from_list([{riak_core_ring:owner_node(R), R} || {ok, R} <- RawRings]),
    {lists:sort(Down), Rings}.

%% Produce a hash of the 'chash' portion of the ring
hash_ring(R) ->
    erlang:phash2(riak_core_ring:all_owners(R)).

%% Check if all rings match given a hash and a list of [{N,P}] to check
rings_match(_, []) ->
    true;
rings_match(R1hash, [{N2, R2} | Rest]) ->
    case hash_ring(R2) of
        R1hash ->
            rings_match(R1hash, Rest);
        _ ->
            {false, N2}
    end.



reip([OldNode, NewNode]) ->
    try
        %% reip is called when node is down (so riak_core_ring_manager is not running),
        %% so it has to use the basic ring operations.
        %%
        %% Do *not* convert to use riak_core_ring_manager:ring_trans.
        %%
        application:load(riak_core),
        RingStateDir = app_helper:get_env(riak_core, ring_state_dir),
        {ok, RingFile} = riak_core_ring_manager:find_latest_ringfile(),
        BackupFN = filename:join([RingStateDir, filename:basename(RingFile)++".BAK"]),
        {ok, _} = file:copy(RingFile, BackupFN),
        io:format("Backed up existing ring file to ~p~n", [BackupFN]),
        Ring = riak_core_ring_manager:read_ringfile(RingFile),
        NewRing = riak_core_ring:rename_node(Ring, OldNode, NewNode),
        riak_core_ring_manager:do_write_ringfile(NewRing),
        io:format("New ring file written to ~p~n",
            [element(2, riak_core_ring_manager:find_latest_ringfile())])
    catch
        Exception:Reason ->
            lager:error("Reip failed ~p:~p", [Exception,
                    Reason]),
            io:format("Reip failed, see log for details~n"),
            error
    end.