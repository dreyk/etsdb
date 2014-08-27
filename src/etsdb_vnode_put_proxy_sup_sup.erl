%%%-------------------------------------------------------------------
%%% @author dreyk
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. Aug 2014 20:00
%%%-------------------------------------------------------------------
-module(etsdb_vnode_put_proxy_sup_sup).
-author("dreyk").

%% API
-export([]).
-author("dreyk").
-behaviour(supervisor).

-export([start_link/3, init/1]).

start_link(Bucket,BufferSize,Timeout) ->
    supervisor:start_link(?MODULE, [Bucket,BufferSize,Timeout]).

init([Bucket,BufferSize,Timeout]) ->
    {ok, Ring} = get_my_ring(),
    ProxyWorkers = [{etsdb_vnode_put_proxy:reg_name(I, Bucket),
        {etsdb_vnode_put_proxy, start_link, [I, Bucket, BufferSize, Timeout]},
        permanent, 5000, worker, [etsdb_vnode_put_proxy]} || {I, _} <- riak_core_ring:all_owners(Ring)],
    {ok,
        {{one_for_one, 5, 10},
            ProxyWorkers}}.