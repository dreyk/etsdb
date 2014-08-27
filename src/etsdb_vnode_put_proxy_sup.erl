%%%-------------------------------------------------------------------
%%% @author dreyk
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. Aug 2014 19:54
%%%-------------------------------------------------------------------
-module(etsdb_vnode_put_proxy_sup).
-author("dreyk").
-behaviour(supervisor).

-export([start_link/0, init/1]).

-export([start_proxy/3]).

start_proxy(Bucket,BufferSize,Timeout) ->
    supervisor:start_child(?MODULE, [Bucket,BufferSize,Timeout]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok,
        {{simple_one_for_one, 10, 10},
            [{undefined,
                {etsdb_vnode_put_proxy_sup_sup, start_link, []},
                permanent, 5000, supervisor, [etsdb_vnode_put_proxy_sup_sup]}]}}.