%%%-------------------------------------------------------------------
%%% @author lol4t0
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. Mar 2015 12:08
%%%-------------------------------------------------------------------
-module(etsdb_read_prev).
-author("lol4t0").

%% API
-export([read_prev/3]).

-type bucket() :: module().
-spec read_prev(Bucket :: bucket(), Key :: any(), Timeout :: non_neg_integer()) ->
    {ok, OrigKey :: Key, ActualKey :: binary(), Value :: binary()} | {'not_found', OrigKey :: Key} | {error, Reason :: term()}.
read_prev(Bucket, Key, Timeout) ->
    D = {Key, <<>>},
    {BinKey, _} = Bucket:serialize(D),
    Index = partition_for_data(Bucket, D),
    ReqRef = make_ref(),
    Me = self(),
    Command = {read_prev, [Index, BinKey]},
    etsdb_execute_command_fsm:start_link({raw,ReqRef,Me}, Command, Timeout),
    wait_for_results(ReqRef,client_wait_timeout(Timeout)).

partition_for_data(Bucket, D) ->
    [{Partition, D}] = Bucket:make_partitions(D),
    {ok,Ring} = riak_core_ring_manager:get_my_ring(),
    Idx = crypto:hash(sha,Partition),
    VnodeIdx=riak_core_ring:responsible_index(Idx,Ring),
    etsdb_util:hash_for_partition(VnodeIdx).

wait_for_results(ReqRef,Timeout)->
    receive
        {ReqRef,Res}->
            Res;
        {_OldReqRef,_OldRes}->
            wait_for_results(ReqRef,Timeout)
    after Timeout->
        {error,timeout}
    end.

%%Add 50ms to operation timeout
client_wait_timeout(Timeout)->
    Timeout + 50.
    
    
    
    