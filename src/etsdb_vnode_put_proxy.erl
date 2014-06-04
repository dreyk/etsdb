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
-module(etsdb_vnode_put_proxy).
-author("gunin").

-behaviour(gen_server).

%% API
-export([start_link/4,reg_name/3,put/4]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {partition,data=[],count=0,callers=[],max_count=1000,bucket,timeout=Timeout}).


put(ProxyName,Bucket,Data,Timeout)->
    Ref = erlang:make_ref(),
    From = {Ref,self()},
    BucketPartiotion = Bucket:make_partitions(Data),
    {ok,Ring} = riak_core_ring_manager:get_my_ring(),
    Partitioned = lists:foldl(fun({Key,Data},Acc)->
        Idx = crypto:hash(sha,Key),
        Partition=riak_core_ring:responsible_index(Idx,Ring),
        [{Partition,Data}|Acc] end,[],BucketPartiotion),
    SortByPartition = lists:keysort(1,Partitioned),
    DataToWrite = etsdb_util:reduce_orddict(fun(A1,A2)->merge_partiotion_data(Bucket,A1,A2) end,SortByPartition),
    WaitCount = lists:foldl(fun({Partition,SerializedData},Acc)->
        To = reg_name(ProxyName,Partition,Bucket),
        gen_server:cast(To,{put,From,SerializedData}),
        Acc+1 end,0,DataToWrite),
    wait_result(WaitCount,Timeout,Ref).

wait_result(0,_Timeout,_Ref)->
    ok;
wait_result(WaitCount,Timeout,Ref)->
    receive
        {Ref,ok}->
            wait_result(WaitCount,Timeout,Ref);
        {Ref,{error,Else}}->
            {error,Else};
        {Ref,Else}->
            {error,Else}
    after Timeout->
        {error,timeout}
    end,
    wait_result(WaitCount-1,Timeout,Ref).

merge_partiotion_data(_Bucket,Data,'$start')->
    [Data];
merge_partiotion_data(Bucket,'$end',Acc)->
    Bucket:serialize(Acc);
merge_partiotion_data(_Bucket,Data,Acc)->
    [Data|Acc].

reg_name(Name,Partition,Bucket)->
    FullName=Name++"_etsb_vproxy_"++atom_to_list(Bucket)++"_"++integer_to_list(Partition),
    list_to_atom(FullName).

start_link(Name,Partition,Bucket,Timeout) ->
    RegName = reg_name(Name,Partition,Bucket),
    gen_server:start_link({local, RegName}, ?MODULE, [Partition,Bucket,Timeout], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Partition,Bucket,Timeout]) ->
    {ok, #state{partition = Partition,bucket=Bucket,timeout = Timeout}}.

handle_call({put,Data},From, State) ->
    Caller = {sycn,From},
    NewState = add_data(Caller,Data,State),
    {noreply,NewState,timeout(NewState)}.

handle_cast({put,From,Data}, State) ->
    NewState = add_data(From,Data,State),
    {noreply,NewState,timeout(NewState)};
handle_cast({put,Data}, State) ->
    Caller = undefined,
    NewState = add_data(Caller,Data,State),
    {noreply,NewState,timeout(NewState)}.

handle_info(timeout, #state{data=Buffer,callers=Callers,count = Count}=State) when Count>0->
    start_process(State#state.partition,State#state.bucket,Callers,Buffer,State#state.timeout),
    NewState = State#state{data = [],count = 0,callers = []},
    {noreply, NewState};
handle_info(_Info, State) ->
    {noreply, State,timeout(State)}.


terminate(normal, _State) ->
    ok;
terminate(Reason, #state{partition = Vnode}) ->
    %%TODO maybe send reply
    lager:error("etsdb_vnode_put_proxy[~p] failed ~p",[Vnode,Reason]),
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State,timeout(State)}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

add_data(Caller,Data,#state{data=Buffer,count=Count,callers=Callers,max_count=Max}=State)->
    {NewBuffer,NewCount} = lists:foldl(fun(Packet,{BufferAcc,CountAcc})->
        {[Packet|BufferAcc],CountAcc+1} end,{Buffer,Count},Data),
    NewCallers = [Caller|Callers],
    if
        NewCount < Max->
            State#state{data = NewBuffer,count = NewCount,callers = NewCallers};
        true->
            start_process(State#state.partition,State#state.bucket,NewCallers,NewBuffer,State#state.timeout),
            State#state{data = [],count = 0,callers = []}
    end.

timeout(#state{count=0})->
    infinity;
timeout(_)->
    1.

start_process(Partition,Bucket,Callers,Buffer,Timeout)->
    ResultHandler = fun(Result)->
        lists:foreach(fun(Caller)->
            reply_to_caller(Caller,Result),
        end,Callers) end,
    case etsdb_put_fsm:start_link(ResultHandler,Partition,Bucket,Buffer,Timeout) of
        {ok,Pid} when is_pid(Pid)->
            ok;
        Else->
            lager:error("Can't start put fsm for ~p:~p ~p",[Partition,Bucket,Else]),
            ResultHandler({error,put_failed})
    end.

reply_to_caller({sync,From},Msg)->
    gen_server:reply(From,Msg);
reply_to_caller({Ref,From},Msg)->
    From ! {Ref,Msg}.