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
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {vnode,data=[],count=0,callers=[],max_count=1000}).

start_link(Idx) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Idx], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Idx]) ->
    {ok, #state{vnode = Idx}}.

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
    start_process(Callers,Buffer),
    NewState = State#state{data = [],count = 0,callers = []},
    {noreply, NewState};
handle_info(_Info, State) ->
    {noreply, State,timeout(State)}.


terminate(normal, _State) ->
    ok;
terminate(Reason, #state{vnode=Vnode}) ->
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
            start_process(NewCallers,NewBuffer),
            State#state{data = [],count = 0,callers = []}
    end.

timeout(#state{count=0})->
    infinity;
timeout(_)->
    1.

start_process(Callers,_Buffer)->
    lists:foreach(fun(Caller)->
        case Caller of
            {sync,From}->
                gen_server:reply(From,ok);
            {Ref,From}->
                From ! {Ref,ok};
            _->
                ok
        end end,Callers).