% -------------------------------------------------------------------
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
-module(etsdb_client_worker).

-behaviour(gen_fsm).

-export([handle_event/3,handle_info/3,handle_sync_event/4,invoke/4]).
-export([start_link/1, stop/0, init/1, terminate/3, code_change/4]).

-export([wait_task/3]).

-record(state, {caller,request,timeout_timer}).


start_link(Args) -> gen_fsm:start_link(?MODULE, Args, []).
stop() -> gen_fsm:send_all_state_event(?MODULE, stop).

init(_Args) ->
    {ok,wait_task,#state{}}.



invoke(M,F,A,Timeout)->
	Worker = poolboy:checkout(?MODULE),
	Result = gen_fsm:sync_send_event(Worker,{M,F,A,Timeout},infinity),
	poolboy:checkin(?MODULE,Worker),
	Result.

wait_task({M,F,A,Timeout},From,State)->
	Req = spawn_monitor(fun()->
									Result = erlang:apply(M,F,A),
									exit(Result)
							end),
	{ok,TimerRef}=timer:send_after(Timeout,{expired,Req}),
	{next_state,wait_result,State#state{caller=From,request=Req,timeout_timer=TimerRef}}.


handle_info({'DOWN', MonitorRef, _Type, Object, Info},wait_result,#state{caller=Caller,request={Object,MonitorRef},timeout_timer=Timer}) ->
	gen_fsm:reply(Caller,Info),
	timer:cancel(Timer),
	{next_state,wait_task, #state{}};

handle_info({expired,{TaskPid,TaskRef}=Req},StateName,#state{caller=Caller,request=CurrentReq}=State) ->
	erlang:demonitor(TaskRef,[flush]),
	erlang:exit(TaskPid,task_expired),
	case CurrentReq of
		Req->
			gen_fsm:reply(Caller,{error,timeout}),
    		{next_state,wait_task, #state{}};
		_->
			{next_state,StateName,State}
	end;


handle_info(_Info, StateName, StateData) ->
    {next_state, StateName, StateData}.	


terminate(Reason,_StateName,_State)->
	lager:debug("terminating client worker ~p",[Reason]),
	ok.


code_change(_OldVsn,_StateName, State, _Extra) ->
    {ok, State}.

handle_event(_Event, StateName, StateData) ->
    {next_state, StateName, StateData}.

handle_sync_event(_Event,_From, StateName, StateData) ->
    Reply = ok,
    {reply, Reply, StateName, StateData}.

