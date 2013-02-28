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
-module(etsdb_socket_server).
-behaviour(gen_server).

-export([start_link/0, set_socket/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(DEFAULT_TIMEOUT, 60000).

-record(state, {
          socket
         }).


start_link() ->
    gen_server:start_link(?MODULE, [], []).

set_socket(Pid, Socket) ->
    gen_server:call(Pid, {set_socket, Socket}, infinity).

init([]) ->
    {ok, #state{}}.

handle_call({set_socket, Socket}, _From, State) ->
    inet:setopts(Socket, [{active, once}, {packet, 4}]),
    {reply, ok, State#state{socket = Socket}}.


handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp_closed, Socket}, State=#state{socket=Socket}) ->
    {stop, normal, State};
handle_info({tcp_error, Socket, _Reason}, State=#state{socket=Socket}) ->
    {stop, normal, State};
handle_info({tcp, _Sock,MsgData}, State=#state{
                                               socket=Socket}) ->
   NewState = process_message(MsgData, State),
   inet:setopts(Socket, [{active, once}]),
   {noreply, NewState};

handle_info(Message, State) ->
    %% Throw out messages we don't care about, but log them
    lager:error("Unrecognized message ~p", [Message]),
    {noreply, State}.

terminate(_Reason, _State) ->
    riak_api_stat:update(pbc_disconnect),
    ok.

code_change(_OldVsn,State,_Extra) ->
    {ok, State}.

%% ===================================================================
%% Internal functions
%% ===================================================================
process_message(<<Type:8/integer,RequestData/binary>>,State)->
	process_message(Type,RequestData,State);
process_message(_,#state{socket=Sock}=State)->
	gen_tcp:send(Sock,<<1:8/integer>>),
	State.
process_message(1,BatchData,#state{socket=Sock}=State)->
	case catch get_batch(BatchData,[]) of
		{ok,ErlData}->
			case etsdb_put:put(etsdb_tkb,ErlData,?DEFAULT_TIMEOUT) of
				ok->
					gen_tcp:send(Sock,<<0:8/integer>>);
				{errors, Reason} ->
					lager:error("error ~p",[Reason]),
					gen_tcp:send(Sock,<<3:8/integer>>)
			end;
		Else->
			lager:error("Bad request from client ~p",[Else]),
			gen_tcp:send(Sock,<<2:8/integer>>)
	end,
	State;
process_message(_Type,_BatchData,#state{socket=Sock}=State)->
	gen_tcp:send(Sock,<<1:8/integer>>),
	State.

get_batch(<<>>,Acc)->
	{ok,lists:reverse(Acc)};
get_batch(<<DataSize:32/unsigned-integer,ID:64/integer,Time:64/integer,Data:DataSize/binary,Tail/binary>>,Acc)->
	get_batch(Tail,[{{ID,Time},Data}|Acc]);
get_batch(_Else,_Acc)->
	{error,bad_format}.