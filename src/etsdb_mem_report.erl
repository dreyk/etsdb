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
-module(etsdb_mem_report).

-export([build/0,file/1,build/2]).

build(Node,Name) when is_atom(Node)->
    FName = Name++"_"++atom_to_list(Node)++".csv",
    Body = rpc:call(Node,?MODULE,build,[]),
    file:write_file(FName,iolist_to_binary(Body));
build([Node|T],Name) when is_atom(Node)->
    FName = Name++"_"++atom_to_list(Node)++".csv",
    Body = rpc:call(Node,?MODULE,build,[]),
    file:write_file(FName,iolist_to_binary(Body)),
    build(T,Name);
build(_,_)->
    ok.
file(Name)->
    file:write_file(Name,iolist_to_binary(build())).
build()->
    Ps = erlang:processes(),
    build_info(Ps,[]).

build_info([],Acc)->
    Acc1 = ["pid;registered_name;memory;total_heap_size;heap_size;stack_size;reductions;message_queue_len;initial_call;current_function;current_stacktrace;garbage_collection;group_leader"|Acc],
    string:join(Acc1,"\r");
build_info([P|T],Acc)->
    case is_process_alive(P) of
        true->
            build_info(T,[build_info(P)|Acc]);
        _->
            build_info(T,Acc)
    end.


build_info(P)->
    case catch erlang:process_info(P,[registered_name,memory,total_heap_size,heap_size,stack_size,reductions,message_queue_len,initial_call,current_function,current_stacktrace,garbage_collection,group_leader]) of
             Info when is_list(Info)-> 
                build_report_row([{pid,P}|Info]);
            _->
                build_report_row([{pid,P}])
    end.
build_report_row(Info)->
    Row = build_report_row(Info,[]),
    Row1 = orddict:from_list(Row),
    Row2 = orddict:merge(fun(_,V1,_)->V1 end,Row1,[{I,"-"}||I<-lists:seq(0,11)]),
    Row3 = lists:map(fun({_,V})->V end,Row2),
    string:join(Row3,";").
    
build_report_row([],Acc)->
    Acc;
build_report_row([{N,V}|T],Acc)->
    case format_cell(N,V) of
        {_,_}=Else->
            build_report_row(T,[Else|Acc]);
        _->
            build_report_row(T,Acc)
    end.


format_cell(pid,G)->
    {0,cast_2_string(G)};
format_cell(registered_name,V)->
    {1,cast_2_string(V)};
format_cell(memory,V) when is_integer(V)->
    {2,integer_to_list(V)};
format_cell(total_heap_size,V)when is_integer(V)->
    {3,integer_to_list(V)};
format_cell(heap_size,V) when is_integer(V)->
    {4,integer_to_list(V)};
format_cell(stack_size,V) when is_integer(V)->
    {5,integer_to_list(V)};
format_cell(reductions,V) when is_integer(V)->
    {6,integer_to_list(V)};
format_cell(message_queue_len,V) when is_integer(V)->
    {7,integer_to_list(V)};
format_cell(initial_call,{Module, Function, Arity})->
    {8,atom_to_list(Module)++":"++atom_to_list(Function)++"/"++integer_to_list(Arity)};
format_cell(current_function,{Module, Function, Arity})->
    {9,atom_to_list(Module)++":"++atom_to_list(Function)++"/"++integer_to_list(Arity)};
format_cell(current_stacktrace,Stack)->
    {10,format_stack(Stack,[])};
format_cell(garbage_collection,Props) when is_list(Props)->
    After = proplists:get_value(fullsweep_after, Props,-1),
    {11,integer_to_list(After)};
format_cell(group_leader,G)->
    {12,cast_2_string(G)};

format_cell(_,_)->
    undefined.

format_stack([],Acc)->
    Acc1 = lists:reverse(Acc),
    string:join(Acc1,"->");
format_stack([{Module, Function, Arity, _Location}|T],Acc)->
    Arity1 = if
                is_integer(Arity)->
                    integer_to_list(Arity);
                is_list(Arity)->
                    integer_to_list(length(Arity));
                true->
                    "-1"
    end,
    format_stack(T,[[atom_to_list(Module)++":"++atom_to_list(Function)++"/"++Arity1]|Acc]).
    
    

cast_2_string(V) when is_binary(V)->binary_to_list(V);
cast_2_string(V) when is_list(V)->V;
cast_2_string(V) when is_integer(V)->cast_2_string(integer_to_list(V));
cast_2_string(V) when is_float(V)->cast_2_string(float_to_list(V));
cast_2_string(V) when is_atom(V)->cast_2_string(atom_to_binary(V,utf8));
cast_2_string(V) -> io_lib:format("~p",[V]).