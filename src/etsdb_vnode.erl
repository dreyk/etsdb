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

%% @doc etsdb riak_core_vnode implementation
-module(etsdb_vnode).
-author('Alex G. <gunin@mail.mipt.ru>').

-export([start_vnode/1,
         init/1,
         handle_command/3,
         handle_handoff_command/3,
         handle_handoff_data/2,
         handoff_cancelled/1,
         handle_info/2,
         handoff_finished/2,
         handoff_starting/2,
         encode_handoff_item/2,
         terminate/2,
         delete/1,
         handle_coverage/4,
         is_empty/1,
         handle_exit/3,
         put_internal/3,
         put_external/4,
         put_external/5,
         get_query/4,
         scan/3,
        register_bucket/1,
        dump_to/5]).

-behaviour(riak_core_vnode).

-include("etsdb_request.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

%%vnode state
-record(state,{delete_mod,vnode_index,backend,backend_ref}).

%%Start vnode
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, etsdb_vnode).

register_bucket(Bucket)->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    AllOwners = riak_core_ring:all_owners(Ring),
    [{riak_core_vnode_master:sync_command(IN,{register,Bucket},etsdb_vnode_master),IN}||IN<-AllOwners].


put_internal(ReqID,Preflist,Data)->
    riak_core_vnode_master:command(Preflist,#etsdb_innerstore_req_v1{value=Data,req_id=ReqID},{fsm,undefined,self()},etsdb_vnode_master).

put_external(Caller,ReqID,Preflist,Bucket,Data)->
    riak_core_vnode_master:command(Preflist,#etsdb_store_req_v1{value=Data,req_id=ReqID,bucket=Bucket},{fsm,undefined,Caller},etsdb_vnode_master).
put_external(ReqID,Preflist,Bucket,Data)->
    riak_core_vnode_master:command(Preflist,#etsdb_store_req_v1{value=Data,req_id=ReqID,bucket=Bucket},{fsm,undefined,self()},etsdb_vnode_master).

dump_to(ReqID,Preflist,Bucket,File,Param)->
    riak_core_vnode_master:command(Preflist,#etsdb_dump_req_v1{bucket = Bucket,file = File,param = Param,req_id = ReqID},{fsm,undefined,self()},etsdb_vnode_master).

scan(ReqID,Vnode,Scans)->
    riak_core_vnode_master:command([{Vnode,node()}],#etsdb_get_query_req_v1{get_query=Scans,req_id=ReqID,bucket=custom_scan},{fsm,undefined,self()},etsdb_vnode_master).
get_query(ReqID,Preflist,Bucket,Query)->
    riak_core_vnode_master:command(Preflist,#etsdb_get_query_req_v1{get_query=Query,req_id=ReqID,bucket=Bucket},{fsm,undefined,self()},etsdb_vnode_master).

%%Init Callback.
init([Index]) ->
    DeleteMode = app_helper:get_env(etsdb, delete_mode, 3000),
    {BackEndModule,BackEndProps0} = case app_helper:get_env(etsdb, backend,{etsdb_leveldb_backend,[{data_root, ["./data/leveldb"]}]}) of
            [List]->
                List;
            Tuple->
                Tuple
            end,
    Path = etsdb_leveldb_affinity:get_path(Index, BackEndProps0),
    BackEndProps = lists:keyreplace(data_root, 1, BackEndProps0, {data_root, Path}),
    %%Start storage backend
    case BackEndModule:init(Index,BackEndProps) of
        {ok,Ref}->
            RBuckets = app_helper:get_env(etsdb,registered_bucket),
            start_clear_buckets(RBuckets),
            {ok,#state{vnode_index=Index,delete_mod=DeleteMode,backend=BackEndModule,backend_ref=Ref},[{pool,etsdb_vnode_worker, 10, []}]};
        {error,Else}->
            {error,Else}
    end.

start_clear_buckets([B|Tail])->
    lager:debug("start timer for ~p",[B]),
    riak_core_vnode:send_command_after(clear_period(B),{clear_db,B}),
    start_clear_buckets(Tail);
start_clear_buckets([])->
    ok;
start_clear_buckets(_)->
    ok.
handle_handoff_command(Req=?ETSDB_STORE_REQ{}, Sender, State) ->
    {noreply, NewState} = handle_command(Req, Sender, State),
    {forward, NewState};
handle_handoff_command({remove_expired,_,_}, _Sender, State) ->
    {noreply,State};
handle_handoff_command({clear_db,_}, _Sender, State) ->
    {noreply,State};
handle_handoff_command(Req, Sender, State) ->
    handle_command(Req, Sender, State).

handle_command({remove_dumped,Bucket,Records}, _Sender,
    #state{backend=BackEndModule,backend_ref=BackEndRef,vnode_index=Index}=State)->
    ToDelete = lists:usort(Records),
    case BackEndModule:delete(Bucket,ToDelete,BackEndRef) of
        {ok,NewBackEndRef}->
            ok;
        {error,Reason,NewBackEndRef}->
            lager:error("Can't delete dumped records ~p on ~p",[Reason,Index])
    end,
    {noreply,State#state{backend_ref=NewBackEndRef}};

handle_command({remove_expired,_,_}, _Sender,
               #state{vnode_index=undefined}=State)->
    {noreply,State};
handle_command({remove_expired,Bucket,{expired_records,{0,_Records}}}, _Sender,
               State)->
    riak_core_vnode:send_command_after(clear_period(Bucket),{clear_db,Bucket}),
    {noreply,State};
handle_command({remove_expired,Bucket,{expired_records,{Count,Records}}}, _Sender,
               #state{backend=BackEndModule,backend_ref=BackEndRef,vnode_index=Index}=State)->
    ToDelete = lists:usort(Records),
    case BackEndModule:delete(Bucket,ToDelete,BackEndRef) of
        {ok,NewBackEndRef}->
            ok;
        {error,Reason,NewBackEndRef}->
            lager:error("Can't delete old records ~p on ~p",[Reason,Index])
    end,
    case Count of
        {continue,_}->
            riak_core_vnode:send_command_after(1000,{clear_db,Bucket});
        _->
            riak_core_vnode:send_command_after(clear_period(Bucket),{clear_db,Bucket})
    end,
    {noreply,State#state{backend_ref=NewBackEndRef}};

handle_command({remove_expired,Bucket,Error}, _Sender,#state{vnode_index=Index}=State)->
    lager:error("Find expired task failed ~p on ~p",[Error,Index]),
    riak_core_vnode:send_command_after(clear_period(Bucket),{clear_db,Bucket}),
    {noreply, State};

handle_command({clear_db,_}, _Sender,
               #state{vnode_index=undefined}=State)->
    {noreply, State};
handle_command({clear_db,Bucket}, Sender,
               #state{backend=BackEndModule,backend_ref=BackEndRef,vnode_index=Index}=State)->
    Me = self(),
    case BackEndModule:find_expired(Bucket,BackEndRef) of
        {async, AsyncWork} ->
            Fun = fun()->
                          riak_core_vnode:send_command(Me,{remove_expired,Bucket,AsyncWork()}) end,
            {async,{clear_db,Fun},Sender, State};
        Else->
            lager:error("Can't create clear db task ~p on ~p",[Else,Index]),
            {noreply, State}
    end;

handle_command({register,Bucket},Sender,State)->
    lager:info("register ~p",[Bucket]),
    riak_core_vnode:send_command_after(clear_period(Bucket),{clear_db,Bucket}),
    riak_core_vnode:reply(Sender,started),
    {noreply,State};
%%Receive command to store data in user format.
handle_command(?ETSDB_DUMP_REQ{bucket=Bucket,param = Param,file = File,req_id=ReqID}, Sender,
    #state{backend=BackEndModule,backend_ref=BackEndRef,vnode_index=Index}=State)->
    make_dir(File),
    LFile = filename:join([File,integer_to_list(Index)++".dmp"]),
    lager:info("prepare dump ~p",[{Bucket,Param,LFile}]),
    case catch BackEndModule:dump_to(self(), Bucket, Param, LFile,BackEndRef) of
        {async, AsyncWork} ->
            Fun =
                fun()->
                    InvokeRes = AsyncWork(),
                    {r,{Index,node()},ReqID,InvokeRes} end,
            {async, {invoke,Fun},Sender, State};
        Else->
            lager:info("err ~p",[Else]),
            riak_core_vnode:reply(Sender, {r,{Index,node()},ReqID,Else}),
            {noreply,State}
    end;
handle_command(?ETSDB_STORE_REQ{bucket=Bucket,value=Value,req_id=ReqID}, Sender,
               #state{backend=BackEndModule,backend_ref=BackEndRef,vnode_index=Index}=State)->
    case BackEndModule:save(Bucket,Value,BackEndRef) of
        {Result,NewBackEndRef}->

            riak_core_vnode:reply(Sender, {w,Index,ReqID,Result});
        {error,Reason,NewBackEndRef}->
            riak_core_vnode:reply(Sender, {w,Index,ReqID,{error,Reason}})
    end,
    {noreply,State#state{backend_ref=NewBackEndRef}};

handle_command(?ETSDB_GET_QUERY_REQ{get_query=Scans,req_id=ReqID,bucket=custom_scan}, Sender,
               #state{backend=BackEndModule,backend_ref=BackEndRef,vnode_index=Index}=State)->
     case do_scan(BackEndModule,BackEndRef,Scans) of
        {async, AsyncWork} ->
            Fun =
                fun()->
                        InvokeRes = AsyncWork(),
                        {r,Index,ReqID,InvokeRes} end,
            {async, {scan,Fun},Sender, State};
        Result->
            riak_core_vnode:reply(Sender, {r,Index,ReqID,Result}),
            {noreply, State}
    end;
handle_command(?ETSDB_GET_QUERY_REQ{bucket=Bucket,get_query=Query,req_id=ReqID}, Sender,
               #state{backend=BackEndModule,backend_ref=BackEndRef,vnode_index=Index}=State)->
     case do_get_qyery(BackEndModule,BackEndRef,Bucket,Query) of
        {async, AsyncWork} ->
            Fun =
                fun()->
                        InvokeRes = AsyncWork(),
                        {r,Index,ReqID,InvokeRes} end,
            {async, {invoke,Fun},Sender, State};
        Result->
            riak_core_vnode:reply(Sender, {r,Index,ReqID,Result}),
            {noreply, State}
    end;

handle_command(?FOLD_REQ{foldfun=FoldFun, acc0=Acc0}, Sender,
               #state{backend=BackEndModule,backend_ref=BackEndRef,vnode_index=VIndex}=State)->
    Ref = node(),
    WrapperFun = fun(K,V,{Count,WAcc,ExtrenalAcc})->
                         if Count rem 100000 == 0 ->
                                lager:info("start send handoff data ~p count Ref ~p.Index ~p",[Count,Ref,VIndex]),
                                ExtrenalAcc1 = FoldFun(<<Count:64/integer>>,{Ref,Count,[{K,V}|WAcc]},ExtrenalAcc),
                                {Count+1,[],ExtrenalAcc1};
                            true->
                                {Count+1,[{K,V}|WAcc],ExtrenalAcc}
                         end end,
    case BackEndModule:fold_objects(WrapperFun,{1,[],Acc0},BackEndRef) of
        {async, AsyncWork} ->
            Fun =
                fun()->
                        {AllCount,Avalable,ExternalAcc}=AsyncWork(),
                        case Avalable of
                            []->
                                ExternalAcc;
                            _->
                                Length = length(Avalable),
                                lager:info("start send handoff data ~p count. Ref ~p",[Length,Ref]),
                                FoldFun(<<AllCount:64/integer>>,{Ref,Length,Avalable},ExternalAcc)
                        end
                end,
            {async, {invoke,Fun},Sender, State};
        Else->
            riak_core_vnode:reply(Sender,{error,Else}),
            {noreply, State}
    end.
    

handle_info(timeout,State)->
    lager:debug("receive timeout ~p",[State]),
    {ok,State};

handle_info(Info,State)->
    lager:debug("receive info ~p",[{Info,State}]),
    {ok,State}.



handoff_starting(TargetNode, State) ->
    lager:debug("handof stating ~p",[TargetNode]),
    {true, State}.

handoff_cancelled(State) ->
    lager:debug("handof canceled ~p",[State]),
    {ok, State}.

handoff_finished(TargetNode, State) ->
    lager:debug("handof finished ~p",[TargetNode]),
    {ok, State}.

handle_handoff_data(BinObj, #state{backend=BackEndModule,backend_ref=BackEndRef,vnode_index=VIndex}=State) ->
    {Ref,Count,Values} = binary_to_term(BinObj),
    Im = node(),
    lager:info("receive handoff data ~p count. ref ~p.Index ~p.I'm ~p",[Count,Ref,VIndex,Im]),
       case BackEndModule:save(BackEndModule,Values,BackEndRef) of
        {Result,NewBackEndRef}->
            {reply,Result, State#state{backend_ref=NewBackEndRef}};
        {error,Reason,NewBackEndRef}->
            {reply, {error, Reason}, State#state{backend_ref=NewBackEndRef}}
    end.

encode_handoff_item(_ObjectName,ObjectValue) ->
    term_to_binary(ObjectValue).

delete(#state{backend=BackEndModule,backend_ref=BackEndRef,vnode_index=Index}=State)->
    case BackEndModule:drop(BackEndRef) of
        {ok,NewBackEndRef} ->
            lager:info("data dropped for ~p",[Index]),
            ok;
        {error, Reason, NewBackEndRef} ->
            lager:error("Failed to drop ~p. Reason: ~p~n", [BackEndModule, Reason]),
            ok
    end,
    {ok, State#state{backend_ref=NewBackEndRef,vnode_index=undefined}}.


handle_coverage(_Request, _KeySpaces, _Sender, ModState)->
   {noreply,ModState}.

is_empty(#state{backend=Mod,backend_ref=Ref}=State)->
    IsEmpty = Mod:is_empty(Ref),
    {IsEmpty,State}.

handle_exit(_Pid,_Reason,State)->
    {noreply,State}.
%%------------------------------------------
%% Terminate vnode process.
%% Try terminate all child(user process) like supervisor
%%------------------------------------------
terminate(Reason,#state{backend=BackEndModule,backend_ref=BackEndRef}=State)->
    lager:info("etsdb vnode terminated in state ~p reason ~p",[State,Reason]),
    BackEndModule:stop(BackEndRef),
    ok;
terminate(Reason,State)->
    lager:info("etsdb vnode terminated in state ~p reason ~p",[State,Reason]),
    ok.

%% ===================================================================
%% Internal functions
%% ===================================================================

do_get_qyery(BackEndModule,BackEndRef,Bucket,{scan,From,To})->
    BackEndModule:scan(Bucket,From,To,[],BackEndRef);
do_get_qyery(BackEndModule,BackEndRef,Bucket,{scan,From,To,Acc})->
    BackEndModule:scan(Bucket,From,To,Acc,BackEndRef);
do_get_qyery(_BackEndModule,BackEndRef,_Bucket,_Query)->
    {{error,bad_query},BackEndRef}.

do_scan(BackEndModule,BackEndRef,Scans)->
    BackEndModule:scan(Scans,[],BackEndRef).

clear_period(Bucket)->
    I = Bucket:clear_period(),
    I+etsdb_util:random_int(I).

make_dir(undefined)->
    exit({error,dir_undefined});
make_dir("undefined"++_)->
    exit({error,dir_undefined});
make_dir(Dir) ->
    case make_safe(Dir) of
        ok ->
            ok;
        {error, enoent} ->
            S1 = filename:split(Dir),
            S2 = lists:droplast(S1),
            case make_dir(filename:join(S2)) of
                ok ->
                    make_safe(Dir);
                Else ->
                    Else
            end;
        Else ->
            Else
    end.
make_safe(Dir)->
    case file:make_dir(Dir) of
        ok->
            ok;
        {error,eexist}->
            ok;
        Else->
            Else
    end.