%%%-------------------------------------------------------------------
%%% @author lol4t0
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Jan 2015 11:34
%%%-------------------------------------------------------------------
-module(etsdb_leveldb_affinity).
-author("lol4t0").
-behaviour(gen_server).

%% API
-export([start_link/0, get_path/1]).


%% callback
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).


%% --------------------- API -----------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_path(Partition) ->
    {ok, Path} = gen_server:call(?MODULE, {get_path, Partition}),
    Path.

%% --------------------- CALLBACK ------------------------------------

-record(state, {affinity, data_roots, current_roots, affinity_path}).

init([]) ->
    {_BackEndModule,BackEndProps} = app_helper:get_env(etsdb, backend,{etsdb_leveldb_backend,[]}),
    DataRoots0 = etsdb_util:propfind(data_roots, BackEndProps, ["./data/leveldb"]),
    DataRoots = if
                    is_list(DataRoots0) ->
                        DataRoots0;
                    true ->
                        [DataRoots0]
                end,
    AffinityPath = etsdb_util:propfind(affinity_path, BackEndProps, "./data/leveldb/affinity.term"),
    AffinityList = read_affinity(AffinityPath),
    {_Index, LastAffinityPath} = lists:last(AffinityList),
    CurrentRoots = lists:dropwhile(fun(P) -> P /= LastAffinityPath end, DataRoots),
    {
        ok,
        #state{
            affinity = AffinityList,
            data_roots = DataRoots,
            current_roots = next_roots(CurrentRoots, DataRoots),
            affinity_path = AffinityPath
        }
    }.

read_affinity(Path) ->
    {ok, AffinityList} = file:consult(Path),
    AffinityList.

next_roots([], DataRoots) ->
    DataRoots;
next_roots([_Curr], DataRoots) ->
    DataRoots;
next_roots([_Curr|Rest], _DataRoots) ->
    Rest.


handle_call({get_path, Index}, _From,
    State = #state{affinity = AffinityList, current_roots = CurrentRoots, data_roots = DataRoots, affinity_path = AffinityPath}) ->
    {Path, NextRoots} = case etsdb_util:propfind(Index, AffinityList, 'not_found') of
                            SomePath when is_list(SomePath) ->
                                {SomePath, CurrentRoots};
                            'not_found' ->
                                NewPath = hd(CurrentRoots),
                                NewRoots = next_roots(CurrentRoots, DataRoots),
                                NewAffinity = AffinityList ++ [{Index, NewPath}],
                                ok = save_affinity(NewAffinity, AffinityPath),
                                {NewPath, NewRoots}
                        end,
    {reply, Path, State#state{current_roots = NextRoots}}.

save_affinity(AffinityList, AffinityPath) ->
    file:write_file(AffinityPath, list_to_binary(io_lib:format("~p.", [AffinityList]))).

handle_cast(_Arg0, _Arg1) ->
    error(unexpected_cast).

handle_info(_Arg0, _Arg1) ->
    error(unexpected_info).

terminate(_Reason, _State) ->
    ok.

code_change(_Old, State, _Extra) ->
    {ok, State}.

