%%%-------------------------------------------------------------------
%%% @author lol4t0
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Jan 2015 12:02
%%%-------------------------------------------------------------------
-module(etsdb_dbsequence_proxy_backend).
-author("lol4t0").

-behaviour(etsdb_backend).

%% API
-export([init/2, stop/1, drop/1, save/3, scan/3, scan/5, fold_objects/3, find_expired/2, delete/3, is_empty/1]).

-record(backend_info, {start_timestamp, last_timestamp, path, backend_state = undefined, last_accessed = 0}).
-record(state, {
    partition::non_neg_integer(),
    source_backends,
    source_module::module(),
    current_backend::#backend_info{} | undefined,
    config::proplists:proplist()
}).

init(Partition, Config) ->
    SourceBackendsTable = ets:new(undefined, [set, private, {keypos, #backend_info.start_timestamp}]),
    init_sequence(SourceBackendsTable, Config),
    [SourceBackend | RestBackends ] = etsdb_util:propfind(proxy_source, Config, [etsdb_leveldb_backend]),
    NewConfig = lists:keyreplace(proxy_source, 1, Config, {proxy_source, RestBackends}),
    {ok, #state{partition = Partition, source_backends = SourceBackendsTable, source_module = SourceBackend, config = NewConfig}}.


stop(#state{source_module = SrcModule, source_backends = Backends}) ->
    ets:Backends(
        fun
            (#backend_info{backend_state = undefined}, ok) ->
                ok;
            (#backend_info{backend_state = State}, ok) ->
                SrcModule:stop(State)
        end,
        ok, Backends).


drop(SelfState = #state{source_module = SrcModule, source_backends = Backends}) ->
    SrcDropResult = ets:foldl(
        fun
            (#backend_info{backend_state = undefined}, Result) ->
                Result;
            (I = #backend_info{backend_state = OldState}, Result) ->
                {NewResult, NewState} = case SrcModule:drop(OldState) of
                    {ok, State} ->
                        {Result, State};
                    {error, Reason, State} ->
                        {[{error, I, Reason}|Result], State}
                end,
                ets:insert(Backends, I#backend_info{backend_state = NewState}),
                NewResult
        end,
        [], Backends),
    if
        SrcDropResult =:= [] ->
            drop_self(SelfState);
        true ->
            {error, SrcDropResult, SelfState}
    end.

save(_, _, _) ->
    erlang:error(not_implemented).

scan(_, _, _) ->
    erlang:error(not_implemented).

scan(_, _, _, _, _) ->
    erlang:error(not_implemented).

fold_objects(_, _, _) ->
    erlang:error(not_implemented).

find_expired(_, _) ->
    erlang:error(not_implemented).

delete(_, _, _) ->
    erlang:error(not_implemented).

is_empty(_) ->
    erlang:error(not_implemented).


%% PRIVATE

init_sequence(Table, Config) ->
    DataRoot = etsdb_util:propfind(data_root, Config, "./data"),
    SequencePaths = etsdb_dbsequence_proxy_fileaccess:read_sequence(DataRoot),
    lists:foreach(
        fun(Path) ->
            TimeStampRange = filename:basename(Path),
            [FromStr, ToStr] = string:tokens(TimeStampRange, "-"),
            Item = #backend_info{
                start_timestamp = list_to_integer(FromStr),
                last_timestamp = list_to_integer(ToStr),
                path = Path
            },
            ets:insert(Table, Item)
        end,
        SequencePaths).

drop_self(#state{config = Config}) ->
    DataRoot = etsdb_util:propfind(data_root, Config, "./data"),
    etsdb_dbsequence_proxy_fileaccess:remove_root_path(DataRoot).


