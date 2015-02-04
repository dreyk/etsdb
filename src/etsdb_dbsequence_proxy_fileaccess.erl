%%%-------------------------------------------------------------------
%%% @author lol4t0
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Jan 2015 16:03
%%%-------------------------------------------------------------------
-module(etsdb_dbsequence_proxy_fileaccess).
-author("lol4t0").

%% API
-export([read_sequence/1, remove_dir_recursively/1]).



-spec read_sequence(file:filename()) -> [file:filename()].
read_sequence(RootPath) ->
    case file:list_dir(RootPath) of
        {ok, Entries} ->
            [filename:join(RootPath, E) || E <- Entries];
        {error, Reason} ->
            error({failed_reading_sequnce, RootPath, Reason})
    end.

-spec remove_dir_recursively(file:filename()) -> true | {error, Reason :: term()}.
remove_dir_recursively(Dir) ->
    case remove_dir_recursively(Dir, []) of
        [] ->
            true;
        Errs ->
            {error, Errs}
    end.

remove_dir_recursively(Dir, Errs) ->
    case file:list_dir_all(Dir) of
        {ok, Entries} ->
            NewErrs = lists:foldl(
                fun(Entry, CurrErrs) ->
                    CurrPath = filename:join(Dir, Entry),
                    case filelib:is_dir(CurrPath) of
                        true ->
                            remove_dir_recursively(CurrPath, CurrErrs);
                        false ->
                            remove_file(CurrPath, CurrErrs)
                    end
                end, Errs, Entries),
            case file:del_dir(Dir) of
                ok ->
                    NewErrs;
                {error, Reason} ->
                    [{Dir, Reason} | NewErrs]
            end;
        {error, Reason} ->
            [{Dir, Reason} | Errs]
    end.

remove_file(FileName, Errs) ->
    case file:delete(FileName) of
        ok -> Errs;
        {error, Reason} -> [{FileName, Reason} | Errs]
    end.
