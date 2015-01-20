%%%-------------------------------------------------------------------
%%% @author lol4t0
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Jan 2015 11:18
%%%-------------------------------------------------------------------
-module(etsdb_leveldb_affinity).
-author("lol4t0").

%% API
-export([get_path/2]).

-include_lib("eunit/include/eunit.hrl").


-type index()::non_neg_integer().
-type path()::string().

-spec get_path(index(), [proplists:proplist()]) -> path().
get_path(Index, Props) ->
    PathListOrPath = etsdb_util:propfind(data_root, Props, ["./data/leveldb"]),
    PathList = case [X||X <- PathListOrPath, is_list(X)] of
                   [] -> %% PathList was just string
                        [PathListOrPath];
                   V when V == PathListOrPath ->
                       PathListOrPath;
                   _ ->
                       error({"Invalid leveldb path spec", PathListOrPath})
               end,
    Modulus = length(PathList),
    PathIndex = (Index rem Modulus) + 1,
    lists:nth(PathIndex, PathList).


%% TEST

list_test_() ->
    Prop = [
                {data_root, ["./data/leveldb/1", "./data/leveldb/2", "./data/leveldb/3", "./data/leveldb/4", "./data/leveldb/5"]},
                {write_buffer_size, 8388608},
                {cache_size, 8388608},
                {max_open_files, 100}
            ],
    [
        ?_assertEqual(get_path(0, Prop), "./data/leveldb/1"),
        ?_assertEqual(get_path(1, Prop), "./data/leveldb/2"),
        ?_assertEqual(get_path(103353454656754623110575, Prop), "./data/leveldb/1"),
        ?_assertEqual(get_path(12335435467564343245236579, Prop), "./data/leveldb/5")
    ].

string_test_() ->
    Prop = [
        {data_root, ["./data/leveldb"]},
        {write_buffer_size, 8388608},
        {cache_size, 8388608},
        {max_open_files, 100}
    ],
    [
        ?_assertEqual(get_path(0, Prop), "./data/leveldb"),
        ?_assertEqual(get_path(1, Prop), "./data/leveldb"),
        ?_assertEqual(get_path(103353454656754623110575, Prop), "./data/leveldb"),
        ?_assertEqual(get_path(12335435467564343245236579, Prop), "./data/leveldb")
    ].

invalid_spec_test_() ->
    Prop = [
        {data_root, ["./data/leveldb", wtf]},
        {write_buffer_size, 8388608},
        {cache_size, 8388608},
        {max_open_files, 100}
    ],
    [
        ?_assertError({"Invalid leveldb path spec", ["./data/leveldb", wtf]}, get_path(0, Prop)),
        ?_assertError({"Invalid leveldb path spec", ["./data/leveldb", wtf]}, get_path(1, Prop))
    ].

empty_spec_test_() ->
    Prop = [
        {data_root, ["./data/leveldb"]},
        {write_buffer_size, 8388608},
        {cache_size, 8388608},
        {max_open_files, 100}
    ],
    [
        ?_assertEqual(get_path(0, Prop), "./data/leveldb"),
        ?_assertEqual(get_path(1, Prop), "./data/leveldb"),
        ?_assertEqual(get_path(103353454656754623110575, Prop), "./data/leveldb"),
        ?_assertEqual(get_path(12335435467564343245236579, Prop), "./data/leveldb")
    ].
