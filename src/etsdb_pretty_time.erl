%%%-------------------------------------------------------------------
%%% @author lol4t0
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. Jan 2015 12:39
%%%-------------------------------------------------------------------
-module(etsdb_pretty_time).
-author("lol4t0").

%% API
-export([to_millisec/1, to_sec/1]).

%% Modifiers are (s)econd, (m)inute, (h)our, (d)ay, (w)eek, ('M')onth, (y)ear.
%% If modifier is not specified assuming milliseconds

-type time_modifier() :: s | m | h | d | w | 'M' | y.
-type simple_time_interval() :: non_neg_integer() | {non_neg_integer(), time_modifier() }.
-type time_interval() :: simple_time_interval() | [simple_time_interval()].

-spec to_millisec(time_interval())->non_neg_integer().
to_millisec(X) when is_list(X) ->
    lists:sum(lists:map(fun to_millisec/1, X));
to_millisec(X) when is_number(X) ->
    X;
to_millisec({X, y}) ->
    to_millisec({X*365, d});
to_millisec({X, 'M'}) ->
    to_millisec({X*30, d});
to_millisec({X, w}) ->
    to_millisec({X*7, d});
to_millisec({X, d}) ->
    to_millisec({X * 24, h});
to_millisec({X, h}) ->
    to_millisec({X * 60, m});
to_millisec({X, m}) ->
    to_millisec({X*60, s});
to_millisec({X, s}) ->
    X * 1000.

-spec to_sec(time_interval())->non_neg_integer().
to_sec(X) ->
    to_millisec(X) div 1000.

