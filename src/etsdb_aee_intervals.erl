%%%-------------------------------------------------------------------
%%% @author sidorov
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 31. Oct 2014 1:59 PM
%%%-------------------------------------------------------------------
-module(etsdb_aee_intervals).
-author("sidorov").

%% API

-type start_options() :: etsdb_aee:start_options().
-type time_interval() :: {non_neg_integer(), non_neg_integer()}.
-type time_intervals() :: [time_interval()].
-type bucket() :: etsdb_aee:bucket().

-export([generate_date_intervals/1, date_interval_to_string/1, should_exchange_interval/1, get_current_interval/1,
    get_keys_intervals/3]).

-spec generate_date_intervals(start_options()) -> time_intervals().
generate_date_intervals(Opts) ->
    GranularitySecs = zont_pretty_time:to_sec(zont_data_util:propfind(granularity, Opts)),
    NIntervals = zont_data_util:propfind(granularity_intervals_to_expire, Opts),
    Now = zont_time_util:system_time(sec),
    CurrentStart = Now - (Now rem GranularitySecs),
    Intervals = [{CurrentStart - (I + 1)* GranularitySecs, CurrentStart - I*GranularitySecs} || I <- lists:seq(0, NIntervals - 1)],
    Intervals.

-spec date_interval_to_string(time_interval()) -> string().
date_interval_to_string({Start, End}) ->
    io_lib:format("~20..0B_~20..0B", [Start, End]).

-spec should_exchange_interval(time_interval()) -> boolean().
should_exchange_interval({_Start, End}) ->
    Now = zont_time_util:system_time(sec),
    End < Now. %% not exchangin CURRENT interval , i.e. not yet finished.


-spec get_current_interval(time_intervals()) -> time_interval().
get_current_interval(Intervals) ->
    hd(Intervals). %% first interval is current according to the structure of the  INTERVALS date type.


-spec get_keys_intervals(bucket(), [binary()], time_intervals()) -> [{time_interval(), [binary()]}].
get_keys_intervals(Bucket, Keys, DateIntervals) ->
    KeysDates = Bucket:timestamp_for_keys(Keys),
    EmptyDict = dict:from_list([{Interval, []}||Interval<- DateIntervals]),
    IntervalKeys = lists:foldl(
        fun({K, TSMSec}, WorkDict) ->
            TS = TSMSec / 1000,
            KInterval = hd(lists:dropwhile(fun({Start, _End}) -> TS < Start end, DateIntervals)),
            dict:update(fun (IKeys) -> [K | IKeys] end, KInterval, WorkDict)
        end, EmptyDict, KeysDates),
    dict:to_list(IntervalKeys).


