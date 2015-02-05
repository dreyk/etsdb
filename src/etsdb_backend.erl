%%%-------------------------------------------------------------------
%%% @author lol4t0
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Jan 2015 18:00
%%%-------------------------------------------------------------------
-module(etsdb_backend).
-author("lol4t0").

-include("etsdb_request.hrl").


-type partition() :: non_neg_integer().
-type config() :: proplists:proplist().
-type state() :: term().
-type state_or_error() :: {ok, state()} | {error, Reason :: term(), state()}.
-type bucket()::module().
-type key() :: term().
-type value() :: term().
-type kv()::{key(), value()}.
-type kv_list()::[kv()].

-type range() :: term(). %% defined by bucket
-type acc()::term(). %% determined by user function


-type scan_query()::[#pscan_req{}].
-type fold_function() :: fun(() -> {ok, acc()} | {error, Reason :: term()}).
-type scan_result()::{async, fold_function()}.
-type fold_objects_function() :: fun((key(), value(), acc()) -> acc()).

-type expired_records()::{expired_records, {Count :: non_neg_integer(), [key()]}}.
-type find_expired_fun() :: fun(() -> expired_records()). 
-type find_expired_result() :: {async, find_expired_fun()}.

-export_type([partition/0, config/0, state/0, state_or_error/0, bucket/0, kv_list/0, expired_records/0, scan_query/0, 
    fold_function/0, scan_result/0, fold_objects_function/0]).

-callback init(Partition :: partition(), Config :: config()) -> {ok, state()} | {error, Reason :: term()}.
-callback stop(State :: state()) -> ok.

-callback drop(State :: state()) -> state_or_error().

-callback save(Bucket :: bucket(), Data :: kv_list(), State :: state()) -> state_or_error().
-callback scan(Request :: scan_query(), Acc :: acc(), State :: state()) -> scan_result().
-callback scan(Bucket :: bucket(), From :: range(), To :: range(), Acc :: acc(), State :: state()) -> scan_result().
-callback fold_objects(FoldFun :: fold_objects_function(), Acc :: acc(), State :: state()) -> scan_result().

-callback find_expired(Bucket :: bucket(), State :: state()) -> find_expired_result().
-callback delete(Bucket :: bucket(), KeysToDelete :: [key()], State :: state()) -> state_or_error(). %% should delete records and expired

-callback is_empty(State :: state()) -> boolean().



