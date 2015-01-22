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
-type reason() :: term().
-type state_or_error() :: {ok, state()} | {error, reason(), state()}.
-type bucket()::module().
-type key() :: term().
-type value() :: term().
-type kv()::{key(), value()}.
-type kv_list()::[kv()].
-type expired_records()::{expired_records, {non_neg_integer(), [key()]}}.

-type range() :: term(). %% defined by bucket
-type acc()::term(). %% determined by user function


-type scan_query()::[#pscan_req{}].
-type fold_function() :: fun(() -> {ok, acc()} | {error, term()}).
-type scan_result()::{async, fold_function()}.
-type fold_objects_function() :: fun((key(), value(), acc()) -> acc()).

-callback init(partition(), config()) -> {ok, state()} | {error, reason()}.
-callback stop(state()) -> ok.

-callback drop(state()) -> state_or_error().

-callback save(bucket(), kv_list(),state()) -> state_or_error().
-callback scan(scan_query(), acc(), state()) -> scan_result().
-callback scan(bucket(), range(), range(), acc(), state()) -> scan_result().
-callback fold_objects(fold_objects_function(), acc(), state()) -> scan_result().

-callback find_expired(bucket(), state()) -> expired_records().
-callback delete(bucket(), [key()], state()) -> state_or_error(). %% should delete records and expired

-callback is_empty(state()) -> {boolean(), state()}.



