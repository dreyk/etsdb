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

%% @doc Simple implementation tsdb bucket for storing any data in {id,time} order
-module(etsdb_tkb).



-export([
  api_version/0,
  w_val/0,
  r_val/0,
  w_quorum/0,
  r_quorum/0,
  make_partitions/1,
  serialize/1,
  clear_period/0,
  read/4,
  def_join_fun/2,
  def_end_fun/1,
  read_spec/3,
  partition_by_time/2]).

-behaviour(etsdb_bucket).

-author('Alex G. <gunin@mail.mipt.ru>').


-define(LIFE_TIME, 86400000 * 7). %%One week

-define(MAX_EXPIRED_COUNT, 100000).

-define(CLEAR_PERIOD, 300 * 1000). %%Five minutes

-define(PREFIX, "pv").
-include("etsdb_request.hrl").

api_version() ->
  "0.1".

w_val() ->
  3.
r_val() ->
  3.
w_quorum() ->
  2.
r_quorum() ->
  2.

clear_period() ->
  ?CLEAR_PERIOD.

make_partitions(Datas) when is_list(Datas) ->
  [{make_partition(Data), Data} || Data <- Datas];
make_partitions(Data) ->
  [{make_partition(Data), Data}].

make_partition({{ID, _Time}, _Value}) ->
  partiotion_by_region(ID).



serialize(Datas) when is_list(Datas) ->
  Batch = lists:foldl(fun(Data, Acc) ->
    serialize_internal(Data, Acc) end, [], Datas),
  lists:keysort(1, Batch);

serialize(Data) ->
  serialize_internal(Data, []).

serialize_internal({{ID, Time}, Value}, Acc) ->
  Key = sext:encode({?PREFIX, ID, Time}),
  [{Key, <<Value/binary>>} | Acc].

partiotion_by_region(ID) when is_binary(ID) ->
  ID;
partiotion_by_region(ID) ->
  term_to_binary(ID).


read_spec(_ID, Time1, Time2) when Time1 > Time2 ->
  empty;
read_spec(ID, Time1, Time2) ->
  PScan = #pscan_req{partition = partiotion_by_region(ID),
  n_val = 3,
  quorum = 2,
  function = {?MODULE, read, [ID, Time1, Time2]}},
  #scan_req{pscan = PScan,
  end_fun = {?MODULE, def_end_fun, []},
  join_fun = {?MODULE, def_join_fun, []}}.

read(ID,From, To, _BackEnd) ->
  StartKey = sext:encode({?PREFIX, ID,From}),
  StopKey = sext:encode({?PREFIX, ID,To}),
  Fun = fun
    ({K, V}, Acc) when K >= StartKey andalso K =< StopKey ->
      case catch sext:decode(K) of
        {?PREFIX,ID,Time}->
          [{{ID,Time},V}|Acc];
        _->
          throw({break, Acc})
      end;
    (_V, Acc) ->
      throw({break, Acc})
  end,
  {StartKey, Fun, 1}.

def_end_fun(Data) ->
  lists:reverse(Data).

def_join_fun(NewData, OldData) ->
  orddict:merge(fun(_, V1, _) -> V1 end, NewData, OldData).

partition_by_time(_KvList, _RotationInterval) ->
  error('not implemented').
