%% -------------------------------------------------------------------
%%
%% etsdb_bucket
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

%% @doc etsdb_bucket behaviour
-module(etsdb_bucket).
-author('Alex G. <gunin@mail.mipt.ru>').

-type key() :: binary().
-type timestamp() :: non_neg_integer(). %% timestamp in millisec

-callback api_version() -> any().
-callback w_val() -> pos_integer().
-callback r_val() -> pos_integer().
-callback w_quorum() -> pos_integer().
-callback r_quorum() -> pos_integer().
-callback make_partitions(any()) -> any(). %% TODO @Dreyk provide actual type spec.
-callback timestamp_for_keys([key()]) -> [{key(), timestamp()}].
-callback key_ranges_for_interval({timestamp(), timestamp()}) -> [{key(), key()}].
