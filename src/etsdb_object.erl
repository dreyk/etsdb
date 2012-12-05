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

%% @doc Bootstrapping the etsdb extension application.
-module(etsdb_object).

-export([make_object/2]).

make_object(Bucket,UserObject)->
	{UserKey,UserData} = Bucket:serialize(UserObject),
	Prefix = atom_to_binary(Bucket,latin1),
	StorageKey = <<Prefix/binary,":",UserKey>>,
	{StorageKey,UserData}.


