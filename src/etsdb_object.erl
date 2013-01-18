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

%% @doc Interface functions for serialize/unserialize user objects.
-module(etsdb_object).

-export([make_kv/3]).

make_kv(Bucket,{batch,UserObjects},TimeStamp)->
	Vals = lists:foldl(fun(UserObject,Acc)->
							   StorageObject = make_kv(Bucket,UserObject,TimeStamp),
							   [StorageObject|Acc] end,[],UserObjects),
	lists:reverse(Vals);
make_kv(Bucket,UserObject,TimeStamp)->
	{UserKey,UserData} = Bucket:serialize(UserObject),
	Prefix = atom_to_binary(Bucket,latin1),
	StorageKey = <<Prefix/binary,":",UserKey/binary>>,
	StorageData = <<1,TimeStamp:64/integer,UserData/binary>>,
	{StorageKey,StorageData}.


