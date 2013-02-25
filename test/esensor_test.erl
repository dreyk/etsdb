-module(esensor_test).


-export([test/2]).

test(SensorCount,BatchSize,SampleCount)->
	Start = os:timestamp(),
	start_sensors(SensorCount,BatchSize,SampleCount),
	wait_res(SensorCount),
	timer:now_diff(os:timestamp(),Start).


wait_res(0)->
	ok;
wait_res(C)->
	receive 
		done->
			wait_res(C-1)
	end.

start_sensors(0,_BatchSize,_SampleCount)->
	ok;
start_sensors(ID,BatchSize,SampleCount)->
	Me = self(),
	start_link(fun()->sensor(Me,ID,BatchSize,0,SampleCount) end),
	start_sensors(ID-1,BatchSize,SampleCount).


sensor(Master,_ID,_BatchSize,_Time,0)->
	Master ! done;
sensor(Master,ID,BatchSize,Time,SampleCount)->
	ok = etsdb_put:put(etsdb_tkb,generate_data(ID,Time,BatchSize,[])),
	sensor(Master,ID,BatchSize,Time+BatchSize,SampleCount-1).

generate_data(_,_,0,Acc)->
	Acc;
generate_data(ID,Time,I,Acc)->
	generate_data(ID,Time+1,I-1,[{{ID,Time},"test"}|Acc]).