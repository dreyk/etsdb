%% Author: gunin
%% Created: Nov 28, 2013
%% Description: TODO: Add description to etsdb_scan
-module(etsdb_scan).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([scan/2]).

scan(ScanReq,Timeout)->
    ReqRef = make_ref(),
    Me = self(),
    etsdb_scan_master_fsm:start_link({raw,ReqRef,Me}, ScanReq, Timeout),
    case wait_for_results(ReqRef,client_wait_timeout(Timeout)) of
        {ok,Res} when is_list(Res)->
            {ok,Res};
        Else->
            lager:error("Bad scan responce for range (~p - ~p) ~p used timeout ~p",[ScanReq,Timeout]),
            etsdb_util:make_error_response(Else)
    end.

wait_for_results(ReqRef,Timeout)->
    receive 
        {ReqRef,Res}->
            Res
    after Timeout->
            {error,timeout}
    end.

%%Add 50ms to operation timeout
client_wait_timeout(Timeout)->
    Timeout + 50.