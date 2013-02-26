-ifndef(TKVRECORD_PB_H).
-define(TKVRECORD_PB_H, true).
-record(tkvrecord, {
    id = erlang:error({required, id}),
    time = erlang:error({required, time}),
    value = erlang:error({required, value})
}).
-endif.

-ifndef(TKVBATCH_PB_H).
-define(TKVBATCH_PB_H, true).
-record(tkvbatch, {
    records = []
}).
-endif.

-ifndef(TKVRESP_PB_H).
-define(TKVRESP_PB_H, true).
-record(tkvresp, {
    status = erlang:error({required, status})
}).
-endif.

