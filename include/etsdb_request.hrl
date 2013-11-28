-record(etsdb_store_req_v1,{bucket::module(),value::term(),timestamp::non_neg_integer(),req_id::term()}).
-record(etsdb_get_query_req_v1,{bucket::module(),get_query::term(),req_id::term()}).
-record(etsdb_innerstore_req_v1,{value,req_id::term()}).
-record(etsdb_get_cell_req_v1,{bucket::module(),value,filter,req_id::term()}).

-record(etsdb_store_res_v1,{count,error_count,errors}).

-record(scan_it,{rgn_count,partition,rgn,from,to,start_rgn,end_rgn}).
-record(pscan_req,{partition,n_val,quorum,function}).
-record(scan_req,{end_fun,join_fun,pscan}).

-define(ETSDB_STORE_REQ, #etsdb_store_req_v1).
-define(ETSDB_GET_QUERY_REQ, #etsdb_get_query_req_v1).
-define(ETSDB_INNERSTORE_REQ, #etsdb_innerstore_req_v1).
-define(ETSDB_GET_CELL_REQ, #etsdb_get_cell_req_v1).