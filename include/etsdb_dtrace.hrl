-include_lib("riak_core/include/riak_core_dtrace.hrl").
-define(DTRACE(Category, Ints, Strings),
        dtrace_int(Category, Ints, Strings)).