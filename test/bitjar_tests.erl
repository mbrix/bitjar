%% (c) 2015, Matthew Branton
%% BSD 2-clause, check /LICENSE


-module(bitjar_tests).
-author('mbranton@emberfinancial.com').

-define(PATH, "/tmp/bitjartests").
-define(PATH2, "/tmp/bitjartests2").

-include_lib("../include/bitjar.hrl").
-include_lib("eunit/include/eunit.hrl").


start_leveldb() -> ok.
stop_leveldb(_) -> cleanup_directories().
cleanup_directories() -> 
    os:cmd("rm -rf " ++ ?PATH),
    os:cmd("rm -rf " ++ ?PATH2).

%%%% LevelDB bitjar backend
ldb_init() ->
	{ok, B} = bitjar:open(leveldb, #{path => ?PATH}),
	?assertMatch(#bitjar{}, B),
	?assertEqual(ok, bitjar:close(B)).

ldb_store() ->
	{ok, B} = bitjar:open(leveldb, #{path => ?PATH}),
	{ok, B2} = bitjar:store(B, test, <<"key">>, <<"value">>),
	{ok, B3} = bitjar:store(B2, test, <<"key2">>, <<"value2">>),
	{ok, B4} = bitjar:store(B3, test2, <<"key">>, <<"value2">>),
	{ok, B5} = bitjar:store(B4, test3, <<"key">>, <<"value2">>),
	{ok, B6} = bitjar:store(B5, test3, <<"key2">>, <<"value2">>),
	G = bitjar:groups(B6),
	?assertEqual(3, maps:size(G)),
	ok = bitjar:close(B),
	{ok, C} = bitjar:open(leveldb, #{path => ?PATH}),
	G2 = bitjar:groups(C),
	?assertEqual(3, maps:size(G2)),
	ok = bitjar:close(C).

ldb_lookup() ->
	{ok, B} = bitjar:open(leveldb, #{path => ?PATH}),
	{ok, B2} = bitjar:store(B, test, <<"key">>, <<"value">>),
	{ok, B3} = bitjar:store(B2, test, <<"key2">>, <<"value">>),
	{ok, B4} = bitjar:store(B3, test2, <<"key">>, <<"value">>),
	{ok, [{test2, <<"key">>, <<"value">>}]} = bitjar:lookup(B4, test2, <<"key">>),
	{partial, _, _} = bitjar:lookup(B4, [{test, <<"key">>}, {test, <<"key2">>}, {test, <<"key3">>}]),
	{ok, _} = bitjar:lookup(B4, [{test, <<"key">>}, {test, <<"key2">>}, {test2, <<"key">>}]),
	ok = bitjar:close(B4).

ldb_delete() ->
	{ok, B} = bitjar:open(leveldb, #{path => ?PATH}),
	{ok, B2} = bitjar:store(B, test, <<"key">>, <<"value">>),
	{ok, [{test, <<"key">>, <<"value">>}]} = bitjar:lookup(B2, test, <<"key">>),
	{ok, B3} = bitjar:delete(B2, test, <<"key">>),
	?assertMatch(not_found, bitjar:lookup(B3, test, <<"key">>)),
	ok = bitjar:close(B3).

ldb_all() ->
	{ok, B} = bitjar:open(leveldb, #{path => ?PATH}),
	?assertMatch([], bitjar:all(B, missinggroup)),
	{ok, B2} = bitjar:store(B, test, <<"key">>, <<"value">>),
	{ok, B3} = bitjar:delete(B2, test, <<"key">>),
	?assertMatch([], bitjar:all(B3, testgroup)),
	?assertMatch({ok, _}, maps:find(test, bitjar:groups(B3))),
	{ok, B4} = bitjar:store(B3, test, <<"key2">>, <<"value2">>),
	{ok, B5} = bitjar:store(B4, test2, <<"k">>, <<"v">>),
	{ok, B6} = bitjar:store(B5, test2, <<"k2">>, <<"v2">>),
	?assertMatch([{<<"key2">>, <<"value2">>}], bitjar:all(B6, test)),
	?assertMatch([{<<"k">>, <<"v">>}, {<<"k2">>, <<"v2">>}], bitjar:all(B6, test2)),
	ok = bitjar:close(B6).
	
ldb_foldl() ->
	{ok, B} = bitjar:open(leveldb, #{path => ?PATH}),
	?assertMatch([], bitjar:foldl(B, fun(_K, _V, _Acc) -> ok end, [], test)),
	{ok, B2} = bitjar:store(B, test, <<"mykey">>, <<"myvalue">>),
	?assertMatch([{folded, <<"mykey">>, <<"myvalue">>}],
				 bitjar:foldl(B2, fun(K,V,Acc) -> [{folded, K, V}|Acc] end, [], test)),
	ok = bitjar:close(B2).

ldb_filter() ->
	{ok, B} = bitjar:open(leveldb, #{path => ?PATH}),
	{ok, B2} = bitjar:store(B, test, <<"mykey">>, <<"myvalue">>),
	{ok, B3} = bitjar:store(B2, test, <<"mykey2">>, <<"myvalue2">>),
	{ok, B4} = bitjar:store(B3, test, <<"mykey3">>, <<"myvalue3">>),
	?assertMatch([], bitjar:filter(B4, fun({_, _}) -> false end, test)),
	?assertMatch([_,_,_], bitjar:filter(B4, fun({_,_}) -> true end, test)),
	ok = bitjar:close(B4).

ldb_multistore() ->
	{ok, B} = bitjar:open(leveldb, #{path => ?PATH}),
	{ok, B2} = bitjar:store(B, [{test, <<"mykey">>, <<"myvalue">>},
								{test, <<"m2">>, <<"k2">>},
								{test, <<"m3">>, <<"k3">>}]),
	?assertMatch([_,_,_], bitjar:all(B2, test)),
	ok = bitjar:close(B2).

serialization() ->
	{ok, B} = bitjar:open(leveldb, #{path => ?PATH}),
	SerialFun = fun(E) -> erlang:term_to_binary(E) end,
	KDeserialFun = fun(K) -> erlang:binary_to_term(K) end,
	VDeserialFun = fun(_, E) -> erlang:binary_to_term(E) end,
	{ok, B2} = bitjar:set_serializer(B, test,
						  SerialFun, %% Key Serializer
						  SerialFun, %% Value Serializer
						  KDeserialFun, %% Key Deserializer
						  VDeserialFun), %% Value Deserializer
	{ok, B3} = bitjar:store(B2, [{test, mykey, "myvalue"}]),
	?assertEqual({ok, [{test, mykey, "myvalue"}]},
				 bitjar:lookup(B3, test, mykey)),
	ok = bitjar:close(B2).

range() ->
	{ok, B} = bitjar:open(leveldb, #{path => ?PATH}),
	{ok, B2} = bitjar:store(B, [{test, <<"key">>, <<"value">>},
								{test, <<"key2">>, <<"value2">>},
								{test, <<"key3">>, <<"value3">>},
								{test, <<"key4">>, <<"value4">>},
								{test, <<"key5">>, <<"value5">>},
								{test, <<"otherkey">>, <<"value">>}]),
	{ok, Values} = bitjar:range(B2, test, <<"key">>),
	?assertEqual(5, length(Values)).

bigrange() ->
	{ok, B} = bitjar:open(leveldb, #{path => ?PATH}),
	B3 = lists:foldl(fun(Num, Acc) ->
							 BinNum =  list_to_binary(integer_to_list(Num)),
							 {ok, B2} = bitjar:store(Acc, [{test, <<"key", BinNum/binary>>, <<"value">>}]),
							 B2
					 end, B, lists:seq(1, 1001)),
	{ok, Values} = bitjar:range(B3, test, <<"key">>),
	?assertEqual(1001, length(Values)),
	[A|_] = Values,
	%% Order is not actually guaranteed
	?assertEqual({test,<<"key1">>,<<"value">>}, A),
	?assertEqual({test, <<"key1001">>,<<"value">>}, lists:keyfind(<<"key1001">>, 2, Values)).


delete_then_store() ->
	{ok, B} = bitjar:open(leveldb, #{path => ?PATH}),
	{ok, B2} = bitjar:store(B, [{test, <<"key">>, <<"value">>},
								{test, <<"key2">>, <<"value2">>},
								{test, <<"key3">>, <<"value3">>}]),
	{ok, B3} = bitjar:delete_then_store(B2,
							[{test, <<"key">>}, {test, <<"key2">>}, {test, <<"key3">>}],
							[{test, <<"key5">>, <<"blah">>}]),
	?assertMatch([{<<"key5">>, <<"blah">>}], bitjar:all(B3, test)),
	ok = bitjar:close(B3).

multiple_open() ->
	{ok, B} = bitjar:open(leveldb, #{path => ?PATH}),
	{ok, B2} = bitjar:store(B, [{test, <<"key">>, <<"value">>},
							    {test2, <<"key">>, <<"value">>}]),
	ok = bitjar:close(B2),
	{ok, C} = bitjar:open(leveldb, #{path => ?PATH}),
	G = bitjar:groups(C),
	?assertEqual(2, maps:size(G)). 


foldl_nolist() ->
	{ok, B} = bitjar:open(leveldb, #{path => ?PATH}),
	{ok, B2} = bitjar:store(B, [{test, <<"key">>, <<"value">>}]),
	?assertMatch(ok, bitjar:foldl(B2, fun(_K, _V, _Acc) -> ok end, noacc, test)).

stacked_lookup() ->
	{ok, B} = bitjar:open(leveldb, #{path => ?PATH}),
	{ok, B2} = bitjar:store(B, [{test, <<"key">>, <<"value">>},
								{test, <<"key2">>, <<"value2">>},
								{test, <<"key3">>, <<"value3">>},
							    {test, <<"key4">>, <<"value4">>},
							    {test, <<"key5">>, <<"value5">>}]),
    {ok, C} = bitjar:open(leveldb , #{path => ?PATH2}),
	{ok, C2} = bitjar:store(C, [{test, <<"key">>, <<"value_hidden">>},
								{test, <<"key2">>, <<"value2_hidden">>},
								{test, <<"key3">>, <<"value3_hidden">>},
							    {test, <<"key7">>, <<"value7">>},
							    {test, <<"key8">>, <<"value9">>}]),

	?assertEqual({ok, [{test, <<"key">>, <<"value">>}]}, bitjar:lookup([B2, C2], test, <<"key">>)),
	?assertEqual({ok, [{test, <<"key">>, <<"value_hidden">>}]}, bitjar:lookup([C2, B2], test, <<"key">>)),
	?assertEqual({ok, [{test, <<"key4">>, <<"value4">>}]}, bitjar:lookup([B2, C2], test, <<"key4">>)),
	?assertEqual({ok, [{test, <<"key4">>, <<"value4">>}]}, bitjar:lookup([C2, B2], test, <<"key4">>)),
	?assertEqual({ok, [{test, <<"key7">>, <<"value7">>}]}, bitjar:lookup([C2, B2], test, <<"key7">>)),
	?assertEqual({ok, [{test, <<"key7">>, <<"value7">>}]}, bitjar:lookup([B2, C2], test, <<"key7">>)).


leveldb_test_() -> 
  {foreach,
  fun start_leveldb/0,
  fun stop_leveldb/1,
   [
            {"Bitjar init", fun ldb_init/0},
			{"Store values", fun ldb_store/0},
			{"lookup", fun ldb_lookup/0},
			{"delete", fun ldb_delete/0},
			{"all", fun ldb_all/0},
			{"fold", fun ldb_foldl/0},
			{"filter", fun ldb_filter/0},
			{"multistore", fun ldb_multistore/0},
			{"Serialize and deserialize", fun serialization/0},
			{"Search sub range", fun range/0},
			{"range with continuations", fun bigrange/0},
			{"Delete then store (atomic)", fun delete_then_store/0},
			{"Multiple open", fun multiple_open/0},
			{"Foldl without list", fun foldl_nolist/0},
            {"Stacked lookups", fun stacked_lookup/0}
   ]}.

%%%%% Memory / ETS bitjar backend
%%%
