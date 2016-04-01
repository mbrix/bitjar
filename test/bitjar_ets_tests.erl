%% (c) 2015, Matthew Branton
%% BSD 2-clause, check /LICENSE


-module(bitjar_ets_tests).
-author('mbranton@emberfinancial.com').

-define(PATH, "/tmp/bitjartests").

-include_lib("../include/bitjar.hrl").
-include_lib("eunit/include/eunit.hrl").

start() -> ok.
stop(_) -> ok.

init() ->
	{ok, B} = bitjar:open(ets, []),
	?assertMatch(#bitjar{}, B),
	?assertEqual(ok, bitjar:close(B)).

store() ->
  {ok, B} = bitjar:open(ets, []),
  {ok, B2} = bitjar:store(B, test, <<"key">>, <<"value">>),
  {ok, B3} = bitjar:store(B2, test, <<"key2">>, <<"value2">>),
  {ok, B4} = bitjar:store(B3, test2, <<"key">>, <<"value2">>),
  {ok, B5} = bitjar:store(B4, test3, <<"key">>, <<"value2">>),
  {ok, B6} = bitjar:store(B5, test3, <<"key2">>, <<"value2">>),
  G = bitjar:groups(B6),
  ?assertEqual(3, maps:size(G)),
  ok = bitjar:close(B),
  {ok, C} = bitjar:open(ets, []),
  G2 = bitjar:groups(C),
  ?assertEqual(0, maps:size(G2)),
  ok = bitjar:close(C).

lookup() ->
	{ok, B} = bitjar:open(ets, []),
	{ok, B2} = bitjar:store(B, test, <<"key">>, <<"value">>),
	{ok, B3} = bitjar:store(B2, test, <<"key2">>, <<"value">>),
	{ok, B4} = bitjar:store(B3, test2, <<"key">>, <<"value">>),
	{ok, [{test2, <<"key">>, <<"value">>}]} = bitjar:lookup(B4, test2, <<"key">>),
	{partial, _, _} = bitjar:lookup(B4, [{test, <<"key">>}, {test, <<"key2">>}, {test, <<"key3">>}]),
	{ok, _} = bitjar:lookup(B4, [{test, <<"key">>}, {test, <<"key2">>}, {test2, <<"key">>}]),
	ok = bitjar:close(B4).

delete() ->
	{ok, B} = bitjar:open(ets, []),
	{ok, B2} = bitjar:store(B, test, <<"key">>, <<"value">>),
	{ok, [{test, <<"key">>, <<"value">>}]} = bitjar:lookup(B2, test, <<"key">>),
	{ok, B3} = bitjar:delete(B2, test, <<"key">>),
	?assertMatch(not_found, bitjar:lookup(B3, test, <<"key">>)),
	ok = bitjar:close(B3).

all() ->
	{ok, B} = bitjar:open(ets, []),
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
	
foldl() ->
	{ok, B} = bitjar:open(ets, []),
	?assertMatch([], bitjar:foldl(B, fun(_K, _V, _Acc) -> ok end, [], test)),
	{ok, B2} = bitjar:store(B, test, <<"mykey">>, <<"myvalue">>),
	?assertMatch([{folded, <<"mykey">>, <<"myvalue">>}],
				 bitjar:foldl(B2, fun(K,V,Acc) -> [{folded, K, V}|Acc] end, [], test)),
	ok = bitjar:close(B2).

filter() ->
	{ok, B} = bitjar:open(ets, []),
	{ok, B2} = bitjar:store(B, test, <<"mykey">>, <<"myvalue">>),
	{ok, B3} = bitjar:store(B2, test, <<"mykey2">>, <<"myvalue2">>),
	{ok, B4} = bitjar:store(B3, test, <<"mykey3">>, <<"myvalue3">>),
	?assertMatch([], bitjar:filter(B4, fun({_, _}) -> false end, test)),
	?assertMatch([_,_,_], bitjar:filter(B4, fun({_,_}) -> true end, test)),
	ok = bitjar:close(B4).

multistore() ->
	{ok, B} = bitjar:open(ets, []),
	{ok, B2} = bitjar:store(B, [{test, <<"mykey">>, <<"myvalue">>},
								{test, <<"m2">>, <<"k2">>},
								{test, <<"m3">>, <<"k3">>}]),
	?assertMatch([_,_,_], bitjar:all(B2, test)),
	ok = bitjar:close(B2).

serialization() ->
	{ok, B} = bitjar:open(ets, []),
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
	%% Multiple serialization setup
	{ok, B4} = bitjar:set_serializers(B, [
										  [test2,
										   SerialFun,
										   SerialFun,
										   KDeserialFun,
										   VDeserialFun],
										  [test3,
										   SerialFun,
										   SerialFun,
										   KDeserialFun,
										   VDeserialFun]
										 ]),
	ok = bitjar:close(B4).

range() ->
	{ok, B} = bitjar:open(ets, []),
	{ok, B2} = bitjar:store(B, [{test, {<<"key">>,0}, <<"value8">>},
	                            {test, {<<"key">>, 1}, <<"value">>},
								{test, {<<"key">>, 2}, <<"value2">>},
								{test, {<<"key">>, 3}, <<"value3">>},
								{test, {<<"key">>, 4}, <<"value4">>},
								{test, {<<"key">>, 5}, <<"value5">>},
								{test, {<<"otherkey">>,0}, <<"value">>},
								{test, {<<"otherkey">>,1}, <<"value">>}]),
	{ok, Values} = bitjar:range(B2, test, <<"key">>),
    {ok, Values2} = bitjar:range(B2, test, <<"otherkey">>),
	?assertEqual(6, length(Values)),
	?assertEqual(2, length(Values2)),

	%% Gaps
    {ok, B3} = bitjar:store(B2, [{test, {<<"key9">>, 0}, <<"value">>},
                                 {test, {<<"key7">>,10}, <<"value">>},
                                 {test, {<<"key9">>,10}, <<"value">>},
                                 {test, {<<"key8">>,10}, <<"value">>},
                                 {test, {<<"key9">>,22009}, <<"value">>}]),
    {ok, SkippedValues} = bitjar:range(B3, test, <<"key9">>),
    ?assertEqual(3, length(SkippedValues)).

bigrange() ->
	{ok, B} = bitjar:open(ets, []),
	B3 = lists:foldl(fun(Num, Acc) ->
							 {ok, B2} = bitjar:store(Acc, [{test, {<<"key">>, Num}, <<"value">>}]),
							 B2
					 end, B, lists:seq(1, 1001)),
	{ok, Values} = bitjar:range(B3, test, <<"key">>),
	?assertEqual(1001, length(Values)),
	[A|_] = Values,
	[Last|_] = lists:reverse(Values),
	?assertEqual({test,{<<"key">>,1},<<"value">>}, A),
	?assertEqual({test,{<<"key">>,1001},<<"value">>}, Last).

delete_then_store() ->
	{ok, B} = bitjar:open(ets, []),
	{ok, B2} = bitjar:store(B, [{test, <<"key">>, <<"value">>},
								{test, <<"key2">>, <<"value2">>},
								{test, <<"key3">>, <<"value3">>}]),
	{ok, B3} = bitjar:delete_then_store(B2,
							[{test, <<"key">>}, {test, <<"key2">>}, {test, <<"key3">>}],
							[{test, <<"key5">>, <<"blah">>}]),
	?assertMatch([{<<"key5">>, <<"blah">>}], bitjar:all(B3, test)),
	ok = bitjar:close(B3).


foldl_nolist() ->
	{ok, B} = bitjar:open(ets, []),
	{ok, B2} = bitjar:store(B, [{test, <<"key">>, <<"value">>}]),
	?assertMatch(ok, bitjar:foldl(B2, fun(_K, _V, _Acc) -> ok end, noacc, test)).


ets_test_() -> 
  {foreach,
  fun start/0,
  fun stop/1,
   [
            {"Bitjar init", fun init/0},
			{"Store values", fun store/0},
			{"lookup", fun lookup/0},
			{"delete", fun delete/0},
			{"all", fun all/0},
			{"fold", fun foldl/0},
			{"filter", fun filter/0},
			{"multistore", fun multistore/0},
			{"serialization", fun serialization/0},
			{"range", fun range/0},
			{"big ranges", fun bigrange/0},
			{"delete then store", fun delete_then_store/0},
			{"foldl no list", fun foldl_nolist/0}
   ]}.
