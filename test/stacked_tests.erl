%% (c) 2015, Matthew Branton
%% BSD 2-clause, check /LICENSE


-module(stacked_tests).
-author('mbranton@emberfinancial.com').

-define(PATH, "/tmp/bitjartests").

-include_lib("../include/bitjar.hrl").
-include_lib("eunit/include/eunit.hrl").


start() -> ok.
stop(_) -> cleanup_directories().
cleanup_directories() -> os:cmd("rm -rf " ++ ?PATH).


stacked_lookup() ->
	{ok, L} = bitjar:open(leveldb, #{path => ?PATH}),
	{ok, E} = bitjar:open(ets, []),
	{ok, L2} = bitjar:store(L, test, <<"key">>, <<"value">>),
	{ok, E2} = bitjar:store(E, random, <<"blah">>, <<"value2">>),
	{ok, E3} = bitjar:store(E2, test, <<"key2">>, <<"value2">>),
	%% Look for both
	{ok, _} = bitjar:lookup([L2,E3], [{test, <<"key">>}, {test, <<"key2">>}]),
	%% Look for second
	{ok, _} = bitjar:lookup([L2,E3], [{test, <<"key2">>}]),
	ok = bitjar:close(L2),
	ok = bitjar:close(E3).

stacked_delete() ->
	{ok, L} = bitjar:open(leveldb, #{path => ?PATH}),
	{ok, E} = bitjar:open(ets, []),
	{ok, L2} = bitjar:store(L, test, <<"key">>, <<"value">>),
	{ok, E3} = bitjar:store(E, test, <<"key">>, <<"value2">>),
	[L3, E4] = bitjar:delete([L2, E3], test, <<"key">>),
	?assertEqual(not_found, bitjar:lookup([L3, E4], test, <<"key">>)),
	ok = bitjar:close(L3),
	ok = bitjar:close(E4).


leveldb_test_() -> 
  {foreach,
  fun start/0,
  fun stop/1,
   [
			{"Stacked lookup", fun stacked_lookup/0},
			{"Stacked delete", fun stacked_delete/0}
   ]}.
