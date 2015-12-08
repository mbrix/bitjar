%% (c) 2015, Matthew Branton
%% BSD 2-clause, check /LICENSE
%% Generic storage interface

%% Memory Based Storage

-module(bitjar_storage_ets).

-include_lib("bitjar.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(bitjar).

-export([bitjar_init/1,
		 bitjar_shutdown/1,
		 bitjar_store/2,
		 bitjar_lookup/2,
		 bitjar_range/2,
		 bitjar_delete/2,
		 bitjar_delete_then_store/3,
		 bitjar_all/2,
		 bitjar_filter/5,
		 bitjar_foldl/4,
		 bitjar_default_options/0]).

-define(GROUP_ID, 1).

bitjar_init(Options) ->
	{ok, #{refmap => #{},
		   options => lists:merge(bitjar_default_options(), Options)}, ?GROUP_ID, #{}}. 

bitjar_shutdown(#bitjar{state = #{refmap := R}}) ->
	maps:fold(fun(_K, V, _AccIn) -> true = ets:delete(V) end, ok, R),
	ok.

bitjar_default_options() -> [ordered_set, {keypos, 1}, {read_concurrency, true}].

bitjar_store(#bitjar{}=B, StoreList) -> store(B, StoreList).

bitjar_lookup(#bitjar{}=B, LookupList) -> lookup(B, LookupList, length(LookupList), [], []).

bitjar_range(#bitjar{}=B, RangeList) -> range(B, RangeList, [], []).

bitjar_delete(#bitjar{}=B, DeleteList) -> delete(B, DeleteList).

bitjar_delete_then_store(B, DeleteList, StoreList) ->
	{ok, B2} = delete(B, DeleteList),
	store(B2, StoreList).

bitjar_all(#bitjar{}=B, GroupId) -> all(B, GroupId).

bitjar_filter(B, FilterFuns, GroupId, KdeserialFun, VdeserialFun) -> filter(B, FilterFuns, GroupId, KdeserialFun, VdeserialFun).

bitjar_foldl(#bitjar{}=B, Fun, Start, GroupId) -> foldl(B, Fun, Start, GroupId).



%% Internal Functions

store(B, []) -> {ok, B};
store(#bitjar{state=#{refmap := R}=S}=B, [{Id, K, V}|T]) ->
	case do_store(maps:find(Id, R), B, K, V) of
		ok -> store(B, T);
		{newref, Ref} -> store(B#bitjar{state=S#{refmap => maps:put(Id, Ref, R)}}, T)
	end.

do_store(error, #bitjar{state=#{options := O}}, K, V) ->
	lager:info("OPTIONS: ~p~n", [O]),
	Ref = ets:new(jar, O),
	true = ets:insert(Ref, {K, V}),
	{newref, Ref};

do_store({ok, Ref}, _B, K, V) ->
	true = ets:insert(Ref, {K, V}),
	ok.

lookup(_B, [], _L, [], _) -> not_found;
lookup(_B, [], _L, Acc, []) -> {ok, Acc};
lookup(_B, [], _L, Acc, LeftOver) -> {partial, Acc, LeftOver};

lookup(#bitjar{state=#{refmap := RefMap}}=B, [{GroupName, GroupId, Key}|T], L, Acc, LeftOver) ->
	case lookup_ets(maps:find(GroupId, RefMap), GroupName, Key, Acc) of
		not_found -> lookup(B, T, L, Acc, [{GroupName, Key}|LeftOver]);
		NewAcc -> lookup(B, T, L, NewAcc, LeftOver)
	end.

lookup_ets(error, _GroupName, _Key, Acc) -> Acc;
lookup_ets({ok, Ref}, GroupName, Key, Acc) ->
	case ets:lookup(Ref, Key) of
		[{K,V}] -> [{GroupName, K, V}|Acc];
		[] -> not_found
	end.

range(_B, [], [], _) -> not_found;
range(_B, [], Acc, []) -> {ok, Acc};
range(_B, [], Acc, LeftOver) -> {partial, Acc, LeftOver};

%% A range query presumably has the Key in the correct format for this group
%% in order to do a sub select of an ordered_set ets table.
range(#bitjar{state=#{refmap := RefMap}}=B, [{GroupName, GroupId, Key}|T], Acc, LeftOver) ->
	case do_range_lookup(maps:find(GroupId, RefMap), GroupName, Key, Acc) of
		not_found -> range(B, T, Acc, [{GroupName, Key}|LeftOver]);
		NewAcc -> range(B, T, NewAcc, LeftOver)
	end.

do_range_lookup(error, _GroupName, _Key, Acc) -> Acc;
do_range_lookup({ok, Ref}, GroupName, Key, Acc) ->
	MatchSpec = [{{{Key, '_'},'_'},
				  [],['$_']}],
	case ets:select(Ref, MatchSpec, 100) of
		'$end_of_table' -> Acc;
		{Matches, Continuation} ->  
			do_range_lookup(Continuation, GroupName, lists:map(fun({K,V}) -> {GroupName, K, V} end, Matches) ++ Acc)
	end.

do_range_lookup(Continuation, GroupName, Acc) ->
	case ets:select(Continuation) of 
		'$end_of_table' -> Acc;
		{Matches, Continuation} ->
			do_range_lookup(Continuation, GroupName, lists:map(fun({K,V}) -> {GroupName, K, V} end, Matches) ++ Acc)
	end.

delete(B, []) -> {ok, B};
delete(#bitjar{state=#{refmap := R}}=B, [{_GroupName, Id, K}|T]) ->
	ets_delete(maps:find(Id, R), K),
	delete(B, T).

ets_delete(error, _Key) -> ok;
ets_delete({ok, Ref}, Key) ->
	true = ets:delete(Ref, Key).

all(#bitjar{state=#{refmap := R}}, GroupId) ->
	case maps:find(GroupId, R) of
		error -> [];
		{ok, Ref} -> ets:tab2list(Ref)
	end.


filter(B, FilterFuns, GroupId, KdeserialFun, VdeserialFun) ->
	foldl(B, fun(K, V, Acc) ->
					 K2 = KdeserialFun(K),
					 V2 = VdeserialFun(K2, V),
					 case bitjar_helper:run_fundefs(FilterFuns, {K2, V2}) of
					 	 true -> [V|Acc];
					 	 false -> Acc
					 end
			 end, [], GroupId).

foldl(#bitjar{state=#{refmap := R}}, Fun, Start, GroupId) ->
	ets_foldl(maps:find(GroupId, R), Fun, Start).

ets_foldl(error, _, Start) -> Start;
ets_foldl({ok, Ref}, Fun, Start) ->
	lists:reverse(ets:foldl(fun({K,V}, Acc) -> Fun(K, V, Acc) end, Start, Ref)).


