%% (c) 2015, Matthew Branton
%% BSD 2-clause, check /LICENSE
%% Generic storage interface

-module(bitjar_storage_leveldb).


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
		 bitjar_foldl/6,
		 bitjar_new_group/2,
		 bitjar_default_options/0]).

-define(GROUP_ID, 1).

%%%===================================================================
%%% Bitjar behaviour
%%%===================================================================

bitjar_init(Options) ->
	Ref = init_leveldb(maps:merge(bitjar_default_options(), Options)),
	Groups = init_groups(Ref),
	LastGroupId = last_id(Groups),
    {ok, #{dbref => Ref}, LastGroupId, Groups}.

bitjar_shutdown(#bitjar{state = #{dbref := D}}) ->
	ok = eleveldb:close(D),
	ok.

bitjar_default_options() ->
	#{path => random_path(),
	  storage_options => [{create_if_missing, true}]}.

bitjar_store(#bitjar{state=#{dbref := Ref}}=B, StoreList) ->
	ok = eleveldb:write(Ref, map_store(StoreList), []),
	{ok, B}.

bitjar_lookup(#bitjar{}=B, LookupList) -> lookup(B, LookupList, length(LookupList), [], []).

bitjar_range(#bitjar{}=B, RangeList) -> range(B, RangeList, [], []).

bitjar_delete(#bitjar{state=#{dbref := Ref}}=B, DeleteList) ->
	ok = eleveldb:write(Ref, map_delete(DeleteList), []),
	{ok, B}.

map_delete(DeleteList) -> lists:map(fun({_GroupName, Id, K}) -> {delete, <<Id:8, K/binary>>} end, DeleteList).
map_store(StoreList) -> lists:map(fun({Id, K, V}) -> {put, <<Id:8, K/binary>>, V} end, StoreList).


bitjar_delete_then_store(#bitjar{state=#{dbref := Ref}}=B, DeleteList, StoreList) ->
	ok = eleveldb:write(Ref, lists:flatten([map_delete(DeleteList)|map_store(StoreList)]), []),
	{ok, B}.

bitjar_all(B, GroupId) ->
	lists:reverse(bitjar_foldl(B, fun(K, V, Acc) -> [{K,V}|Acc] end, [], GroupId, fun null_fun/1, fun null_fun/2 )).

bitjar_filter(B, FilterFuns, GroupId, KdeserialFun, VdeserialFun) ->
	lists:reverse(bitjar_foldl(B, fun(K, V, Acc) ->
							%% Lets decode the K, V values so that the fundefs can run on the deserialized form
							case bitjar_helper:run_fundefs(FilterFuns, {K, V}) of
								true -> [V|Acc];
								false -> Acc
							end
					end, [], GroupId, KdeserialFun, VdeserialFun)).

bitjar_foldl(#bitjar{state=#{dbref := Ref}}, Fun, Start, GroupId, KdeserialFun, VdeserialFun) ->
	try
	    Res = eleveldb:fold(Ref, fun({<<G:8, Key/binary>>, V}, Acc) -> 
	    						   case G of
	    						   	   GroupId -> 
	    						   	   	   K2 = KdeserialFun(Key),
	    						   	   	   V2 = VdeserialFun(K2, V),
	    						   	   	   Fun(K2, V2, Acc);
	    						   	   _ -> throw({done, Acc})
								   end;
							  (_,Acc) -> throw({done, Acc})
						   end, Start, [{first_key, <<GroupId:8>>}]),
		Res
	catch
		throw:{done, Acc} -> Acc
	end.

bitjar_new_group(Bitjar, _Id) -> {ok, Bitjar}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

lookup(_B, [], _L, [], _) -> not_found;
lookup(_B, [], _L, Acc, []) -> {ok, Acc};
lookup(_B, [], _L, Acc, LeftOver) -> {partial, Acc, LeftOver};

lookup(#bitjar{state=#{dbref := Ref}}=B, [{GroupName,
										   GroupId, Key}|T], L, Acc, LeftOver) ->
	case eleveldb:get(Ref, <<GroupId:8, Key/binary>>, []) of
		{ok, Value} -> lookup(B, T, L, [{GroupName, Key, Value}|Acc], LeftOver);
		_ -> lookup(B, T, L, Acc, [{GroupName, Key}|LeftOver])
	end.

range(_B, [], [], _) -> not_found;
range(_B, [], Acc, []) -> {ok, Acc};
range(_B, [], Acc, LeftOver) -> {partial, Acc, LeftOver};

range(#bitjar{state=#{dbref := Ref}}=B, [{GroupName, GroupId, Key}|T], Start, LeftOver) ->
	Results = 
		try
	    Res = eleveldb:fold(Ref, fun({<<G:8, RawKey/binary>>, V}, Acc) -> 
	    						   case G of
	    						   	   GroupId -> 
										   case binary:match(RawKey, Key) of
											   {0,_} -> [{GroupName, RawKey, V}|Acc]; 
										   	   _ -> throw({done, Acc})
									   end;
	    						   	   _ -> throw({done, Acc})
								   end;
							  (_,Acc) -> throw({done, Acc})
						   end, Start, [{first_key, <<GroupId:8, Key/binary>>}]),
		lists:reverse(Res)
	catch
		throw:{done, Acc} -> lists:reverse(Acc)
	end,
		if length(Results) =/= length(Start) ->
			   range(B, T, Results, LeftOver);
		   true ->
		   	   %% Nothing was added to our results
		   	   range(B, T, Results, [{GroupName, Key}|LeftOver])
		end.


init_leveldb(Options) ->
	Path = maps:get(path, Options),
	StorageOptions = maps:get(storage_options, Options),
    {ok, Dbref} = eleveldb:open(Path, StorageOptions),
    Dbref.


init_groups(Ref) ->
	try
	    eleveldb:fold(Ref, fun({<<?GROUP_ID:8, Key/binary>>, V}, Acc) ->
	    						   maps:put(erlang:binary_to_term(Key), 
											#bitjar_groupdef{groupid = erlang:binary_to_term(V)}, Acc);
							  (_,Acc) -> throw({done, Acc})
						   end, #{}, [{first_key, <<?GROUP_ID:8>>}]),
	    throw(missing)
	catch
		throw:missing -> #{};
		throw:{done, Acc} -> Acc
	end.

random_path() ->
 lists:flatten([io_lib:format("~2.16.0b", [X]) || X <- binary_to_list(crypto:rand_bytes(32))]).

last_id(#{}) -> 1;  %% Reserved for group lists
last_id(GroupMap) ->
	[Id|_] = lists:sort(fun({_,A}, {_,B}) -> A#bitjar_groupdef.groupid > B#bitjar_groupdef.groupid end, maps:to_list(GroupMap)),
	Id.


null_fun(X) -> X.
null_fun(_, X) -> X.
