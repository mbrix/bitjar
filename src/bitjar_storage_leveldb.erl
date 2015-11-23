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
		 bitjar_delete/2,
		 bitjar_all/2,
		 bitjar_filter/3,
		 bitjar_foldl/4,
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
	ok = eleveldb:write(Ref, lists:map(fun({Id, K, V}) -> {put, <<Id:8, K/binary>>, V} end, StoreList), []),
	{ok, B}.

bitjar_lookup(#bitjar{}=B, LookupList) -> lookup(B, LookupList, length(LookupList), []).

bitjar_delete(#bitjar{state=#{dbref := Ref}}=B, DeleteList) ->
	ok = eleveldb:write(Ref, lists:map(fun({_GroupName, Id, K}) -> {delete, <<Id:8, K/binary>>} end, DeleteList), []),
	{ok, B}.

bitjar_all(B, GroupId) -> bitjar_foldl(B, fun(K, V, Acc) -> [{K,V}|Acc] end, [], GroupId).

bitjar_filter(B, FilterFuns, GroupId) -> bitjar_foldl(B, fun(K, V, Acc) ->
														 case run_fundefs(FilterFuns, {K,V}) of
														 	 true -> [V|Acc];
														 	 false -> Acc
														 end
														 end, [], GroupId).

bitjar_foldl(#bitjar{state=#{dbref := Ref}}, Fun, Start, GroupId) ->
	try
	    Res = eleveldb:fold(Ref, fun({<<G:8, Key/binary>>, V}, Acc) -> 
	    						   case G of
	    						   	   GroupId -> Fun(Key, V, Acc);
	    						   	   _ -> throw({done, Acc})
								   end;
							  (_,Acc) -> throw({done, Acc})
						   end, Start, [{first_key, <<GroupId:8>>}]),
		lists:reverse(Res)
	catch
		throw:{done, Acc} -> lists:reverse(Acc)
	end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

lookup(_B, [], _L, []) -> not_found;
lookup(_B, [], L, Acc) when L =:= length(Acc) -> {ok, Acc};
lookup(_B, [], _L, Acc) -> {partial, Acc};

lookup(#bitjar{state=#{dbref := Ref}}=B, [{GroupName,
										   GroupId, Key}|T], L, Acc) ->
	case eleveldb:get(Ref, <<GroupId:8, Key/binary>>, []) of
		{ok, Value} -> lookup(B, T, L, [{GroupName, Key, Value}|Acc]);
		_ -> lookup(B, T, L, Acc)
	end.


init_leveldb(Options) ->
	Path = maps:get(path, Options),
	StorageOptions = maps:get(storage_options, Options),
    {ok, Dbref} = eleveldb:open(Path, StorageOptions),
    Dbref.


init_groups(Ref) ->
	try
	    eleveldb:fold(Ref, fun({<<?GROUP_ID:8, Key/binary>>, V}, Acc) ->
	    						   maps:put(erlang:binary_to_term(Key), erlang:binary_to_term(V), Acc);
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
	[Id|_] = lists:sort(fun({_,A}, {_,B}) -> A > B end, maps:to_list(GroupMap)),
	Id.

run_fundefs(Defs, Datum) ->
	try
		run_fundefs_do(Defs, Datum)
	catch _:_ -> false
	end.

run_fundefs_do(F, Datum) when is_function(F) -> F(Datum);

run_fundefs_do([], _) -> false;
run_fundefs_do([D|T], Datum) ->
	case D(Datum) of
		true -> true;
		false -> run_fundefs(T, Datum)
	end.
