%% (c) 2015, Matthew Branton
%% BSD 2-clause, check /LICENSE

%% Generic storage interface
%% Fronts bucket-based storage

-module(bitjar).

%% API functions
-export([open/1,
		 open/2,
		 set_serializer/4,
		 set_deserializer/3,
		 close/1,
		 groups/1,
		 store/2,
		 store/4,
		 lookup/2,
		 lookup/3,
		 delete/2,
		 delete/3,
		 all/2,
		 filter/3,
		 foldl/4,
		 behaviour_info/1]).

-behaviour(application).
-export([start/0, start/2, stop/0, stop/1]).

-behaviour(supervisor).
-export([init/1]).

-include_lib("bitjar.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

behaviour_info(callbacks)->
    [{bitjar_init, 1},
	 {bitjar_shutdown, 1},
	 {bitjar_store, 2},
     {bitjar_lookup, 2},
     {bitjar_delete, 2},
	 {bitjar_add_group, 2},
	 {bitjar_all, 2},
	 {bitjar_filter, 3},
	 {bitjar_foldl, 4},
	 {bitjar_default_options, 0}];


%% Gen Behaviours
behaviour_info(_Other)->
    undefined.

start() ->
	application:ensure_all_started(lib_bitter).

start(_, _) ->
 supervisor:start_link({local,?MODULE},?MODULE,[]).

stop() -> application:stop(lib_bitter).

stop(_) -> ok.

init([]) ->
 {ok, {{one_for_one,3,10},[]}}.

%% Bitjar public interface

open({mod, Mod}) -> open({mod, Mod}, Mod:bitjar_default_options());
open(Type) -> open(resolve_mod(Type)).

open({mod, Mod}, Options) ->
	Ref = make_ref(),
	case Mod:bitjar_init(Options) of
		{ok, S, LastGroupId, Groups} ->
			{ok, #bitjar{ref = Ref,
						 mod = Mod,
						 last_id = LastGroupId,
						 groups=Groups,
						 state = S}};
		Other -> Other
	end;
open(Type, Options) -> open(resolve_mod(Type), Options).

close(#bitjar{mod=M}=B) -> M:bitjar_shutdown(B).

groups(#bitjar{groups=G}) -> G.

store(B, Group, Key, Value) -> store(B, [{Group, Key, Value}]).
store(B, GKVList) -> 
	{ok, B2, TransformedList} = resolve_gkvlist(B, GKVList),
	do_store(B2, TransformedList).

lookup(B, Group, Key) -> lookup(B, [{Group, Key}]).
lookup(B, GKList) ->
	{ok, B2, TransformedList} = resolve_gklist(B, GKList),
	do_lookup(B2, TransformedList).
	

delete(B, Group, Key) -> delete(B, [{Group, Key}]).
delete(B, GKList) ->
	{ok, B2, TransformedList} = resolve_gklist(B, GKList),
	do_delete(B2, TransformedList).

all(#bitjar{mod=M, groups=G}=B, Group) ->
	case maps:find(Group, G) of
		error -> [];
		{ok, GroupId} ->  M:bitjar_all(B, GroupId)
	end.

filter(B, Fun, Group) when is_function(Fun) -> filter(B, [Fun], Group);

filter(#bitjar{mod=M, groups=G}=B, FilterFuns, Group) ->
	case maps:find(Group, G) of
		error -> [];
		{ok, GroupId} -> M:bitjar_filter(B, FilterFuns, GroupId)
	end.

foldl(#bitjar{mod=M, groups=G}=B, Fun, StartAcc, Group) ->
	case maps:find(Group, G) of
		error -> StartAcc;
		{ok, GroupId} -> M:bitjar_foldl(B, Fun, StartAcc, GroupId)
	end.


set_serializer(B, Group, KeySerializerFun, ValueSerializerFun) -> ok.

set_deserializer(B, Group, KeyDeserializerFun, ValueDeserializerFun) -> ok.

%% Private functions

do_store(B, []) -> {ok, B};
do_store(#bitjar{mod=M}=B, GKVList) -> M:bitjar_store(B, GKVList).

do_lookup(_, []) -> not_found;
do_lookup(#bitjar{mod=M}=B, GKList) -> M:bitjar_lookup(B, GKList).

do_delete(B, []) -> {ok, B};
do_delete(#bitjar{mod=M}=B, GKList) -> M:bitjar_delete(B, GKList).

resolve_gkvlist(B, L) -> resolve_gkvlist(B, L, []).
resolve_gkvlist(B, [], Acc) -> {ok, B, Acc};
resolve_gkvlist(#bitjar{mod=M}=B, [{G, K, V}|T], Acc) ->
	{ok, B2, GroupId} = M:bitjar_add_group(B, G),
	resolve_gkvlist(B2, T, [{GroupId, K, V}|Acc]).

resolve_gklist(B, L) -> resolve_gklist(B, L, []).
resolve_gklist(B, [], Acc) -> {ok, B, Acc};
resolve_gklist(#bitjar{mod=M}=B, [{G, K}|T], Acc) ->
	{ok, B2, GroupId} = M:bitjar_add_group(B, G),
	resolve_gklist(B2, T, [{GroupId, K}|Acc]).

resolve_mod(leveldb) -> {mod, bitjar_storage_leveldb};
resolve_mod(X) -> {mod, X}.
