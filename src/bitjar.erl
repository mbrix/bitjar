%% (c) 2015, Matthew Branton
%% BSD 2-clause, check /LICENSE

%% Generic storage interface
%% Fronts bucket-based storage

-module(bitjar).

%% API functions
-export([open/1,
		 open/2,
		 set_serializer/6,
		 set_key_serializer/3,
		 set_key_deserializer/3,
		 set_value_serializer/3,
		 set_value_deserializer/3,
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

-define(GROUP_ID, 1).

behaviour_info(callbacks)->
    [{bitjar_init, 1},
	 {bitjar_shutdown, 1},
	 {bitjar_store, 2},
     {bitjar_lookup, 2},
     {bitjar_delete, 2},
	 {bitjar_all, 2},
	 {bitjar_filter, 5},
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
		{ok, GDef} ->  
			lists:map(fun({K,V}) ->
							  DK = run_fun(GDef#bitjar_groupdef.kdeserializer, K),
							  {DK,
							   run_fun(GDef#bitjar_groupdef.vdeserializer, DK, V)} end,
							  M:bitjar_all(B, GDef#bitjar_groupdef.groupid))
	end.

filter(B, Fun, Group) when is_function(Fun) -> filter(B, [Fun], Group);

filter(#bitjar{mod=M, groups=G}=B, FilterFuns, Group) ->
	case maps:find(Group, G) of
		error -> [];
		{ok, GDef} -> M:bitjar_filter(B, FilterFuns,
									  GDef#bitjar_groupdef.groupid,
									  GDef#bitjar_groupdef.kdeserializer,
									  GDef#bitjar_groupdef.vdeserializer
									  )
	end.

foldl(#bitjar{mod=M, groups=G}=B, Fun, StartAcc, Group) ->
	case maps:find(Group, G) of
		error -> StartAcc;
		{ok, GDef} -> M:bitjar_foldl(B, Fun, StartAcc, GDef#bitjar_groupdef.groupid)
	end.

%% Inline serialization code
%% attached to a specific bucket

set_serializer(B,
			   Group,
			   KeySerializerFun,
			   ValueSerializerFun,
			   KeyDeserializerFun,
			   ValueDeserializerFun) ->
	{ok, B2} = set_key_serializer(B, Group, KeySerializerFun),
	{ok, B3} = set_value_serializer(B2, Group, ValueSerializerFun),
	{ok, B4} = set_key_deserializer(B3, Group,KeyDeserializerFun),
	set_value_deserializer(B4, Group, ValueDeserializerFun).

set_key_serializer(#bitjar{groups=G}=B, Group, Fun) ->
	{ok, B2, GDef} = add_group(B, Group),
	{ok, B2#bitjar{groups = maps:put(Group, GDef#bitjar_groupdef{kserializer = Fun}, G)}}.

set_key_deserializer(#bitjar{groups=G}=B, Group, Fun) ->
	{ok, B2, GDef} = add_group(B, Group),
	{ok, B2#bitjar{groups = maps:put(Group, GDef#bitjar_groupdef{kdeserializer = Fun}, G)}}.

set_value_serializer(#bitjar{groups=G}=B, Group, Fun) ->
	{ok, B2, GDef} = add_group(B, Group),
	{ok, B2#bitjar{groups = maps:put(Group, GDef#bitjar_groupdef{vserializer = Fun}, G)}}.

set_value_deserializer(#bitjar{groups=G}=B, Group, Fun) ->
	{ok, B2, GDef} = add_group(B, Group),
	{ok, B2#bitjar{groups = maps:put(Group, GDef#bitjar_groupdef{vdeserializer = Fun}, G)}}.

%% End Serialization

%% Private functions

do_store(B, []) -> {ok, B};
do_store(#bitjar{mod=M}=B, GKVList) -> M:bitjar_store(B, GKVList).

do_lookup(_, []) -> not_found;
do_lookup(#bitjar{mod=M}=B, GKList) ->
	deserialize(B, M:bitjar_lookup(B, GKList)).

do_delete(B, []) -> {ok, B};
do_delete(#bitjar{mod=M}=B, GKList) -> M:bitjar_delete(B, GKList).

resolve_gkvlist(B, L) -> resolve_gkvlist(B, L, []).
resolve_gkvlist(B, [], Acc) -> {ok, B, Acc};
resolve_gkvlist(B, [{G, K, V}|T], Acc) ->
	{ok, B2, GDef} = add_group(B, G),
	resolve_gkvlist(B2, T,
					[{GDef#bitjar_groupdef.groupid,
					  run_fun(GDef#bitjar_groupdef.kserializer, K),
					  run_fun(GDef#bitjar_groupdef.vserializer, V)}|Acc]).

resolve_gklist(B, L) -> resolve_gklist(B, L, []).
resolve_gklist(B, [], Acc) -> {ok, B, Acc};
resolve_gklist(B, [{G, K}|T], Acc) ->
	{ok, B2, GDef} = add_group(B, G),
	resolve_gklist(B2, T,
				   [{G,
				   	 GDef#bitjar_groupdef.groupid,
				   	 run_fun(GDef#bitjar_groupdef.kserializer, K)}|Acc]).

resolve_mod(leveldb) -> {mod, bitjar_storage_leveldb};
resolve_mod(ets) -> {mod, bitjar_storage_ets};
resolve_mod(X) -> {mod, X}.

run_fun(Fun, K) -> Fun(K).
run_fun(Fun, K, V) -> Fun(K, V).

%% XXX deserialization is not optimized

deserialize(#bitjar{}, not_found) -> not_found;
deserialize(#bitjar{groups=G}, {ResCode, Results}) ->
	{ResCode, deserialize(G, Results, [])}.

deserialize(_G, [], Acc) -> lists:reverse(Acc);
deserialize(G, [{Name, K, V}|T], Acc) ->
	deserialize(G, T,
				[deserialize_run(Name, maps:find(Name, G), K, V)|Acc]).

deserialize_run(_Name, error, K, V) -> {K, V};
deserialize_run(Name, {ok, GDef}, K, V) ->
	DK = run_fun(GDef#bitjar_groupdef.kdeserializer, K),
	{Name,
	 DK,
	 run_fun(GDef#bitjar_groupdef.vdeserializer, DK, V)}.


add_group(#bitjar{mod=M, groups = G}=B, Identifier) ->
	case maps:find(Identifier, G) of
		{ok, Val} -> {ok, B, Val};
		error -> 
			NextId = next_id(B),
			{ok, B2} = M:bitjar_store(B, [{?GROUP_ID,
										 erlang:term_to_binary(Identifier),
										 erlang:term_to_binary(NextId)}]),
			Group = #bitjar_groupdef{groupid = NextId,
									 kserializer = fun null_fun/1,
									 vserializer = fun null_fun/1,
									 kdeserializer = fun null_fun/1,
									 vdeserializer = fun null_fun/2},
			{ok, B2#bitjar{last_id = NextId,
						   groups = maps:put(Identifier, Group, G)},
			 Group}
	end.

next_id(#bitjar{last_id = Id}) -> Id+1.

null_fun(X) -> X.
null_fun(_,Y) -> Y.
