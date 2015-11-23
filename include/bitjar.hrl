%% (c) 2015, Matthew Branton
%% BSD 2-clause, check /LICENSE

-author('mbranton@emberfinancial.com').

-record(bitjar, {ref, mod, last_id, groups, serializers, state}). 
%% ref = internal reference
%% mod = module nmae
%% groups = group mapping
%% state = internal storage state data
%%

-record(bitjar_groupdef), {groupid, serializer=undefined, deserializer=undefined}).
