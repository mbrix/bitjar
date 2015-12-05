-module(bitjar_helper).
-author('mbranton@emberfinancial.com').

-export([run_fundefs/2]).

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
		false -> run_fundefs_do(T, Datum)
	end.
