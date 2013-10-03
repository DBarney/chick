-module(chick).

-export([in/3,out/1]).

in(Key,HighWater,Threshold) ->
	gen_server:call(get_process(Key),{in,Key,self(),HighWater,Threshold}).
	
out(Key)->
	gen_server:cast(get_process(Key),{out,Key,self()}),
	ok.

get_process(Key) ->
	Size = ets:info(chick,size),
	Id = erlang:phash2(Key,Size),
	[{Id,Process}] = ets:lookup(chick,Id),
	Process.




-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

no_crash_test() ->
	ok = application:start(chick),
	Me = self(),
	[spawn(fun()->
		[begin
			Ids = lists:seq(1,10),
			Res = [case chick:in(Id,1,20) of ok -> Id; _ -> ignore end || Id <- Ids],
			NewIds = lists:filter(fun
				(ignore)-> false;
				(_)-> true
			end,Res),
			[chick:out(Id) || Id <- NewIds]
		end || _ <- lists:seq(1,100)],
		Me ! done
	end) || _ <- lists:seq(1,400)],
	wait(400),
	application:stop(chick),
	ok.

wait(0) -> ok;
wait(Count) ->
	receive
		done -> wait(Count - 1)
	after
		20000 -> throw(timed_out)
	end.

-endif.