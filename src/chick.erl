-module(chick).

-export([in/1,out/3]).

out(Key,HighWater,Threshold) when HighWater < Threshold ->
	gen_server:call(get_process(Key),{out,Key,self(),HighWater,Threshold},infinity).

in(Key)->
	gen_server:cast(get_process(Key),{in,Key,self()}),
	ok.

get_process(Key) ->
	Size = ets:info(chick,size),
	Id = erlang:phash2(Key,Size),
	[{Id,Process}] = ets:lookup(chick,Id),
	Process.




-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% yeah this isn't going to chatch much.
no_crash_test() ->
	ok = application:start(chick),
	Me = self(),
	[spawn(fun()->
		[begin
			Ids = lists:seq(1,10),
			Res = [case chick:out(Id,1,20) of ok -> Id; _ -> ignore end || Id <- Ids],
			NewIds = lists:filter(fun
				(ignore)-> false;
				(_)-> true
			end,Res),
			[chick:in(Id) || Id <- NewIds]
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