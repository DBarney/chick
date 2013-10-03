-module(chick).

-export([in/3,out/1]).

in(Key,HighWater,Threshold) ->
	gen_server:cast(get_process(Key),{in,Key,self(),HighWater,Threshold}).
	
out(Key)->
	gen_server:cast(get_process(Key),{out,Key,self()}),
	ok.

get_process(Key) ->
	Size = ets:info(chick,size),
	Id = erlang:phash2(Key,Size),
	[{Id,Process}] = ets:lookup(chick,Id),
	Process.
