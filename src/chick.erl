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