-module(chick_app).

-behaviour(application).

%% Application callbacks
-export([start/0, start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start() -> 
    application:start(chick_app).

start(_StartType, _StartArgs) ->
	ets:new(chick,[{read_concurrency,true},named_table,public,set]),
	WorkerCount = application:get_env(chick,worker_count,10),
    chick_sup:start_link(WorkerCount).

stop(_State) ->
	ets:delete(chick),
    ok.
