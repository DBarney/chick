-module(chick_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link()->
	start_link(10).
start_link(Amount) ->
    Ret = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    [supervisor:start_child(?MODULE,[Id]) || Id <- lists:seq(0,Amount)],
    Ret.

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, { {simple_one_for_one, 5, 10}, [

      {chick_worker, {chick_worker, start_link, []}, permanent, 2000, worker, [chick_worker]}
    ]}}.
