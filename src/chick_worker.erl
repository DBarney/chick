-module(chick_worker).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
     terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Id) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [Id], []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([Id]) ->
  ets:insert(chick,{Id,self()}),
  {ok, {Id,dict:new()}}.

handle_call({in,Key,Pid,HighWater,Threshold}, From, {Id,Queues}) ->
  case dict:find(Key,Queues) of
    {ok,{Count,_Waiting,_Running}} when Count > Threshold ->
      {reply,{error,threshold_reached},{Id,Queues}};

    {ok,{Count,Waiting,Running}} when Count > HighWater ->
      Waiting1 = queue:in({From,Pid},Waiting),
      Queues1 = dict:store(Key,{Count + 1,Waiting1,Running},Queues),
      {noreply,{Id,Queues1}};

    {ok,{Count,Waiting,Running}} ->
      Running1 = sets:add_element(Pid,Running),
      Queues1 = dict:store(Key,{Count + 1,Waiting,Running1},Queues),
      {reply,ok,{Id,Queues1}};

    error ->
      Queues1 = dict:store(Key,{1,queue:new(),sets:add_element(Pid,sets:new())},Queues),
      {reply,ok,{Id,Queues1}}
  end;

handle_call({{info,waiting},Key},_From,{Id,Queue}) ->
  case dict:find(Key,Queue) of
    {ok,{_Count,Waiting,_Running}} -> {reply,{ok,queue:len(Waiting)},{Id,Queue}};
    error -> {reply,{ok,0},{Id,Queue}}
  end;

handle_call({{info,running},Key},_From,{Id,Queue}) ->
  case dict:find(Key,Queue) of
    {ok,{_Count,_Waiting,Running}} -> {reply,{ok,sets:size(Running)},{Id,Queue}};
    error -> {reply,{ok,0},{Id,Queue}}
  end;

handle_call({{info,total},Key},_From,{Id,Queue}) ->
  case dict:find(Key,Queue) of
    {ok,{Count,_Waiting,_Running}} -> {reply,{ok,Count},{Id,Queue}};
    error -> {reply,{ok,0},{Id,Queue}}
  end;

handle_call(_Cmd, _From, State) ->
  {noreply, State}.

handle_cast({out,Key,Pid}, {Id,Queues}) ->
  case dict:find(Key,Queues) of      
    {ok,{1,_Waiting,_Running}} ->
      Queues1 = dict:erase(Key,Queues),
      {noreply,{Id,Queues1}};
    {ok,{Count,Waiting,Running}} ->
      Running1 = sets:del_element(Pid,Running),

      case queue:out(Waiting) of
        {{value,{From,NewPid}},Waiting1} ->
          Running2 = sets:add_element(NewPid,Running1),
          gen_server:reply(From,ok),
          Queues1 = dict:store(Key,{Count - 1,Waiting1,Running2},Queues),
          {noreply,{Id,Queues1}};
        {empty,Waiting} -> 
          Queues1 = dict:store(Key,{Count - 1,Waiting,Running1},Queues),
          {noreply,{Id,Queues1}}
      end;
    error ->
      {noreply,{Id,Queues}}
  end;

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, {Id,_Queues}) ->
  ets:delete(chick,Id),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

chick_in_out_test() ->
    State1 = {1,dict:new()},
    {reply,ok,State2} = handle_call({in,<<"hello">>,pid1,2,3},{self(),make_ref()},State1),
    ?assertEqual({reply,{ok,1},State2},handle_call({{info,running},<<"hello">>},'_',State2)),
    ?assertEqual({reply,{ok,0},State2},handle_call({{info,waiting},<<"hello">>},'_',State2)),
    ?assertEqual({reply,{ok,1},State2},handle_call({{info,total},<<"hello">>},'_',State2)),

    {reply,ok,State3} = handle_call({in,<<"hello1">>,pid2,2,3},{self(),make_ref()},State2),
    ?assertEqual({reply,{ok,1},State3},handle_call({{info,running},<<"hello1">>},'_',State3)),
    ?assertEqual({reply,{ok,0},State3},handle_call({{info,waiting},<<"hello1">>},'_',State3)),
    ?assertEqual({reply,{ok,1},State3},handle_call({{info,total},<<"hello1">>},'_',State3)),

    {reply,ok,State4} = handle_call({in,<<"hello">>,pid3,2,3},{self(),make_ref()},State3),
    ?assertEqual({reply,{ok,2},State4},handle_call({{info,running},<<"hello">>},'_',State4)),
    ?assertEqual({reply,{ok,0},State4},handle_call({{info,waiting},<<"hello">>},'_',State4)),
    ?assertEqual({reply,{ok,2},State4},handle_call({{info,total},<<"hello">>},'_',State4)),

    {reply,ok,State5} = handle_call({in,<<"hello">>,pid4,2,3},{self(),make_ref()},State4),
    ?assertEqual({reply,{ok,3},State5},handle_call({{info,running},<<"hello">>},'_',State5)),
    ?assertEqual({reply,{ok,0},State5},handle_call({{info,waiting},<<"hello">>},'_',State5)),
    ?assertEqual({reply,{ok,3},State5},handle_call({{info,total},<<"hello">>},'_',State5)),

    {noreply,State7} = handle_call({in,<<"hello">>,pid5,2,3},{self(),make_ref()},State5),
    ?assertEqual({reply,{ok,3},State7},handle_call({{info,running},<<"hello">>},'_',State7)),
    ?assertEqual({reply,{ok,1},State7},handle_call({{info,waiting},<<"hello">>},'_',State7)),
    ?assertEqual({reply,{ok,4},State7},handle_call({{info,total},<<"hello">>},'_',State7)),

    {reply,{error,threshold_reached},State8} = handle_call({in,<<"hello">>,self(),2,3},{self(),make_ref()},State7),
    ?assertEqual({reply,{ok,3},State8},handle_call({{info,running},<<"hello">>},'_',State8)),
    ?assertEqual({reply,{ok,1},State8},handle_call({{info,waiting},<<"hello">>},'_',State8)),
    ?assertEqual({reply,{ok,4},State8},handle_call({{info,total},<<"hello">>},'_',State8)),

    receive
      Msg -> throw(Msg)
    after 
      1 -> ok
    end,


    {noreply,State9} = handle_cast({out,<<"hello">>,pid1},State8),
    ?assertEqual({reply,{ok,3},State9},handle_call({{info,running},<<"hello">>},'_',State9)),
    ?assertEqual({reply,{ok,0},State9},handle_call({{info,waiting},<<"hello">>},'_',State9)),
    ?assertEqual({reply,{ok,3},State9},handle_call({{info,total},<<"hello">>},'_',State9)),

    receive
      _Msg -> ok
    after 
      10 -> throw(no_message)
    end,

    {noreply,State10} = handle_cast({out,<<"hello">>,pid3},State9),
    ?assertEqual({reply,{ok,2},State10},handle_call({{info,running},<<"hello">>},'_',State10)),
    ?assertEqual({reply,{ok,0},State10},handle_call({{info,waiting},<<"hello">>},'_',State10)),
    ?assertEqual({reply,{ok,2},State10},handle_call({{info,total},<<"hello">>},'_',State10)),

    {noreply,State11} = handle_cast({out,<<"hello">>,pid4},State10),
    ?assertEqual({reply,{ok,1},State11},handle_call({{info,running},<<"hello">>},'_',State11)),
    ?assertEqual({reply,{ok,0},State11},handle_call({{info,waiting},<<"hello">>},'_',State11)),
    ?assertEqual({reply,{ok,1},State11},handle_call({{info,total},<<"hello">>},'_',State11)),

    {noreply,State12} = handle_cast({out,<<"hello">>,pid5},State11),
    ?assertEqual({reply,{ok,0},State12},handle_call({{info,running},<<"hello">>},'_',State12)),
    ?assertEqual({reply,{ok,0},State12},handle_call({{info,waiting},<<"hello">>},'_',State12)),
    ?assertEqual({reply,{ok,0},State12},handle_call({{info,total},<<"hello">>},'_',State12)),


    % ?assertEqual({ok,"BCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/A"},to_base64(all_chars(0,0),[])).
    ok.

-endif.