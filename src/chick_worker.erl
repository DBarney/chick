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

-record(state,{
  id,
  queues,
  maps
}).

-ifdef(TEST).
-define(RET(Cmd,From,State),int_handle_call(Cmd,From,State)).
-else.
-define(RET(Cmd,From,State),{noreply, State}).
-endif.
%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([Id]) ->
  ets:insert(chick,{Id,self()}),
  {ok, {Id,dict:new()}}.

handle_call({out,Key,Pid,HighWater,Threshold}, From, State = #state{queues= Queues,maps= Maps}) ->
  case dict:find(Pid,Maps) of
    {ok,_Key}->
      {reply,{error,already_enqueued},State};
    error ->
      case dict:find(Key,Queues) of
        {ok,{Count,_Waiting,_Running}} when Count > Threshold ->
          {reply,{error,threshold_reached},State#state{queues= Queues}};

        {ok,{Count,Waiting,Running}} when Count > HighWater ->
          Ref = monitor(process,Pid),
          Maps1 = dict:store(Pid,{Key,Ref},Maps),
          Waiting1 = queue:in({From,Pid},Waiting),
          Queues1 = dict:store(Key,{Count + 1,Waiting1,Running},Queues),
          {noreply,State#state{queues= Queues1,maps= Maps1}};

        {ok,{Count,Waiting,Running}} ->
          Ref = monitor(process,Pid),
          Maps1 = dict:store(Pid,{Key,Ref},Maps),
          Running1 = sets:add_element(Pid,Running),
          Queues1 = dict:store(Key,{Count + 1,Waiting,Running1},Queues),
          {reply,ok,State#state{queues= Queues1,maps= Maps1}};

        error ->
          Ref = monitor(process,Pid),
          Maps1 = dict:store(Pid,{Key,Ref},Maps),
          Queues1 = dict:store(Key,{1,queue:new(),sets:add_element(Pid,sets:new())},Queues),
          {reply,ok,State#state{queues= Queues1,maps= Maps1}}
      end
  end;

handle_call(_Cmd, _From, State) ->
  ?RET(_Cmd, _From, State).

handle_cast({in,Key,Pid}, State = #state{queues= Queues,maps= Maps}) ->
  case dict:find(Pid,Maps) of
    {ok,{Key,Ref}} ->
      demonitor(Ref),
      Maps1 = dict:erase(Pid,Maps),
      case dict:find(Key,Queues) of 
        {ok,{1,_Waiting,_Running}} ->
          Queues1 = dict:erase(Key,Queues),
          {noreply,State#state{queues= Queues1,maps= Maps1}};
        {ok,{Count,Waiting,Running}} ->
          Running1 = sets:del_element(Pid,Running),

          case queue:out(Waiting) of
            {{value,{From,NewPid}},Waiting1} ->
              Running2 = sets:add_element(NewPid,Running1),
              gen_server:reply(From,ok),
              Queues1 = dict:store(Key,{Count - 1,Waiting1,Running2},Queues),
              {noreply,State#state{queues= Queues1,maps= Maps1}};
            {empty,Waiting} -> 
              Queues1 = dict:store(Key,{Count - 1,Waiting,Running1},Queues),
              {noreply,State#state{queues= Queues1,maps= Maps1}}
          end
      end;
    {ok,{Key1,_}}->
      {reply,{error,{wrong_key,Key1}},State};
    error ->
      {reply,{error,not_enqueued},State}
  end;

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info({'DOWN', _MonitorRef, process, Pid, _Info}, State = #state{maps= Maps}) ->
  case dict:find(Pid,Maps) of
    {ok,{Key,_Ref}} -> handle_cast({in,Key,Pid},State);
    error -> {noreply,State}
  end;
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



int_handle_call({{info,waiting},Key},_From,State = #state{queues= Queues}) ->
  case dict:find(Key,Queues) of
    {ok,{_Count,Waiting,_Running}} -> {reply,{ok,queue:len(Waiting)},State};
    error -> {reply,{ok,0},State}
  end;

int_handle_call({{info,running},Key},_From,State = #state{queues= Queues}) ->
  case dict:find(Key,Queues) of
    {ok,{_Count,_Waiting,Running}} -> {reply,{ok,sets:size(Running)},State};
    error -> {reply,{ok,0},State}
  end;

int_handle_call({info,refs},_From,State = #state{maps= Maps}) ->
  {reply,{ok,dict:size(Maps)},State};

int_handle_call({{info,total},Key},_From,State = #state{queues= Queues}) ->
  case dict:find(Key,Queues) of
    {ok,{Count,_Waiting,_Running}} -> {reply,{ok,Count},State};
    error -> {reply,{ok,0},State}
  end.

eat() ->
  receive
    {'DOWN',_,process,{_pid,nonode@nohost},noproc} -> eat();
    Msg -> throw(Msg)
  after
    1 -> ok
  end.

no_eat() ->
  receive
    {'DOWN',_,process,{_pid,nonode@nohost},noproc} -> no_eat();
    _Msg -> ok
  after 
    10 -> throw(no_message)
  end.

chick_in_out_test() ->
    State1 = #state{id= 1,queues=dict:new(),maps=dict:new()},
    {reply,ok,State2} = handle_call({out,<<"hello">>,pid1,2,3},{self(),make_ref()},State1),
    ?assertEqual({reply,{ok,1},State2},handle_call({{info,running},<<"hello">>},'_',State2)),
    ?assertEqual({reply,{ok,0},State2},handle_call({{info,waiting},<<"hello">>},'_',State2)),
    ?assertEqual({reply,{ok,1},State2},handle_call({{info,total},<<"hello">>},'_',State2)),
    ?assertEqual({reply,{ok,1},State2},handle_call({info,refs},'_',State2)),

    {reply,ok,State3} = handle_call({out,<<"hello1">>,pid2,2,3},{self(),make_ref()},State2),
    ?assertEqual({reply,{ok,1},State3},handle_call({{info,running},<<"hello1">>},'_',State3)),
    ?assertEqual({reply,{ok,0},State3},handle_call({{info,waiting},<<"hello1">>},'_',State3)),
    ?assertEqual({reply,{ok,1},State3},handle_call({{info,total},<<"hello1">>},'_',State3)),
    ?assertEqual({reply,{ok,2},State3},handle_call({info,refs},'_',State3)),

    {reply,ok,State4} = handle_call({out,<<"hello">>,pid3,2,3},{self(),make_ref()},State3),
    ?assertEqual({reply,{ok,2},State4},handle_call({{info,running},<<"hello">>},'_',State4)),
    ?assertEqual({reply,{ok,0},State4},handle_call({{info,waiting},<<"hello">>},'_',State4)),
    ?assertEqual({reply,{ok,2},State4},handle_call({{info,total},<<"hello">>},'_',State4)),
    ?assertEqual({reply,{ok,3},State4},handle_call({info,refs},'_',State4)),

    {reply,ok,State5} = handle_call({out,<<"hello">>,pid4,2,3},{self(),make_ref()},State4),
    ?assertEqual({reply,{ok,3},State5},handle_call({{info,running},<<"hello">>},'_',State5)),
    ?assertEqual({reply,{ok,0},State5},handle_call({{info,waiting},<<"hello">>},'_',State5)),
    ?assertEqual({reply,{ok,3},State5},handle_call({{info,total},<<"hello">>},'_',State5)),
    ?assertEqual({reply,{ok,4},State5},handle_call({info,refs},'_',State5)),

    {noreply,State7} = handle_call({out,<<"hello">>,pid5,2,3},{self(),make_ref()},State5),
    ?assertEqual({reply,{ok,3},State7},handle_call({{info,running},<<"hello">>},'_',State7)),
    ?assertEqual({reply,{ok,1},State7},handle_call({{info,waiting},<<"hello">>},'_',State7)),
    ?assertEqual({reply,{ok,4},State7},handle_call({{info,total},<<"hello">>},'_',State7)),
    ?assertEqual({reply,{ok,5},State7},handle_call({info,refs},'_',State7)),

    {reply,{error,threshold_reached},State8} = handle_call({out,<<"hello">>,self(),2,3},{self(),make_ref()},State7),
    ?assertEqual({reply,{ok,3},State8},handle_call({{info,running},<<"hello">>},'_',State8)),
    ?assertEqual({reply,{ok,1},State8},handle_call({{info,waiting},<<"hello">>},'_',State8)),
    ?assertEqual({reply,{ok,4},State8},handle_call({{info,total},<<"hello">>},'_',State8)),
    ?assertEqual({reply,{ok,5},State8},handle_call({info,refs},'_',State8)),

    eat(),

    {noreply,State9} = handle_cast({in,<<"hello">>,pid1},State8),
    ?assertEqual({reply,{ok,3},State9},handle_call({{info,running},<<"hello">>},'_',State9)),
    ?assertEqual({reply,{ok,0},State9},handle_call({{info,waiting},<<"hello">>},'_',State9)),
    ?assertEqual({reply,{ok,3},State9},handle_call({{info,total},<<"hello">>},'_',State9)),
    ?assertEqual({reply,{ok,4},State9},handle_call({info,refs},'_',State9)),

    no_eat(),
      

    {noreply,State10} = handle_cast({in,<<"hello">>,pid3},State9),
    ?assertEqual({reply,{ok,2},State10},handle_call({{info,running},<<"hello">>},'_',State10)),
    ?assertEqual({reply,{ok,0},State10},handle_call({{info,waiting},<<"hello">>},'_',State10)),
    ?assertEqual({reply,{ok,2},State10},handle_call({{info,total},<<"hello">>},'_',State10)),
    ?assertEqual({reply,{ok,3},State10},handle_call({info,refs},'_',State10)),

    {noreply,State11} = handle_cast({in,<<"hello">>,pid4},State10),
    ?assertEqual({reply,{ok,1},State11},handle_call({{info,running},<<"hello">>},'_',State11)),
    ?assertEqual({reply,{ok,0},State11},handle_call({{info,waiting},<<"hello">>},'_',State11)),
    ?assertEqual({reply,{ok,1},State11},handle_call({{info,total},<<"hello">>},'_',State11)),
    ?assertEqual({reply,{ok,2},State11},handle_call({info,refs},'_',State11)),

    {noreply,State12} = handle_cast({in,<<"hello">>,pid5},State11),
    ?assertEqual({reply,{ok,0},State12},handle_call({{info,running},<<"hello">>},'_',State12)),
    ?assertEqual({reply,{ok,0},State12},handle_call({{info,waiting},<<"hello">>},'_',State12)),
    ?assertEqual({reply,{ok,0},State12},handle_call({{info,total},<<"hello">>},'_',State12)),
    ?assertEqual({reply,{ok,1},State12},handle_call({info,refs},'_',State12)),

    {noreply,State13} = handle_cast({in,<<"hello1">>,pid2},State12),
    ?assertEqual({reply,{ok,0},State13},handle_call({{info,running},<<"hello1">>},'_',State13)),
    ?assertEqual({reply,{ok,0},State13},handle_call({{info,waiting},<<"hello1">>},'_',State13)),
    ?assertEqual({reply,{ok,0},State13},handle_call({{info,total},<<"hello1">>},'_',State13)),
    ?assertEqual({reply,{ok,0},State13},handle_call({info,refs},'_',State13)),

    ok.

-endif.