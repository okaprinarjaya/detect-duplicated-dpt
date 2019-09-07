-module(test_worker_server).
-behaviour(gen_server).
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {something}).

start_link(StartOffset) ->
  gen_server:start_link(?MODULE, StartOffset, []).

init(StartOffset) ->
  io:format("Starting: ~p, with params StartOffset: ~p~n", [self(), StartOffset]),
  {ok, #state{something=undefined}}.

handle_call(_Msg, _From, State) ->
  {noreply, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(stop, State) ->
  {stop, normal, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
