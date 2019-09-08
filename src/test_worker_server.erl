-module(test_worker_server).
-behaviour(gen_server).
-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {something}).

start_link(Offset, Limit) ->
  gen_server:start_link(?MODULE, [Offset, Limit], []).

init([Offset, Limit]) ->
  io:format("Starting: ~p, with params Offset: ~p, Limit: ~p~n", [self(), Offset, Limit]),
  {ok, #state{something=undefined}}.

handle_call(hello, From, State) ->
  io:format("hello I am ~p  - I receive message from: ~p~n", [self(), From]),
  {reply, ok, State};

handle_call(_Msg, _From, State) ->
  {noreply, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(stop, State) ->
  {stop, normal, State};

handle_info(crash, State) ->
  list_to_atom({a,b,c}),
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
