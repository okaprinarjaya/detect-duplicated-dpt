-module(worker).
-behaviour(gen_server).

-export([start_link/1]).
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-record(state, {conn, rows, offset}).

% -define(SLEEP_RANDOM, [3000, 4000, 5000, 6000, 2000]).

% pick_random_sleep() ->
%   Res = lists:nth(rand:uniform(5), ?SLEEP_RANDOM),
%   Res.

start_link(Args) ->
  gen_server:start_link(?MODULE, Args, []).

init(_Args) ->
  % process_flag(trap_exit, true),
  % Hostname = proplists:get_value(dbhost, Args),
  % Database = proplists:get_value(dbname, Args),
  % Username = proplists:get_value(dbuser, Args),
  % Password = proplists:get_value(dbpasswd, Args),
  % {ok, DbConn} = mysql:start_link([
  %   {host, Hostname},
  %   {port, 8889},
  %   {user, Username},
  %   {password, Password},
  %   {database, Database}
  % ]),
  {ok, #state{offset=0}}.

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast({distribute_data, Page}, State) ->
  {noreply, State#state{rows=Page}};

handle_cast({receive_initial_order, Ref, TotalOrders, _InitialOrders}, #state{offset=Offset} = State) ->
  % io:format("~p is receiving: ~p~n", [self(), InitialOrders]),
  % timer:sleep(2000),
  order_manager:next_order(self(), Ref, TotalOrders, Offset + 5),
  {noreply, State#state{offset = Offset + 5}};

handle_cast({next_order, Ref, TotalOrders, NextOrders}, #state{offset=Offset} = State) ->
  % io:format("~p next order is receiving: ~p~n", [self(), NextOrders]),
  % timer:sleep(2000),
  order_manager:next_order(self(), Ref, TotalOrders, Offset + length(NextOrders)),
  {noreply, State#state{offset=Offset + 5}};

handle_cast(reset_state, State) ->
  {noreply, State#state{offset = 0}};

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  % ok = mysql:stop(DbConn),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
