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

-record(state, {hostname, database, username, password, rows, the_order_offset}).

start_link(Args) ->
  gen_server:start_link(?MODULE, Args, []).

init(Args) ->
  Hostname = proplists:get_value(dbhost, Args),
  Database = proplists:get_value(dbname, Args),
  Username = proplists:get_value(dbuser, Args),
  Password = proplists:get_value(dbpasswd, Args),

  {ok, #state{
    hostname = Hostname, database = Database,
    username = Username, password = Password,
    the_order_offset = 0
  }}.

handle_call(get_state, _From, State) ->
  #state{
    rows=Rows,
    the_order_offset=TheOrderOffset,
    hostname = Hostname,
    database = Database,
    username = Username,
    password = Password
  } = State,
  Reply = {ok, [
    {rows, Rows}, {the_order_offset, TheOrderOffset},
    {hostname, Hostname}, {database, Database},
    {username, Username}, {password, Password}
  ]},

  {reply, Reply, State};

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast({distribute_data, Page}, State) ->
  #state{
    hostname = Hostname,
    database = Database,
    username = Username,
    password = Password
  } = State,

  {ok, DbConn} = mysql:start_link([
    {host, Hostname},
    {port, 8889},
    {user, Username},
    {password, Password},
    {database, Database}
  ]),

  OffsetStart = 160 * Page,
  {ok, _, Rows} = mysql:query(DbConn, "SELECT id, nama, status_dpt FROM dpt_pemilihbali LIMIT ?, 160", [OffsetStart]),
  ok = mysql:stop(DbConn),

  {noreply, State#state{rows=Rows}};

handle_cast({receive_initial_order, Ref, TotalOrders, _InitialOrders}, State) ->
  #state{the_order_offset=TheOrderOffset} = State,
  order_manager:next_order(self(), Ref, TotalOrders, TheOrderOffset + 5),

  {noreply, State#state{the_order_offset = TheOrderOffset + 5}};

handle_cast({next_order, Ref, TotalOrders, NextOrders}, State) ->
  #state{the_order_offset = TheOrderOffset} = State,
  order_manager:next_order(self(), Ref, TotalOrders, TheOrderOffset + length(NextOrders)),

  {noreply, State#state{the_order_offset = TheOrderOffset + 5}};

handle_cast(reset_state, State) ->
  {noreply, State#state{the_order_offset = 0}};

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
