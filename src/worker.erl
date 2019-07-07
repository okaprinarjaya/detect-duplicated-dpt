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

-record(state, {hostname, database, username, password, rows, rows_received, the_order_page}).

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
    the_order_page = 0
  }}.

handle_call(get_state, _From, State) ->
  #state{
    rows=Rows,
    rows_received=RowsReceived,
    the_order_page=TheOrderPage,
    hostname = Hostname,
    database = Database,
    username = Username,
    password = Password
  } = State,
  Reply = {ok, [
    {rows, Rows}, {rows_received, RowsReceived},
    {the_order_page, TheOrderPage},
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

  {noreply, State#state{
    hostname = undefined, database = undefined,
    username = undefined, password = undefined,
    rows=Rows
  }};

handle_cast({receive_initial_order, Ref, TotalTheOrders, InitialTheOrders}, State) ->
  #state{the_order_page=TheOrderPage} = State,
  RowsReceived = length(InitialTheOrders),
  order_manager:next_order(self(), Ref, TheOrderPage + 1, TotalTheOrders, RowsReceived),

  {noreply, State#state{the_order_page = TheOrderPage + 1, rows_received = RowsReceived}};

handle_cast({next_order, Ref, TotalTheOrders, NextTheOrders}, State) ->
  #state{the_order_page = TheOrderPage, rows_received = RowsReceived} = State,
  RowsReceivedNext = RowsReceived + length(NextTheOrders),
  order_manager:next_order(self(), Ref, TheOrderPage + 1, TotalTheOrders, RowsReceivedNext),

  {noreply, State#state{the_order_page = TheOrderPage + 1, rows_received = RowsReceivedNext}};

handle_cast(reset_state, State) ->
  {noreply, State#state{the_order_page = 0, rows_received = 0}};

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
