-module(order_manager).
-behaviour(gen_server).

-export([start_link/0, create_workers/1, initiate_order/1, next_order/5, initiate_worker_data/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SRV, order_manager_srv).
-define(POOL_ARGS(WorkerGroupName), [{name, {local, WorkerGroupName}}, {worker_module, worker}, {size, 10}, {max_overflow, 0}]).
-define(WORKER_ARGS, [{dbhost, "localhost"}, {dbname, "dpt_in_da_house"}, {dbuser, "mamp"}, {dbpasswd, "12qwaszx@321"}]).
-define(TOTAL_ROWS, 1600).
-define(DISTRIBUTE_ROWS_PER_PAGE, 160).
-define(THE_ORDER_ROWS_PER_PAGE, 5).

-record(state, {conn, orders}).

start_link() ->
  gen_server:start_link({local, ?SRV}, ?MODULE, [], []).

create_workers(WorkerGroupName) ->
  gen_server:call(?SRV, {create_workers, WorkerGroupName}).

initiate_worker_data(WorkerGroupName) ->
  TotalPages = ceil(?TOTAL_ROWS / ?DISTRIBUTE_ROWS_PER_PAGE),
  Pages = lists:seq(0, TotalPages - 1),
  gen_server:cast(?SRV, {initiate_worker_data, WorkerGroupName, Pages}).

initiate_order(WorkerGroupName) ->
  gen_server:call(?SRV, {initiate_order, WorkerGroupName}).

next_order(WorkerPid, Ref, TheOrderPage, TotalTheOrders, TotalTheOrdersReceived) ->
  gen_server:cast(?SRV, {next_order, WorkerPid, Ref, TheOrderPage, TotalTheOrders, TotalTheOrdersReceived}).

distribute_data(_WorkerGroupName, []) ->
  ok;

distribute_data(WorkerGroupName, Pages) ->
  [Page | TailPages] = Pages,
  spawn(fun() ->
    poolboy:transaction(
      WorkerGroupName,
      fun(Worker) ->
        gen_server:cast(Worker, {distribute_data, Page})
      end
    )
  end),
  distribute_data(WorkerGroupName, TailPages).

distribute_initial_order(_N, 11, _WorkerGroupName, _TotalTheOrders, _InitialTheOrders) ->
  ok;

distribute_initial_order(N, Incr, WorkerGroupName, TotalTheOrders, InitialTheOrders) ->
  spawn(fun() ->
    poolboy:transaction(
      WorkerGroupName,
      fun(Worker) ->
        gen_server:cast(Worker, {initiate_order, make_ref(), TotalTheOrders, InitialTheOrders})
      end
    )
  end),
  distribute_initial_order(N, Incr+1, WorkerGroupName, TotalTheOrders, InitialTheOrders).

init(_Args) ->
  Hostname = proplists:get_value(dbhost, ?WORKER_ARGS),
  Database = proplists:get_value(dbname, ?WORKER_ARGS),
  Username = proplists:get_value(dbuser, ?WORKER_ARGS),
  Password = proplists:get_value(dbpasswd, ?WORKER_ARGS),

  {ok, DbConn} = mysql:start_link([
    {host, Hostname},
    {port, 8889},
    {user, Username},
    {password, Password},
    {database, Database}
  ]),

  {ok, _, Rows} = mysql:query(DbConn, <<"SELECT id, nama, status_dpt FROM dpt_pemilihbali">>),
  ok = mysql:stop(DbConn),

  {ok, #state{conn=DbConn, orders=Rows}}.

handle_call({create_workers, WorkerGroupName}, _From, State) ->
  {ok, Pid} = supervisor:start_child(
    dist_procs_je_asane_sup,
    poolboy:child_spec(WorkerGroupName, ?POOL_ARGS(WorkerGroupName), ?WORKER_ARGS)
  ),
  link(Pid),

  {reply, ok, State};

handle_call({initiate_order, WorkerGroupName}, _From, #state{orders=Orders} = State) ->
  TheOrderOffsetStart = (?THE_ORDER_ROWS_PER_PAGE * 0) + 1,
  InitialTheOrders = lists:sublist(Orders, TheOrderOffsetStart, ?THE_ORDER_ROWS_PER_PAGE),
  distribute_initial_order(10, 1, WorkerGroupName, ?TOTAL_ROWS, InitialTheOrders),

  {reply, ok, State}.

handle_cast({initiate_worker_data, WorkerGroupName, Pages}, State) ->
  distribute_data(WorkerGroupName, Pages),

  {noreply, State};

handle_cast({next_order, WorkerPid, Ref, TheOrderPage, TotalTheOrders, TotalTheOrdersReceived}, State) ->
  #state{orders=TheOrders} = State,
  TheOrderNextOffsetStart = (?THE_ORDER_ROWS_PER_PAGE * TheOrderPage) + 1,

  if TotalTheOrdersReceived < TotalTheOrders ->
    NextTheOrders = lists:sublist(TheOrders, TheOrderNextOffsetStart, ?THE_ORDER_ROWS_PER_PAGE),
    gen_server:cast(WorkerPid, {next_order, Ref, TotalTheOrders, NextTheOrders});
  true ->
    gen_server:cast(WorkerPid, reset_state),
    io:format("Worker: ~p finish. Total rows received: ~p~n", [WorkerPid, TotalTheOrdersReceived])
  end,

  {noreply, State};

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
