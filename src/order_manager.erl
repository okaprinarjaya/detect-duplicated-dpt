-module(order_manager).
-behaviour(gen_server).

-export([start_link/0, create_order/1, cast_order/1, next_order/4, initiate_worker_data/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SRV, order_manager_srv).
-define(POOL_ARGS(PoolName), [{name, {local, PoolName}}, {worker_module, worker}, {size, 10}, {max_overflow, 0}]).
-define(WORKER_ARGS, [{dbhost, "localhost"}, {dbname, "dpt_in_da_house"}, {dbuser, "mamp"}, {dbpasswd, "12qwaszx@321"}]).
-define(TOTAL_ROWS, 1600).
-define(ROWS_PER_PAGE, 160).

-record(state, {conn, orders}).

start_link() ->
  gen_server:start_link({local, ?SRV}, ?MODULE, [], []).

create_order(OrderName) ->
  gen_server:call(?SRV, {create_order, OrderName}).

initiate_worker_data(PoolName) ->
  TotalPages = ceil(?TOTAL_ROWS / ?ROWS_PER_PAGE),
  Pages = lists:seq(0, TotalPages - 1),
  gen_server:cast(?SRV, {initiate_worker_data, PoolName, Pages}).

cast_order(PoolName) ->
  gen_server:call(?SRV, {cast_order, PoolName}).

next_order(WorkerPid, Ref, TotalOrders, TotalRowsReceived) ->
  gen_server:cast(?SRV, {next_order, WorkerPid, Ref, TotalOrders, TotalRowsReceived}).

distribute_data(_PoolName, []) ->
  ok;

distribute_data(PoolName, Pages) ->
  [Page|TailPages] = Pages,
  spawn(fun() ->
    poolboy:transaction(
      PoolName,
      fun(Worker) ->
        gen_server:cast(Worker, {distribute_data, Page})
      end
    )
  end),
  distribute_data(PoolName, TailPages).

buzz(_N, 11, _PoolName, _TotalOrders, _InitialOrders) ->
  ok;

buzz(N, Incr, PoolName, TotalOrders, InitialOrders) ->
  spawn(fun() ->
    poolboy:transaction(
      PoolName,
      fun(Worker) ->
        gen_server:cast(Worker, {receive_initial_order, make_ref(), TotalOrders, InitialOrders})
      end
    )
  end),
  buzz(N, Incr+1, PoolName, TotalOrders, InitialOrders).

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

handle_call({create_order, OrderName}, _From, State) ->
  {ok, Pid} = supervisor:start_child(
    dist_procs_je_asane_sup,
    poolboy:child_spec(OrderName, ?POOL_ARGS(OrderName), ?WORKER_ARGS)
  ),
  link(Pid),

  {reply, ok, State};

handle_call({cast_order, PoolName}, _From, #state{orders=Orders} = State) ->
  InitialOrders = lists:sublist(Orders, 1, 5),
  buzz(10, 1, PoolName, ?TOTAL_ROWS, InitialOrders),

  {reply, ok, State}.

handle_cast({initiate_worker_data, PoolName, Pages}, State) ->
  distribute_data(PoolName, Pages),

  {noreply, State};

handle_cast({next_order, WorkerPid, Ref, TotalOrders, TotalRowsReceived}, #state{orders=Orders} = State) ->
  NextOffsetStart = TotalRowsReceived + 1,
  if NextOffsetStart =< TotalOrders ->
    NextOrders = lists:sublist(Orders, NextOffsetStart, 5),
    gen_server:cast(WorkerPid, {next_order, Ref, TotalOrders, NextOrders});
  true ->
    gen_server:cast(WorkerPid, reset_state),
    io:format("Worker: ~p finish. Total rows received: ~p~n", [WorkerPid, TotalRowsReceived])
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
