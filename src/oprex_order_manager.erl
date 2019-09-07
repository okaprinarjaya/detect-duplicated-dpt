-module(oprex_order_manager).
-behaviour(gen_server).

-export([start_link/0, create_workers/1, initiate_order/1, next_order/5, initiate_worker_data/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SRV, oprex_order_manager_srv).
-define(POOL_ARGS(WorkerGroupName), [{name, {local, WorkerGroupName}}, {worker_module, oprex_worker}, {size, 100}, {max_overflow, 0}]).
-define(WORKER_ARGS, [{dbhost, "localhost"}, {dbname, "dpt_in_da_house"}, {dbuser, "mamp"}, {dbpasswd, "12qwaszx@321"}]).
-define(TOTAL_ROWS, 1000000).
-define(DISTRIBUTE_ROWS_PER_PAGE, 10000).
-define(TOTAL_ORDERS, 10000).
-define(THE_ORDER_ROWS_PER_PAGE, 1000).

% -define(QUERY_STR, <<"SELECT id, nama, status_dpt FROM dpt_pemilihbali LIMIT ?, ?">>).
-define(QUERY_STR, <<"SELECT 1 AS id, CONCAT(transaksi_id, ' ', kuesioner_id, ' ', pilihan_jawaban_id, ' ', pilihan_lain) AS nama, 3 AS status_dpt FROM data_masuk ORDER BY transaksi_id ASC LIMIT ?, ?">>).

-record(state, {procs = [], orders = []}).

start_link() ->
  gen_server:start_link({local, ?SRV}, ?MODULE, [], []).

init(_Args) ->
  gen_server:cast(self(), {start_oprex_worker_supervisor, {test_worker_server, start_link, []}}),
  {ok, #state{procs = [], orders = []}}.

handle_cast({start_oprex_worker_supervisor, {M, F, A}}, State) ->
  ChildSpecs = #{
    id => oprex_worker_supervisor_id,
    start => {oprex_worker_supervisor, start_link, [{M,F,A}]},
    restart => temporary,
    shutdown => 10000,
    type => supervisor,
    modules => [oprex_worker_supervisor]
  },
  {ok, Pid} = supervisor:start_child(
    oprex_supervisor,
    ChildSpecs
  ),
  link(Pid),

  {noreply, State}.

% handle_cast({initiate_order, WorkerGroupName}, #state{orders = Orders} = State) ->
%   TheOrderOffsetStart = (?THE_ORDER_ROWS_PER_PAGE * 0) + 1,
%   InitialTheOrders = lists:sublist(Orders, TheOrderOffsetStart, ?THE_ORDER_ROWS_PER_PAGE),
%   distribute_initial_order(1, WorkerGroupName, ?TOTAL_ORDERS, InitialTheOrders),

%   {noreply, State};

% handle_cast({initiate_worker_data, WorkerGroupName, Pages}, #state{conn = DbConn} = State) ->
%   distribute_data(DbConn, WorkerGroupName, Pages),

%   {noreply, State};

% handle_cast({next_order, WorkerPid, Ref, TheOrderPage, TotalTheOrders, TotalTheOrdersReceived}, State) ->
%   #state{orders=TheOrders} = State,
%   TheOrderNextOffsetStart = (?THE_ORDER_ROWS_PER_PAGE * TheOrderPage) + 1,

%   if TotalTheOrdersReceived < TotalTheOrders ->
%     NextTheOrders = lists:sublist(TheOrders, TheOrderNextOffsetStart, ?THE_ORDER_ROWS_PER_PAGE),
%     gen_server:cast(WorkerPid, {next_order, Ref, TotalTheOrders, NextTheOrders});
%   true ->
%     gen_server:cast(WorkerPid, reset_state),
%     io:format("Worker: ~p finish. Total rows received: ~p~n", [WorkerPid, TotalTheOrdersReceived])
%   end,

%   {noreply, State}.

handle_call({create_workers, _Args}, _From, #state{procs = Processes} = State) ->
  {ok, Pid} = supervisor:start_child(oprex_worker_supervisor, [{wrk1args1, wrk1args2, wrk1args3}]),
  Ref = erlang:monitor(process, Pid),
  io:format("Worker added: ~p~n", [Ref]),

  {reply, okdokie, State#state{procs = [Pid | Processes]}}.

handle_info(procsnumbers, #state{procs = Processes} = State) ->
  io:format("Processes numbers: ~p~n", [length(Processes)]),
  io:format("Processes: ~p~n", [Processes]),

  {noreply, State};

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

create_workers({M, F, A}) ->
  Resp = gen_server:call(?SRV, {create_workers, {M, F, A}}),
  io:format("Resp = ~p~n", [Resp]).

initiate_worker_data(WorkerGroupName) ->
  TotalPages = ceil(?TOTAL_ROWS / ?DISTRIBUTE_ROWS_PER_PAGE),
  Pages = lists:seq(0, TotalPages - 1),
  gen_server:cast(?SRV, {initiate_worker_data, WorkerGroupName, Pages}).

initiate_order(WorkerGroupName) ->
  io:format("Start at: ~p~n", [calendar:local_time()]),
  gen_server:cast(?SRV, {initiate_order, WorkerGroupName}).

next_order(WorkerPid, Ref, TheOrderPage, TotalTheOrders, TotalTheOrdersReceived) ->
  gen_server:cast(?SRV, {next_order, WorkerPid, Ref, TheOrderPage, TotalTheOrders, TotalTheOrdersReceived}).

distribute_data(_, _WorkerGroupName, []) ->
  ok;

distribute_data(DbConn, WorkerGroupName, Pages) ->
  [Page | TailPages] = Pages,
  spawn(fun() ->
    poolboy:transaction(
      WorkerGroupName,
      fun(Worker) ->
        gen_server:cast(Worker, {distribute_data, DbConn, Page})
      end
    )
  end),
  distribute_data(DbConn, WorkerGroupName, TailPages).

distribute_initial_order(101, _WorkerGroupName, _TotalTheOrders, _InitialTheOrders) ->
  ok;

distribute_initial_order(Incr, WorkerGroupName, TotalTheOrders, InitialTheOrders) ->
  spawn(fun() ->
    poolboy:transaction(
      WorkerGroupName,
      fun(Worker) ->
        gen_server:cast(Worker, {initiate_order, make_ref(), TotalTheOrders, InitialTheOrders})
      end
    )
  end),
  distribute_initial_order(Incr + 1, WorkerGroupName, TotalTheOrders, InitialTheOrders).
