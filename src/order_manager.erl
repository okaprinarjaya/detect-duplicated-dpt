-module(order_manager).
-behaviour(gen_server).

-export([
  start_link/0,
  create_workers/3,
  create_orders/2,
  run_orders/1,
  empty_orders/0,
  next_orders/4]
).

-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]
).

-define(SRV, order_manager_srv).

-define(QUERY_STR, <<"SELECT id, nama, status_dpt FROM dpt_pemilihbali LIMIT ?, ?">>).

% -define(QUERY_STR, <<"SELECT 1 AS id, CONCAT(transaksi_id, ' ', kuesioner_id, ' ', pilihan_jawaban_id, ' ', pilihan_lain) AS nama, 3 AS status_dpt FROM data_masuk ORDER BY transaksi_id ASC LIMIT ?, ?">>).

-record(state, {procs_pids = [], procs_refs = [], orders = [], orders_len = 0, rows_per_batch = 0}).

start_link() ->
  gen_server:start_link({local, ?SRV}, ?MODULE, [], []).

init(_Args) ->
  {ok, #state{procs_pids = [], procs_refs = [], orders = []}}.

handle_cast({run_orders, OrdersPerBatch}, #state{orders = Orders, procs_pids = Workers} = State) ->
  lists:foreach(
    fun(WorkerPid) ->
      gen_server:cast(WorkerPid, {run_orders, make_ref(), lists:sublist(Orders, 1, OrdersPerBatch)})
    end,
    Workers
  ),

  {noreply, State#state{rows_per_batch = OrdersPerBatch}};

handle_cast({next_orders, WorkerPid, Ref, OrdersNextPage, TotalOrdersReceivedLen}, State) ->
  #state{orders = Orders, orders_len = OrdersLen, rows_per_batch = RowsPerBatch} = State,

  if
    TotalOrdersReceivedLen < OrdersLen ->
      StartOffset = (RowsPerBatch * OrdersNextPage) + 1,
      gen_server:cast(WorkerPid, {run_orders, Ref, lists:sublist(Orders, StartOffset, RowsPerBatch)});

    true ->
      gen_server:cast(WorkerPid, reset_state),
      io:format("Worker: ~p finish. Total rows received: ~p~n", [WorkerPid, TotalOrdersReceivedLen])
  end,

  {noreply, State};

handle_cast({create_workers, {TotalRows, RowsPerPage, StartOffset}}, State) ->
  #state{procs_pids = ProcsPids, procs_refs = ProcsRefs} = State,
  TotalPages = ceil(TotalRows / RowsPerPage),
  ProcessesNew = [worker_starter(Page, RowsPerPage, StartOffset) || Page <- lists:seq(0, TotalPages - 1)],
  {PidsNew, RefsNew} = pids_refs(ProcessesNew),

  {
    noreply,
    State#state{
      procs_pids = lists:append([PidsNew, ProcsPids]),
      procs_refs = lists:append([RefsNew, ProcsRefs])
    }
  }.

handle_call({create_orders, TotalOrders, StartOffset}, _From, #state{orders = Orders} = State) ->
  if
    length(Orders) < 1 ->
      {ok, _, Rows} = mysql_poolboy:query(pool1, ?QUERY_STR, [StartOffset, TotalOrders]),
      {reply, {ok, "Orders populated"}, State#state{orders = Rows, orders_len = length(Rows)}};

    true ->
      {reply, {ok, "Orders not empty. Please empty the orders first"}, State}
  end;

handle_call(empty_orders, _From, State) ->
  {reply, {ok, "Orders emptied"}, State#state{orders = []}}.

handle_info({'DOWN', Ref, process, Pid, _}, #state{procs_refs = Refs, procs_pids = Processes} = State) ->
  case lists:member(Ref, Refs) of
    true ->
      NewRefs = lists:delete(Ref, Refs),
      NewPids = lists:delete(Pid, Processes),

      {noreply, State#state{procs_refs = NewRefs, procs_pids = NewPids}};

    false ->
      {noreply, State}
  end;

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

-spec create_workers(TotalRows :: integer(), RowsPerPage :: integer(), StartOffset :: integer()) -> ok.
create_workers(TotalRows, RowsPerPage, StartOffset) ->
  gen_server:cast(?SRV, {create_workers, {TotalRows, RowsPerPage, StartOffset}}).

-type reply() :: {ok, string()}.
-spec create_orders(TotalOrders :: integer(), StartOffset :: integer()) -> reply().
create_orders(TotalOrders, StartOffset) ->
  gen_server:call(?SRV, {create_orders, TotalOrders, StartOffset}).

run_orders(OrdersPerBatch) ->
  gen_server:cast(?SRV, {run_orders, OrdersPerBatch}).

next_orders(WorkerPid, Ref, OrdersNextPage, TotalOrdersReceivedLen) ->
  gen_server:cast(?SRV, {next_orders, WorkerPid, Ref, OrdersNextPage, TotalOrdersReceivedLen}).

empty_orders() ->
  gen_server:call(?SRV, empty_orders).

worker_starter(Page, Limit, StartOffset) ->
  Offset = (Limit * Page) + StartOffset,
  {ok, Pid} = supervisor:start_child(worker_supervisor, [Offset, Limit]),
  Ref = erlang:monitor(process, Pid),

  {Pid, Ref}.

pids_refs(ListOfPidsRefs) ->
  pids_refs(ListOfPidsRefs, [], []).
pids_refs([], Pids, Refs) ->
  {Pids, Refs};
pids_refs(ListOfPidsRefs, Pids, Refs) ->
  [Head | Tail] = ListOfPidsRefs,
  {Pid, Ref} = Head,
  pids_refs(Tail, [Pid | Pids], [Ref | Refs]).
