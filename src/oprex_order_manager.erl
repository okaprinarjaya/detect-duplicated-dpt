-module(oprex_order_manager).
-behaviour(gen_server).

-export([start_link/0, create_workers/3, create_orders/1, run_orders/1, empty_orders/0, next_orders/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SRV, oprex_order_manager_srv).

% -define(QUERY_STR, <<"SELECT id, nama, status_dpt FROM dpt_pemilihbali LIMIT ?, ?">>).

-define(QUERY_STR, <<"SELECT 1 AS id, CONCAT(transaksi_id, ' ', kuesioner_id, ' ', pilihan_jawaban_id, ' ', pilihan_lain) AS nama, 3 AS status_dpt FROM data_masuk ORDER BY transaksi_id ASC LIMIT ?, ?">>).

-record(state, {procs_pids = [], procs_refs = [], orders = [], orders_len = 0, rows_per_batch = 0}).

start_link() ->
  gen_server:start_link({local, ?SRV}, ?MODULE, [], []).

init(_Args) ->
  MFA = {oprex_worker, start_link, []},
  gen_server:cast(self(), {start_oprex_worker_supervisor, MFA}),

  {ok, #state{procs_pids = [], procs_refs = [], orders = []}}.

handle_cast({start_oprex_worker_supervisor, {M, F, A}}, State) ->
  ChildSpecs = #{
    id => oprex_worker_supervisor_id,
    start => {oprex_worker_supervisor, start_link, [{M,F,A}]},
    restart => temporary,
    shutdown => 10000,
    type => supervisor,
    modules => [oprex_worker_supervisor]
  },
  {ok, _Pid} = supervisor:start_child(
    oprex_supervisor,
    ChildSpecs
  ),

  {noreply, State};

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

  {noreply, State#state{procs_pids = lists:append([PidsNew, ProcsPids]), procs_refs = lists:append([RefsNew, ProcsRefs])}}.

handle_call({create_orders, TotalOrders}, _From, #state{orders = Orders} = State) ->
  if
    length(Orders) < 1 ->
      {ok, DbCredentials} = application:get_env(dist_procs_je_asane, dbcredentials),
      DbHost = proplists:get_value(dbhost, DbCredentials),
      DbName = proplists:get_value(dbname, DbCredentials),
      DbUser = proplists:get_value(dbuser, DbCredentials),
      DbPasswd = proplists:get_value(dbpasswd, DbCredentials),

      {ok, DbConn} = mysql:start_link([
        {host, DbHost},
        {port, 8889},
        {user, DbUser},
        {password, DbPasswd},
        {database, DbName}
      ]),

      {ok, _, Rows} = mysql:query(DbConn, ?QUERY_STR, [0, TotalOrders]),
      ok = mysql:stop(DbConn),

      {reply, {ok, <<"Orders populated">>}, State#state{orders = Rows, orders_len = length(Rows)}};

    true ->
      {reply, {ok, <<"Orders not empty. Please empty the orders first">>}, State}
  end;

handle_call(empty_orders, _From, State) ->
  {reply, {ok, <<"Orders emptied">>}, State#state{orders = []}}.

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

create_orders(TotalOrders) ->
  gen_server:call(?SRV, {create_orders, TotalOrders}).

run_orders(OrdersPerBatch) ->
  gen_server:cast(?SRV, {run_orders, OrdersPerBatch}).

next_orders(WorkerPid, Ref, OrdersNextPage, TotalOrdersReceivedLen) ->
  gen_server:cast(?SRV, {next_orders, WorkerPid, Ref, OrdersNextPage, TotalOrdersReceivedLen}).

empty_orders() ->
  gen_server:call(?SRV, empty_orders).

worker_starter(Page, Limit, StartOffset) ->
  Offset = (Limit * Page) + StartOffset,
  {ok, Pid} = supervisor:start_child(oprex_worker_supervisor, [Offset, Limit]),
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
