-module(oprex_order_manager).
-behaviour(gen_server).

-export([start_link/0, create_workers/2, initiate_order/1, next_order/5]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SRV, oprex_order_manager_srv).
-define(WORKER_ARGS, [{dbhost, "localhost"}, {dbname, "dpt_in_da_house"}, {dbuser, "mamp"}, {dbpasswd, "12qwaszx@321"}]).
-define(TOTAL_ROWS, 1000000).
-define(DISTRIBUTE_ROWS_PER_PAGE, 10000).
-define(TOTAL_ORDERS, 10000).
-define(THE_ORDER_ROWS_PER_PAGE, 1000).

% -define(QUERY_STR, <<"SELECT id, nama, status_dpt FROM dpt_pemilihbali LIMIT ?, ?">>).
-define(QUERY_STR, <<"SELECT 1 AS id, CONCAT(transaksi_id, ' ', kuesioner_id, ' ', pilihan_jawaban_id, ' ', pilihan_lain) AS nama, 3 AS status_dpt FROM data_masuk ORDER BY transaksi_id ASC LIMIT ?, ?">>).

-record(state, {procs_pids = [], procs_refs = [], orders = []}).

start_link() ->
  gen_server:start_link({local, ?SRV}, ?MODULE, [], []).

init(_Args) ->
  MFA = {test_worker_server, start_link, []},
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

handle_call({create_workers, {TotalRows, RowsPerPage}}, _From, #state{procs_pids = PidsCurrent, procs_refs = RefsCurrent} = State) ->
  TotalPages = ceil(TotalRows / RowsPerPage),
  Pages = lists:seq(0, (TotalPages - 1)),
  ProcessesNew = [worker_starter(Page, RowsPerPage) || Page <- Pages],
  {PidsNew, RefsNew} = pids_refs(ProcessesNew),

  {reply, ok, State#state{procs_pids = lists:append([PidsNew, PidsCurrent]), procs_refs = lists:append([RefsNew, RefsCurrent])}}.

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

create_workers(TotalRows, RowsPerPage) ->
  gen_server:call(?SRV, {create_workers, {TotalRows, RowsPerPage}}).

worker_starter(Page, Limit) ->
  Offset = Limit * Page,
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

initiate_order(WorkerGroupName) ->
  gen_server:cast(?SRV, {initiate_order, WorkerGroupName}).

next_order(WorkerPid, Ref, TheOrderPage, TotalTheOrders, TotalTheOrdersReceived) ->
  gen_server:cast(?SRV, {next_order, WorkerPid, Ref, TheOrderPage, TotalTheOrders, TotalTheOrdersReceived}).
