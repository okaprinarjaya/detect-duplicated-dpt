-module(worker).
-behaviour(gen_server).

-export([start_link/2, compare/3]).
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-define(QUERY_STR, <<"SELECT id, nama, status_dpt FROM dpt_pemilihbali LIMIT ?, ?">>).

% -define(QUERY_STR, <<"SELECT 1 AS id, CONCAT(transaksi_id, ' ', kuesioner_id, ' ', pilihan_jawaban_id, ' ', pilihan_lain) AS nama, 3 AS status_dpt FROM data_masuk ORDER BY transaksi_id ASC LIMIT ?, ?">>).

-record(state, {rows = [], rows_len = 0, total_orders_received = 0, orders_paging = 0}).

start_link(Offset, Limit) ->
  gen_server:start_link(?MODULE, [Offset, Limit], []).

init([Offset, Limit]) ->
  {ok, _, Rows} = mysql_poolboy:query(pool1, ?QUERY_STR, [Offset, Limit]),
  {ok, #state{rows = Rows, rows_len = length(Rows)}}.

handle_cast({worker_run_orders, Ref, OrdersSubmit}, State) ->
  #state{rows = Rows, total_orders_received = TotalOrdersReceivedLen, orders_paging = Page} = State,
  TotalOrdersReceivedNextLen = TotalOrdersReceivedLen + length(OrdersSubmit),
  NextPage = Page + 1,

  % compare(OrdersSubmit, Rows, self()),
  {MicrosecondsTime, _Value} = timer:tc(worker, compare, [OrdersSubmit, Rows, self()]),
  order_manager:next_run_orders(self(), Ref, NextPage, TotalOrdersReceivedNextLen, MicrosecondsTime),

  {noreply, State#state{orders_paging = NextPage, total_orders_received = TotalOrdersReceivedNextLen}};

handle_cast(reset_state, State) ->
  {noreply, State#state{total_orders_received = 0, orders_paging = 0}};

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

compare(Orders, SourcesCompare, Pid) ->
  iterate(Orders, SourcesCompare, Pid).

iterate([], _, _) ->
  ok;
iterate(ListOrders, ListSourcesCompare, Pid) ->
  [HeadOrder | ListTailOrders] = ListOrders,
  [SingleRowId, SingleOrderNama, _] = HeadOrder,
  iterate_compare(SingleRowId, SingleOrderNama, ListSourcesCompare, Pid),
  iterate(ListTailOrders, ListSourcesCompare, Pid).

iterate_compare(_, _, [], _) -> ok;
iterate_compare(SingleRowId, SingleOrderNama, ListSourcesCompare, Pid) ->
  [HeadSourcesCompare | ListTailSourcesCompare] = ListSourcesCompare,
  [SourceRowId, SourceRowNama, _] = HeadSourcesCompare,

  if
    SingleOrderNama =/= <<>> andalso SourceRowNama =/= <<>> andalso SingleRowId =/= SourceRowId  ->
      SingleOrderNamaStr = binary_to_list(SingleOrderNama),
      SourceRowNamaStr = binary_to_list(SourceRowNama),
      CompareLen = someone_lcs:someone_lcs(SingleOrderNamaStr, SourceRowNamaStr),
      BLen = length(SourceRowNamaStr),
      Similarity = (CompareLen / BLen) * 100,

      if
        Similarity > 90 ->
          ets:insert(ets_suspicious, {SourceRowId});
          % io:format(
          %   "Double data detected! - (~p) is similar with (~p), similarity: ~p found at: ~p~n",
          %   [SingleOrderNama, SourceRowNama, Similarity, Pid]
          % );
        true -> ok
      end;

    true -> ok
  end,
  iterate_compare(SingleRowId, SingleOrderNama, ListTailSourcesCompare, Pid).
