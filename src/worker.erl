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

-define(DISTRIBUTE_ROWS_PER_PAGE, 10000).
% -define(QUERY_STR, <<"SELECT id, nama, status_dpt FROM dpt_pemilihbali LIMIT ?, ?">>).
-define(QUERY_STR, <<"SELECT 1 AS id, CONCAT(transaksi_id, ' ', kuesioner_id, ' ', pilihan_jawaban_id, ' ', pilihan_lain) AS nama, 3 AS status_dpt FROM data_masuk ORDER BY transaksi_id ASC LIMIT ?, ?">>).

-record(state, {
  rows, rows_received,
  the_order_page, doubled_data_found
}).

start_link(Args) ->
  gen_server:start_link(?MODULE, Args, []).

init(_Args) ->
  {ok, #state{
    the_order_page = 0, rows = [],
    rows_received = 0, doubled_data_found = []
  }}.

handle_call(get_state, _From, State) ->
  #state{
    rows=Rows,
    rows_received=RowsReceived,
    the_order_page=TheOrderPage
  } = State,
  Reply = {ok, [
    {rows, Rows}, {rows_received, RowsReceived},
    {the_order_page, TheOrderPage}
  ]},

  {reply, Reply, State};

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast({distribute_data, DbConn, Page}, State) ->
  OffsetStart = ?DISTRIBUTE_ROWS_PER_PAGE * Page,
  {ok, _, Rows} = mysql:query(DbConn, ?QUERY_STR, [OffsetStart, ?DISTRIBUTE_ROWS_PER_PAGE]),

  {noreply, State#state{
    rows = Rows
  }};

handle_cast({initiate_order, Ref, TotalTheOrders, InitialTheOrders}, State) ->
  #state{the_order_page=TheOrderPage, rows = WorkerDataDPT} = State,
  RowsReceived = length(InitialTheOrders),

  lists:foreach(
    fun(ElemTheOrder) ->
      [_IdTheOrder, NamaTheOrder, _] = ElemTheOrder,
      lists:foreach(
        fun(ElemGalaxy) ->
          [_IdGalaxy, NamaGalaxy, _] = ElemGalaxy,

          if NamaTheOrder =/= <<>> andalso NamaGalaxy =/= <<>> andalso NamaTheOrder =/= NamaGalaxy  ->
            CompareLen = binary:longest_common_prefix([NamaTheOrder, NamaGalaxy]),
            BLen = byte_size(NamaGalaxy),
            Similarity = (CompareLen / BLen) * 100,

            if Similarity > 90 ->
              io:format("Double data detected! - (~p) is similar with (~p) found at: ~p~n", [NamaTheOrder, NamaGalaxy, self()]);
              true -> ok
            end;

            true -> ok
          end

        end,
        WorkerDataDPT
      )
    end,
    InitialTheOrders
  ),

  order_manager:next_order(self(), Ref, TheOrderPage + 1, TotalTheOrders, RowsReceived),

  {noreply, State#state{the_order_page = TheOrderPage + 1, rows_received = RowsReceived}};

handle_cast({next_order, Ref, TotalTheOrders, NextTheOrders}, State) ->
  #state{
    the_order_page = TheOrderPage, rows = WorkerDataDPT,
    rows_received = RowsReceived
  } = State,
  RowsReceivedNext = RowsReceived + length(NextTheOrders),

  lists:foreach(
    fun(ElemTheOrder) ->
      [_IdTheOrder, NamaTheOrder, _] = ElemTheOrder,
      lists:foreach(
        fun(ElemGalaxy) ->
          [_IdGalaxy, NamaGalaxy, _] = ElemGalaxy,

          if NamaTheOrder =/= NamaGalaxy andalso NamaTheOrder =/= <<>> andalso NamaGalaxy =/= <<>>  ->
            CompareLen = binary:longest_common_prefix([NamaTheOrder, NamaGalaxy]),
            BLen = byte_size(NamaGalaxy),
            Similarity = (CompareLen / BLen) * 100,

            if Similarity > 90 ->
              io:format("Double data detected! - (~p) is similar with (~p) found at: ~p~n", [NamaTheOrder, NamaGalaxy, self()]);
              true -> ok
            end;

            true -> ok
          end

        end,
        WorkerDataDPT
      )
    end,
    NextTheOrders
  ),

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
