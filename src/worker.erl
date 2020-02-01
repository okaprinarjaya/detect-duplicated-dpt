-module(worker).
-behaviour(gen_server).

-export([start_link/2]).
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
  {ok, DbCredentials} = application:get_env(detectdouble, dbcredentials),
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

  {ok, _, Rows} = mysql:query(DbConn, ?QUERY_STR, [Offset, Limit]),
  ok = mysql:stop(DbConn),

  {ok, #state{rows = Rows, rows_len = length(Rows)}}.

handle_cast({run_orders, Ref, OrdersSubmit}, State) ->
  #state{rows = Rows, total_orders_received = TotalOrdersReceivedLen, orders_paging = Page} = State,
  TotalOrdersReceivedNextLen = TotalOrdersReceivedLen + length(OrdersSubmit),
  NextPage = Page + 1,

  compare(OrdersSubmit, Rows, self()),

  order_manager:next_orders(self(), Ref, NextPage, TotalOrdersReceivedNextLen),

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

compare(Orders, Rows, Pid) ->
  lists:foreach(
    fun(Order) ->
      [_, OrderNama, _] = Order,
      lists:foreach(
        fun(Row) ->
          [_, RowNama, _] = Row,

          if
            OrderNama =/= <<>> andalso RowNama =/= <<>> andalso OrderNama =/= RowNama  ->
              CompareLen = binary:longest_common_prefix([OrderNama, RowNama]),
              BLen = byte_size(RowNama),
              Similarity = (CompareLen / BLen) * 100,

              if
                Similarity > 90 ->
                  io:format("Double data detected! - (~p) is similar with (~p) found at: ~p~n", [OrderNama, RowNama, Pid]);
                true -> ok
              end;

            true -> ok
          end

        end,
        Rows
      )
    end,
    Orders
  ).
