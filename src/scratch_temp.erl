-module(scratch_temp).

-compile(export_all).

-define(WORKER_ARGS, [{dbhost, "localhost"}, {dbname, "dpt_in_da_house"}, {dbuser, "mamp"}, {dbpasswd, "12qwaszx@321"}]).

% -define(QUERY_STR, <<"SELECT id, nama, status_dpt FROM dpt_pemilihbali LIMIT ?, ?">>).
-define(QUERY_STR, <<"SELECT 1 AS id, CONCAT(transaksi_id, ' ', kuesioner_id, ' ', pilihan_jawaban_id, ' ', pilihan_lain) AS nama, 3 AS status_dpt FROM data_masuk LIMIT ?, ?">>).

get_worker_state(Pid) when is_pid(Pid) ->
  {ok, PropLists} = gen_server:call(Pid, get_state),
  PropLists.

populate_dpt_ids([], IdSeqs) ->
  IdSeqs;

populate_dpt_ids(Rows, IdSeqs) ->
  [Row | TailRows] = Rows,
  [Id, Nama, _StatusDPT] = Row,
  Concat = integer_to_list(Id) ++ binary_to_list(<<"#">>) ++ binary_to_list(Nama),
  populate_dpt_ids(TailRows, [Concat | IdSeqs]).

detail_worker_data(Pid) when is_pid(Pid) ->
  Rows = proplists:get_value(rows, get_worker_state(Pid)),
  Data = populate_dpt_ids(Rows, []),
  {length(Data), lists:sublist(Data, 1, 10)}.

page_worker_data(Pid) when is_pid(Pid) ->
  proplists:get_value(the_order_page, get_worker_state(Pid)).

test_get_worker_data() ->
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

  {ok, _, Rows} = mysql:query(DbConn, ?QUERY_STR, [0, 125000]),
  ok = mysql:stop(DbConn),
  Rows.

test_get_the_orders() ->
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

  {ok, _, Rows} = mysql:query(DbConn, ?QUERY_STR, [0, 1]),
  ok = mysql:stop(DbConn),
  Rows.

test_working() ->
  RowsTheOrders = test_get_the_orders(),
  RowsWorkerData = test_get_worker_data(),

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
        RowsWorkerData
      )
    end,
    RowsTheOrders
  ).
