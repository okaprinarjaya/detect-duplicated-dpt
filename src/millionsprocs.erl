-module(millionsprocs).
-export([detect_duplicate/1, process_data/0]).

-define(WORKER_ARGS, [{dbhost, "localhost"}, {dbname, "dpt_in_da_house"}, {dbuser, "mamp"}, {dbpasswd, "12qwaszx@321"}]).
-define(QUERY_STR, <<"SELECT 1 AS id, CONCAT(transaksi_id, ' ', kuesioner_id, ' ', pilihan_jawaban_id, ' ', pilihan_lain) AS nama, 3 AS status_dpt FROM data_masuk ORDER BY transaksi_id ASC LIMIT ?, ?">>).

detect_duplicate([A, B]) ->
  if A =/= <<>> andalso B =/= <<>> andalso A =/= B  ->
    CompareLen = binary:longest_common_prefix([A, B]),
    BLen = byte_size(B),
    Similarity = (CompareLen / BLen) * 100,

    if Similarity > 90 ->
      io:format("Double data detected! - (~p) is similar with (~p)~n", [A, B]);
      true -> ok
    end;

    true -> ok
  end.

process_data() ->
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

  {ok, _, RowsOrders} = mysql:query(DbConn, ?QUERY_STR, [0, 1000]),
  {ok, _, RowsGalaxy} = mysql:query(DbConn, ?QUERY_STR, [0, 100000]),

  lists:foreach(
    fun(ElemTheOrder) ->
      [_IdTheOrder, NamaTheOrder, _] = ElemTheOrder,
      lists:foreach(
        fun(ElemGalaxy) ->
          [_IdGalaxy, NamaGalaxy, _] = ElemGalaxy,
          spawn(?MODULE, detect_duplicate, [[NamaTheOrder, NamaGalaxy]])
        end,
        RowsGalaxy
      )
    end,
    RowsOrders
  ),

  ok = mysql:stop(DbConn).
