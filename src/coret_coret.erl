-module(coret_coret).
-compile(export_all).

-define(WORKER_ARGS, [{dbhost, "localhost"}, {dbname, "dpt_in_da_house"}, {dbuser, "mamp"}, {dbpasswd, "12qwaszx@321"}]).

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
  {length(Data), Data}.

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

  {ok, _, Rows} = mysql:query(DbConn, <<"SELECT id, nama, status_dpt FROM dpt_pemilihbali LIMIT 0, 160">>),
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

  {ok, _, Rows} = mysql:query(DbConn, <<"SELECT id, nama, status_dpt FROM dpt_pemilihbali LIMIT 0, 5">>),
  ok = mysql:stop(DbConn),
  Rows.

test_working() ->
  RowsTheOrders = test_get_the_orders(),
  RowsWorkerData = test_get_worker_data(),

  lists:foreach(
    fun(ElemTheOrder) ->
      [IdTheOrder, NamaTheOrder, _] = ElemTheOrder,
      lists:foreach(
        fun(ElemGalaxy) ->
          [IdGalaxy, NamaGalaxy, _] = ElemGalaxy,

          if IdTheOrder =/= IdGalaxy ->
            NamaTheOrderNew = binary_to_list(NamaTheOrder),
            NamaGalaxyNew = binary_to_list(NamaGalaxy),
            LcsLen = length(lcs:lcs(NamaTheOrderNew, NamaGalaxyNew)),
            Blen = length(NamaGalaxyNew),
            Similarity = (LcsLen / Blen) * 100,

            if Similarity > 90 ->
              io:format("Double data detected! - ~p~n", [NamaTheOrderNew]);
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
