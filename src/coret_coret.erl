-module(coret_coret).
-compile(export_all).

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
