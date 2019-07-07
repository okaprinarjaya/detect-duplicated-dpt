-module(coret_coret).
-compile(export_all).

get_worker_state(Pid) when is_pid(Pid) ->
  {ok, PropLists} = gen_server:call(Pid, get_state),
  PropLists.

populate_dpt_ids([], IdSeqs) ->
  IdSeqs;

populate_dpt_ids(Rows, IdSeqs) ->
  [Row | TailRows] = Rows,
  [Id, _Nama, _StatusDPT] = Row,
  populate_dpt_ids(TailRows, [Id | IdSeqs]).

length_worker_data(Pid) when is_pid(Pid) ->
  Rows = proplists:get_value(rows, get_worker_state(Pid)),
  length(populate_dpt_ids(Rows, [])).
