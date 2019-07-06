%%%-------------------------------------------------------------------
%% @doc dist_procs_je_asane public API
%% @end
%%%-------------------------------------------------------------------

-module(dist_procs_je_asane_app).

-behaviour(application).

-export([start/2, stop/1]).
-export([justquery/2, fired/1, dbclose/1]).

start(_StartType, _StartArgs) ->
  dist_procs_je_asane_sup:start_link().

stop(_State) ->
  ok.

%% internal functions

justquery(PoolName, Sql) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {justquery, Sql})
  end).

fired(WorkerPid) ->
  gen_server:cast(WorkerPid, fired).

dbclose(WorkerPid) ->
  gen_server:call(WorkerPid, dbclose).
