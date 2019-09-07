%%%-------------------------------------------------------------------
%% @doc dist_procs_je_asane public API
%% @end
%%%-------------------------------------------------------------------

-module(dist_procs_je_asane_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
  oprex_supervisor:start_link().

stop(_State) ->
  ok.
