-module(oprex_worker_supervisor).
-behaviour(supervisor).

-export([start_link/1]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link({M,F,A}) ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, {M,F,A}).

init({M, F, A}) ->
  SupFlags = #{
    strategy => simple_one_for_one,
    intensity => 10,
    period => 10
  },
  ChildSpecs = [
    {oprex_worker_id, {M, F, A}, temporary, 5000, worker, [M]}
  ],
  {ok, {SupFlags, ChildSpecs}}.
