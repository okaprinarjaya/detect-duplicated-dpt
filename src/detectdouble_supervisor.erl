-module(detectdouble_supervisor).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
  MFA = {worker, start_link, []},
  SupFlags = #{
    strategy => one_for_all,
    intensity => 10,
    period => 10
  },
  ChildSpecs = [
    {order_manager_srv_id, {order_manager, start_link, []}, permanent, 5000, worker, [order_manager]},
    {worker_supervisor_id, {worker_supervisor, start_link, [MFA]}, temporary, 5000, supervisor, [worker_supervisor]}
  ],

  {ok, {SupFlags, ChildSpecs}}.
