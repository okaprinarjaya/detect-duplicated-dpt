-module(oprex_supervisor).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
  SupFlags = #{
    strategy => one_for_all,
    intensity => 10,
    period => 10
  },
  ChildSpecs = [
    {srv, {oprex_order_manager, start_link, []}, permanent, 5000, worker, [oprex_order_manager]}
  ],
  {ok, {SupFlags, ChildSpecs}}.
