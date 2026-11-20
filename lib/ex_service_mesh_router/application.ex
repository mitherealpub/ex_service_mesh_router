defmodule ExServiceMeshRouter.Application do
  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    start_gateway? = Application.get_env(:ex_service_mesh_router, :start_gateway, true)

    children =
      if start_gateway? do
        port = String.to_integer(System.get_env("GATEWAY_PORT") || "4000")
        topologies = Application.get_env(:libcluster, :topologies, [])

        Logger.info(
          "[ExServiceMeshRouter] Starting Bandit edge gateway on port #{port}..."
        )

        [
          {Cluster.Supervisor, [topologies, [name: ExServiceMeshRouter.ClusterSupervisor]]},
          {Router.Registry, name: Router.Registry},
          {Bandit, plug: Router.Router, port: port}
        ]
      else
        []
      end

    opts = [strategy: :one_for_one, name: ExServiceMeshRouter.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
