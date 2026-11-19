defmodule Router.Registry do
  use GenServer
  require Logger

  @table :service_mesh_registry

  # --- Client API ---

  def start_link(opts) do
    GenServer.start_link(__MODULE__, %{}, name: opts[:name] || __MODULE__)
  end

  @doc """
  Lock-free ETS lookup returning `{upstream_address, pid}` or `nil`.
  """
  def resolve(service_host) do
    case :ets.lookup(@table, service_host) do
      [{^service_host, upstream_address, pid}] -> {upstream_address, pid}
      [] -> nil
    end
  end

  def register_service(host, upstream_address) do
    GenServer.call(__MODULE__, {:register, host, upstream_address})
  end

  # --- Server Callbacks ---

  @impl true
  def init(_) do
    Process.flag(:trap_exit, true)
    :ets.new(@table, [:named_table, :set, :protected, read_concurrency: true])
    send(self(), :discover_local_services)
    {:ok, %{}}
  end

  @impl true
  def handle_call({:register, host, upstream_address}, {from_pid, _}, state) do
    Logger.info(
      "[Service Mesh Cluster] Dynamic node registration received from #{inspect(node(from_pid))}: '#{host}' -> #{upstream_address}"
    )

    # Unlink previous worker PID if this host is being re-registered by a new process
    case Map.get(state, host) do
      {_old_addr, old_pid} when old_pid != self() and old_pid != from_pid ->
        Process.unlink(old_pid)

      _ ->
        :ok
    end

    if from_pid != self() do
      Process.link(from_pid)
    end

    :ets.insert(@table, {host, upstream_address, from_pid})
    updated_state = Map.put(state, host, {upstream_address, from_pid})
    {:reply, :ok, updated_state}
  end

  @impl true
  def handle_info(:discover_local_services, state) do
    local_state = fetch_local_service_mappings()

    Enum.each(local_state, fn {host, {upstream_url, pid}} ->
      :ets.insert(@table, {host, upstream_url, pid})
    end)

    {:noreply, Map.merge(state, local_state)}
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, state) do
    dead_node = node(pid)

    Logger.warning(
      "[Service Mesh Cluster] Node #{inspect(dead_node)} disconnected or crashed (Reason: #{inspect(reason)}). Pruning broken routes..."
    )

    cleaned_state =
      Enum.reduce(state, %{}, fn
        {host, {upstream_address, ^pid}}, acc ->
          :ets.delete(@table, host)

          Logger.info(
            "[Service Mesh Cluster] Deregistered route: '#{host}' -> #{upstream_address}"
          )

          acc

        {host, val}, acc ->
          Map.put(acc, host, val)
      end)

    {:noreply, cleaned_state}
  end

  # --- Internal Discovery Loops ---

  defp fetch_local_service_mappings do
    Application.loaded_applications()
    |> Enum.flat_map(fn {app, _, _} ->
      case Application.get_env(app, :endpoint) do
        nil ->
          []

        endpoint_module ->
          if Code.ensure_loaded?(endpoint_module) do
            http_config = endpoint_module.config(:http) || []
            port = Keyword.get(http_config, :port) || 4000

            url_config = endpoint_module.config(:url) || []
            host = Keyword.get(url_config, :host)

            if host do
              upstream_url = "127.0.0.1:#{port}"

              Logger.info(
                "[Service Mesh Discovery] Mapping local umbrella service host '#{host}' to: #{upstream_url}"
              )

              [{host, {upstream_url, self()}}]
            else
              []
            end
          else
            []
          end
      end
    end)
    |> Map.new()
  end
end
