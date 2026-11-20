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
  Prefers local node upstreams when available; otherwise load-balances across healthy peers.
  """
  def resolve(service_host) do
    case :ets.lookup(@table, service_host) do
      [{^service_host, []}] ->
        nil

      [{^service_host, [single]}] ->
        single

      [{^service_host, upstreams}] when is_list(upstreams) ->
        select_upstream(upstreams)

      [] ->
        nil
    end
  end

  @doc """
  Returns all registered upstreams for `service_host` (useful for health/admin inspection).
  """
  def list_upstreams(service_host) do
    case :ets.lookup(@table, service_host) do
      [{^service_host, upstreams}] -> upstreams
      [] -> []
    end
  end

  def register_service(host, upstream_address) do
    GenServer.call(__MODULE__, {:register, host, upstream_address})
  end

  def deregister_service(host, upstream_address) do
    GenServer.call(__MODULE__, {:deregister, host, upstream_address})
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
    existing = Map.get(state, host, [])

    # Replace any stale entry with the same upstream_address or from_pid
    {stale, kept} =
      Enum.split_with(existing, fn {addr, pid} ->
        addr == upstream_address or pid == from_pid
      end)

    Enum.each(stale, fn {_addr, old_pid} ->
      if old_pid != self() and old_pid != from_pid do
        Process.unlink(old_pid)
      end
    end)

    if from_pid != self() do
      Process.link(from_pid)
    end

    updated_upstreams = [{upstream_address, from_pid} | kept]
    :ets.insert(@table, {host, updated_upstreams})

    Logger.info(
      "[Service Mesh Cluster] Registered '#{host}' -> #{upstream_address} from #{inspect(node(from_pid))} (pool size: #{length(updated_upstreams)})"
    )

    {:reply, :ok, Map.put(state, host, updated_upstreams)}
  end

  @impl true
  def handle_call({:deregister, host, upstream_address}, {from_pid, _}, state) do
    existing = Map.get(state, host, [])

    updated_upstreams =
      Enum.reject(existing, fn {addr, pid} ->
        addr == upstream_address and pid == from_pid
      end)

    if from_pid != self() and not pid_referenced_anywhere?(state, host, from_pid) do
      Process.unlink(from_pid)
    end

    sync_ets(host, updated_upstreams)
    Logger.info("[Service Mesh Cluster] Gracefully deregistered '#{host}' -> #{upstream_address}")

    {:reply, :ok, put_or_delete(state, host, updated_upstreams)}
  end

  @impl true
  def handle_info(:discover_local_services, state) do
    local_state = fetch_local_service_mappings()

    merged_state =
      Enum.reduce(local_state, state, fn {host, new_upstreams}, acc ->
        existing = Map.get(acc, host, [])
        combined = Enum.uniq_by(new_upstreams ++ existing, fn {addr, _pid} -> addr end)
        :ets.insert(@table, {host, combined})
        Map.put(acc, host, combined)
      end)

    {:noreply, merged_state}
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, state) do
    dead_node = node(pid)

    Logger.warning(
      "[Service Mesh Cluster] Worker on #{inspect(dead_node)} exited (#{inspect(reason)}). Pruning routes..."
    )

    cleaned_state =
      Enum.reduce(state, %{}, fn {host, upstreams}, acc ->
        {pruned, remaining} = Enum.split_with(upstreams, fn {_addr, p} -> p == pid end)

        Enum.each(pruned, fn {addr, _} ->
          Logger.info("[Service Mesh Cluster] Deregistered dead route: '#{host}' -> #{addr}")
        end)

        sync_ets(host, remaining)
        put_or_delete(acc, host, remaining)
      end)

    {:noreply, cleaned_state}
  end

  # --- Internal Helpers ---

  defp select_upstream(upstreams) do
    local_ip = System.get_env("NODE_IP") || "127.0.0.1"

    local_matches =
      Enum.filter(upstreams, fn {addr, _pid} ->
        String.starts_with?(addr, "127.0.0.1:") or String.starts_with?(addr, "#{local_ip}:")
      end)

    pool = if local_matches != [], do: local_matches, else: upstreams
    Enum.at(pool, :erlang.phash2(self(), length(pool)))
  end

  defp sync_ets(host, []), do: :ets.delete(@table, host)
  defp sync_ets(host, upstreams), do: :ets.insert(@table, {host, upstreams})

  defp put_or_delete(map, key, []), do: Map.delete(map, key)
  defp put_or_delete(map, key, val), do: Map.put(map, key, val)

  defp pid_referenced_anywhere?(state, skip_host, target_pid) do
    Enum.any?(state, fn
      {^skip_host, _} -> false
      {_host, upstreams} -> Enum.any?(upstreams, fn {_addr, pid} -> pid == target_pid end)
    end)
  end

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
              [{host, [{"127.0.0.1:#{port}", self()}]}]
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
