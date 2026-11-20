defmodule Router.Shared.ClusterLifecycleWorker do
  use GenServer
  require Logger

  @retry_interval_ms 5_000
  @reconcile_interval_ms 15_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    gateway_node =
      Keyword.get(opts, :gateway_node) ||
        Application.get_env(:router_shared, :gateway_node, Node.self())

    if gateway_node != Node.self() do
      :net_kernel.monitor_nodes(true)
    end

    state = %{
      gateway_node: gateway_node,
      timer_ref: nil,
      registered_route: nil
    }

    send(self(), :announce_to_gateway)
    {:ok, state}
  end

  @impl true
  def handle_info(:announce_to_gateway, state) do
    state = cancel_retry_timer(state)

    case announce(state.gateway_node) do
      {:ok, route} ->
        # Periodic reconcile ensures route recovers even if Registry process restarts on gateway
        timer_ref = Process.send_after(self(), :announce_to_gateway, @reconcile_interval_ms)
        {:noreply, %{state | timer_ref: timer_ref, registered_route: route}}

      :noop ->
        {:noreply, state}

      :retry ->
        timer_ref = Process.send_after(self(), :announce_to_gateway, @retry_interval_ms)
        {:noreply, %{state | timer_ref: timer_ref}}
    end
  end

  @impl true
  def handle_info({:nodeup, node}, %{gateway_node: node} = state) do
    Logger.info("[Mesh Network] Gateway node #{inspect(node)} connected. Re-announcing route...")
    send(self(), :announce_to_gateway)
    {:noreply, state}
  end

  @impl true
  def handle_info({:nodedown, node}, %{gateway_node: node} = state) do
    Logger.warning("[Mesh Network] Gateway node #{inspect(node)} disconnected.")
    {:noreply, %{state | registered_route: nil}}
  end

  @impl true
  def handle_info(_other, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{gateway_node: gw, registered_route: {host, upstream_address}}) do
    if gw == Node.self() do
      if Process.whereis(Router.Registry),
        do: Router.Registry.deregister_service(host, upstream_address)
    else
      :rpc.call(gw, Router.Registry, :deregister_service, [host, upstream_address], 2_000)
    end

    :ok
  end

  def terminate(_reason, _state), do: :ok

  # --- Internal Registration Helpers ---

  defp announce(gateway_node) do
    cond do
      gateway_node == Node.self() ->
        register_locally()

      gateway_node in Node.list() ->
        register_remotely(gateway_node)

      true ->
        Logger.warning(
          "[Mesh Network] Gateway node #{inspect(gateway_node)} is not visible. Retrying in 5s..."
        )

        :retry
    end
  end

  defp register_locally do
    case fetch_endpoint_details() do
      {:ok, host, upstream_address} ->
        if Process.whereis(Router.Registry) do
          :ok = Router.Registry.register_service(host, upstream_address)
          {:ok, {host, upstream_address}}
        else
          :retry
        end

      :error ->
        :noop
    end
  end

  defp register_remotely(gateway_node) do
    case fetch_endpoint_details() do
      {:ok, host, upstream_address} ->
        case :rpc.call(gateway_node, Router.Registry, :register_service, [host, upstream_address], 5_000) do
          :ok ->
            {:ok, {host, upstream_address}}

          error ->
            Logger.error(
              "[Mesh Network] RPC registration failed (#{inspect(error)}). Retrying in 5s..."
            )

            :retry
        end

      :error ->
        Logger.error(
          "[Mesh Network] Cannot register service: local application endpoint configuration is missing."
        )

        :noop
    end
  end

  defp fetch_endpoint_details do
    app_name = Application.get_env(:router_shared, :app_name)

    case Application.get_env(app_name, :endpoint) do
      nil ->
        :error

      endpoint_module ->
        if Code.ensure_loaded?(endpoint_module) do
          url_config = endpoint_module.config(:url) || []
          host = Keyword.get(url_config, :host)

          http_config = endpoint_module.config(:http) || []
          port = Keyword.get(http_config, :port) || 4000

          local_ip = System.get_env("NODE_IP") || "127.0.0.1"

          if host do
            {:ok, host, "#{local_ip}:#{port}"}
          else
            :error
          end
        else
          :error
        end
    end
  end

  defp cancel_retry_timer(%{timer_ref: nil} = state), do: state

  defp cancel_retry_timer(%{timer_ref: ref} = state) do
    _ = Process.cancel_timer(ref)
    %{state | timer_ref: nil}
  end
end
