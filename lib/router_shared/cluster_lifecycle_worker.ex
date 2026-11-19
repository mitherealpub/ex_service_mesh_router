defmodule Router.Shared.ClusterLifecycleWorker do
  use GenServer
  require Logger

  @retry_interval_ms 5_000

  # --- Client API ---

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # --- Server Callbacks ---

  @impl true
  def init(opts) do
    gateway_node =
      Keyword.get(opts, :gateway_node) ||
        Application.get_env(:router_shared, :gateway_node, Node.self())

    # Watch cluster membership changes so routes re-register after any disconnect/restart
    if gateway_node != Node.self() do
      :net_kernel.monitor_nodes(true)
    end

    state = %{
      gateway_node: gateway_node,
      timer_ref: nil
    }

    send(self(), :announce_to_gateway)
    {:ok, state}
  end

  @impl true
  def handle_info(:announce_to_gateway, state) do
    state = cancel_retry_timer(state)

    result =
      cond do
        state.gateway_node == Node.self() ->
          register_locally()

        state.gateway_node in Node.list() ->
          register_remotely(state.gateway_node)

        true ->
          Logger.warning(
            "[Mesh Network] Gateway node #{inspect(state.gateway_node)} is not visible. Retrying in 5s..."
          )

          :retry
      end

    case result do
      :ok ->
        {:noreply, state}

      :retry ->
        timer_ref = Process.send_after(self(), :announce_to_gateway, @retry_interval_ms)
        {:noreply, %{state | timer_ref: timer_ref}}
    end
  end

  @impl true
  def handle_info({:nodeup, node}, %{gateway_node: node} = state) do
    Logger.info("[Mesh Network] Gateway node #{inspect(node)} reconnected. Re-announcing route...")
    send(self(), :announce_to_gateway)
    {:noreply, state}
  end

  @impl true
  def handle_info({:nodedown, node}, %{gateway_node: node} = state) do
    Logger.warning("[Mesh Network] Gateway node #{inspect(node)} disconnected. Will re-announce on reconnect.")
    {:noreply, state}
  end

  @impl true
  def handle_info(_other, state), do: {:noreply, state}

  # --- Internal Registration Helpers ---

  defp register_locally do
    case fetch_endpoint_details() do
      {:ok, host, upstream_address} ->
        if Process.whereis(Router.Registry) do
          _ = Router.Registry.register_service(host, upstream_address)
          Logger.info("[Mesh Network] Registered local service '#{host}' -> #{upstream_address}")
          :ok
        else
          :retry
        end

      :error ->
        :ok
    end
  end

  defp register_remotely(gateway_node) do
    case fetch_endpoint_details() do
      {:ok, host, upstream_address} ->
        case :rpc.call(gateway_node, Router.Registry, :register_service, [host, upstream_address]) do
          :ok ->
            Logger.info(
              "[Mesh Network] Announced route via RPC to #{inspect(gateway_node)}: '#{host}' -> #{upstream_address}"
            )

            :ok

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

        :ok
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
