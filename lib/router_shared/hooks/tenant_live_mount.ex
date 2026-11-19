defmodule Router.Shared.Hooks.TenantLiveMount do
  import Phoenix.Component
  import Phoenix.LiveView
  alias Router.Shared.TenantResolver
  require Logger

  def on_mount(:default, _params, session, socket) do
    session_tenant = Map.get(session, "tenant")

    with true <- is_binary(session_tenant) and session_tenant != "",
         :ok <- verify_socket_host_matches_session(socket, session_tenant) do
      _ = maybe_put_ecto_tenant(session_tenant)
      _ = maybe_put_ash_tenant(session_tenant)

      {:cont, assign(socket, :tenant, session_tenant)}
    else
      _ ->
        Logger.warning(
          "[Tenant LiveView Mount Guard] Invalid or mismatched tenant context (#{inspect(session_tenant)}). Halting mount."
        )

        {:halt, redirect(socket, to: "/error-invalid-tenant")}
    end
  end

  # When `connect_info: [:uri]` is enabled on the socket, ensure the WebSocket URL host
  # resolves to the exact same tenant stored in the HTTP session cookie.
  defp verify_socket_host_matches_session(socket, session_tenant) do
    case get_connect_info(socket, :uri) do
      %URI{host: host} when is_binary(host) ->
        case TenantResolver.resolve(host) do
          {:ok, ^session_tenant} -> :ok
          _ -> :error
        end

      # Fallback if `:uri` is not configured in `connect_info`
      nil ->
        :ok
    end
  end

  defp maybe_put_ecto_tenant(tenant) do
    if Application.get_env(:router_shared, :enable_ecto_tenant, false) do
      case Application.get_env(:router_shared, :ecto_repo) do
        nil ->
          Logger.error(
            "[Tenant LiveView Mount Guard] Config error: :enable_ecto_tenant is true but :ecto_repo is missing."
          )

        repo ->
          repo.put_tenant(tenant)
      end
    end
  end

  defp maybe_put_ash_tenant(tenant) do
    if Application.get_env(:router_shared, :enable_ash_tenant, false) do
      if Code.ensure_loaded?(Ash) do
        Ash.set_tenant(tenant)
      else
        Logger.error(
          "[Tenant LiveView Mount Guard] Config error: :enable_ash_tenant is true but Ash is not loaded."
        )
      end
    end
  end
end
