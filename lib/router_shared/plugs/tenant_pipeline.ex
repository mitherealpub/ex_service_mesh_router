defmodule Router.Shared.Plugs.TenantPipeline do
  import Plug.Conn
  alias Router.Shared.TenantResolver
  require Logger

  def init(opts), do: opts

  def call(conn, _opts) do
    host =
      conn
      |> get_req_header("host")
      |> List.first()

      |> Kernel.||(conn.host)

    case TenantResolver.resolve(host) do
      {:ok, tenant} ->
        conn
        |> assign(:tenant, tenant)
        |> maybe_put_session_tenant(tenant)
        |> maybe_put_ecto_tenant(tenant)
        |> maybe_put_ash_tenant(tenant)

      :error ->
        Logger.warning(
          "[Tenant Isolation Engine] Rejected request to unresolvable tenant host: #{inspect(host)}"
        )

        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          400,
          Jason.encode!(%{error: "Bad Request", message: "Missing or invalid tenant scope"})
        )
        |> halt()
    end
  end

  # Only writes to the session if `:fetch_session` has already run (allows reuse in `:api` pipelines)
  defp maybe_put_session_tenant(%{private: %{plug_session_fetch: :done}} = conn, tenant) do
    put_session(conn, "tenant", tenant)
  end

  defp maybe_put_session_tenant(conn, _tenant), do: conn

  defp maybe_put_ecto_tenant(conn, tenant) do
    if Application.get_env(:router_shared, :enable_ecto_tenant, false) do
      case Application.get_env(:router_shared, :ecto_repo) do
        nil ->
          Logger.error(
            "[Tenant Isolation Engine] Configuration error: :enable_ecto_tenant is true but :ecto_repo is missing."
          )

        repo ->
          _ = repo.put_tenant(tenant)
      end
    end

    conn
  end

  defp maybe_put_ash_tenant(conn, tenant) do
    if Application.get_env(:router_shared, :enable_ash_tenant, false) do
      if Code.ensure_loaded?(Ash) do
        _ = Ash.set_tenant(tenant)
      else
        Logger.error(
          "[Tenant Isolation Engine] Configuration error: :enable_ash_tenant is true but Ash is not loaded."
        )
      end
    end

    conn
  end
end
