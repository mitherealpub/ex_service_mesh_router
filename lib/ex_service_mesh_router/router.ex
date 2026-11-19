defmodule Router.Router do
  import Plug.Conn
  alias Router.Registry
  alias Router.Shared.TenantResolver
  require Logger

  def init(opts), do: opts

  def call(conn, _opts) do
    # Strip spoofed internal routing headers if the request came from the public internet
    conn = sanitize_public_request(conn)

    # 1. Identity Phase: Resolve service target across backplane, registered services, or custom tenant domains
    service_host = determine_routing_target(conn)

    case Registry.resolve(service_host) do
      nil ->
        Logger.warning(
          "[Backplane Error] Unable to resolve route target for path key: #{inspect(service_host)}"
        )

        send_resp(conn, 404, "unknown service: #{service_host}")

      {endpoint_url, _pid} ->
        # 2. Proxy Execution Phase: Check protocol type and pipe the network stream
        if websocket_upgrade?(conn) do
          forward_websocket(conn, endpoint_url)
        else
          forward_http(conn, endpoint_url)
        end
    end
  end

  # --- Routing Context Discovery Pipeline ---

  defp determine_routing_target(conn) do
    host_header =
      conn
      |> get_req_header("host")
      |> List.first()
      |> normalize_host()

    backplane_header =
      conn
      |> get_req_header("x-backplane-service")
      |> List.first()

    cond do
      # Only trusted internal requests will still have x-backplane-service after sanitize_public_request/1
      is_binary(backplane_header) and backplane_header != "" ->
        backplane_header

      is_binary(host_header) and host_header != "" ->
        resolve_public_host(host_header)

      true ->
        ""
    end
  end

  defp resolve_public_host(host) do
    # 1. Check if the full host itself is a registered service (e.g., "auth.mesh.local")
    if Registry.resolve(host) != nil do
      host
    else
      # 2. Check if stripping the leftmost subdomain matches a registered service (e.g., "tenant1.auth.mesh.local" -> "auth.mesh.local")
      candidate_service = extract_service_host(host)

      if candidate_service != host and Registry.resolve(candidate_service) != nil do
        candidate_service
      else
        # 3. Check if the host is a custom tenant domain (e.g., "clientcompany.com") or tenant subdomain
        case TenantResolver.resolve(host) do
          {:ok, _tenant_slug} ->
            Application.get_env(:ex_service_mesh_router, :default_tenant_upstream, "pms.mesh.local")

          :error ->
            candidate_service
        end
      end
    end
  end

  defp extract_service_host(nil), do: ""

  defp extract_service_host(host) do
    case String.split(host, ".") do
      [_tenant, service, tld] -> "#{service}.#{tld}"
      [_tenant | rest] when length(rest) >= 2 -> Enum.join(rest, ".")
      _ -> host
    end
  end

  # Only allow internal mesh headers from loopback or private VPC/VCN IPs (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16)
  defp sanitize_public_request(conn) do
    if internal_remote_ip?(conn.remote_ip) do
      conn
    else
      delete_req_header(conn, "x-backplane-service")
    end
  end

  defp internal_remote_ip?({127, _, _, _}), do: true
  defp internal_remote_ip?({10, _, _, _}), do: true
  defp internal_remote_ip?({172, second, _, _}) when second in 16..31, do: true
  defp internal_remote_ip?({192, 168, _, _}), do: true
  defp internal_remote_ip?({0, 0, 0, 0, 0, 0, 0, 1}), do: true
  defp internal_remote_ip?(_), do: false

  # Helper to identify WebSocket handshake requests
  defp websocket_upgrade?(conn) do
    upgrade_header =
      conn
      |> get_req_header("upgrade")
      |> List.first()

    case upgrade_header do
      nil -> false
      header -> String.downcase(header) == "websocket"
    end
  end

  defp forward_http(conn, endpoint_upstream_url) do
    http_url = ensure_protocol(endpoint_upstream_url, "http://")
    ReverseProxyPlug.call(conn, upstream: http_url)
  end

  defp forward_websocket(conn, endpoint_upstream_url) do
    ws_url = ensure_protocol(endpoint_upstream_url, "ws://")
    opts = ReverseProxyPlugWebsocket.init(upstream_uri: ws_url)
    ReverseProxyPlugWebsocket.call(conn, opts)
  end

  defp ensure_protocol(url, default_protocol) do
    if String.contains?(url, "://") do
      url
    else
      clean_url = String.trim_leading(url, "/")
      default_protocol <> clean_url
    end
  end

  defp normalize_host(nil), do: nil

  defp normalize_host(host) do
    host
    |> String.split(":")
    |> List.first()
    |> String.downcase()
    |> String.trim()
  end
end
