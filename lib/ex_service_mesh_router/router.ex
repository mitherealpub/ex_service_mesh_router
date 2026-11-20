defmodule Router.Router do
  import Plug.Conn
  alias Router.Registry
  alias Router.Shared.TenantResolver
  require Logger

  @preserve_host_header Application.compile_env(:ex_service_mesh_router, :preserve_host_header, true)


  def init(opts), do: opts

  # Built-in edge health check for Cloudflare Load Balancer & Docker healthchecks
  def call(%Plug.Conn{request_path: "/healthz"} = conn, _opts) do
    send_resp(conn, 200, "ok")
  end

  def call(conn, _opts) do
    conn =
      conn
      |> sanitize_public_request()
      |> append_forwarded_headers()

    service_host = determine_routing_target(conn)

    case Registry.resolve(service_host) do
      nil ->
        Logger.warning(
          "[Backplane Error] Unable to resolve route target for host key: #{inspect(service_host)}"
        )

        send_resp(conn, 404, "unknown service")

      {endpoint_url, _pid} ->
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
      is_binary(backplane_header) and backplane_header != "" ->
        normalize_host(backplane_header)

      is_binary(host_header) and host_header != "" ->
        resolve_public_host(host_header)

      true ->
        ""
    end
  end

  defp resolve_public_host(host) do
    if Registry.resolve(host) != nil do
      host
    else
      candidate_service = extract_service_host(host)

      if candidate_service != host and Registry.resolve(candidate_service) != nil do
        candidate_service
      else
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

  # Strip x-backplane-service if the request came from a non-internal IP OR passed through an edge proxy (Cloudflare/CDN)
  defp sanitize_public_request(conn) do
    proxied_from_edge? =
      get_req_header(conn, "cf-connecting-ip") != [] or
        get_req_header(conn, "x-forwarded-for") != []

    if not proxied_from_edge? and internal_remote_ip?(conn.remote_ip) do
      conn
    else
      delete_req_header(conn, "x-backplane-service")
    end
  end

  defp append_forwarded_headers(conn) do
    client_ip = conn.remote_ip |> :inet.ntoa() |> to_string()

    existing_xff = get_req_header(conn, "x-forwarded-for") |> List.first()
    new_xff = if existing_xff, do: "#{existing_xff}, #{client_ip}", else: client_ip

    proto =
      case get_req_header(conn, "x-forwarded-proto") do
        [p | _] -> p
        [] -> if(conn.scheme == :https, do: "https", else: "http")
      end

    conn
    |> put_req_header("x-forwarded-for", new_xff)
    |> put_req_header("x-forwarded-proto", proto)
  end

  # Unwrap IPv4-mapped IPv6 addresses emitted by Bandit dual-stack sockets
  defp internal_remote_ip?({0, 0, 0, 0, 0, 65535, hi, lo}) do
    internal_remote_ip?({hi >>> 8, hi &&& 0xFF, lo >>> 8, lo &&& 0xFF})
  end

  defp internal_remote_ip?({127, _, _, _}), do: true
  defp internal_remote_ip?({10, _, _, _}), do: true
  # Tailscale CGNAT mesh range (100.64.0.0/10)
  defp internal_remote_ip?({100, second, _, _}) when second in 64..127, do: true
  defp internal_remote_ip?({172, second, _, _}) when second in 16..31, do: true
  defp internal_remote_ip?({192, 168, _, _}), do: true
  defp internal_remote_ip?({0, 0, 0, 0, 0, 0, 0, 1}), do: true
  defp internal_remote_ip?(_), do: false

  defp websocket_upgrade?(conn) do
    conn
    |> get_req_header("upgrade")
    |> Enum.any?(&(String.downcase(&1) == "websocket"))
  end

   if @preserve_host_header do
    defp forward_http(conn, endpoint_upstream_url) do
      http_url = ensure_protocol(endpoint_upstream_url, "http://")
      ReverseProxyPlug.call(conn, upstream: http_url, preserve_host_header: true)
    end
  else
    defp forward_http(conn, endpoint_upstream_url) do
      http_url = ensure_protocol(endpoint_upstream_url, "http://")
      ReverseProxyPlug.call(conn, upstream: http_url)
    end
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
      default_protocol <> String.trim_leading(url, "/")
    end
  end

  defp normalize_host(nil), do: nil

  defp normalize_host(host) do
    host
    |> String.split(":")
    |> List.first()
    |> String.downcase()
    |> String.trim()
    |> String.trim_trailing(".")
  end
end
