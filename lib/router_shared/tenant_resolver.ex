defmodule Router.Shared.TenantResolver do
  @moduledoc """
  Resolves incoming HTTP Host headers to a validated tenant identifier, supporting
  both mapped custom domains (apex or subdomain) and standard tenant subdomains.
  """

  require Logger

  # RFC-1123 compliant DNS label (1-63 lowercase alphanumeric chars or hyphens, no leading/trailing hyphen)
  @valid_slug_regex ~r/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/

  @spec resolve(String.t() | nil) :: {:ok, String.t()} | :error
  def resolve(nil), do: :error

  def resolve(host_header) when is_binary(host_header) do
    host =
      host_header
      |> String.split(":")
      |> List.first()
      |> String.downcase()
      |> String.trim()
      |> String.trim_trailing(".")

    cond do
      host == "" ->
        :error

      ignored_host?(host) ->
        :error

      true ->
        case resolve_custom_domain(host) do
          {:ok, tenant} when is_binary(tenant) and tenant != "" -> {:ok, tenant}
          _ -> resolve_subdomain(host)
        end
    end
  end

  defp resolve_custom_domain(host) do
    try do
      case Application.get_env(:router_shared, :custom_domain_resolver) do
        {mod, fun} -> apply(mod, fun, [host])
        fun when is_function(fun, 1) -> fun.(host)
        _ -> :not_found
      end
    rescue
      exception ->
        Logger.error(
          "[TenantResolver] Custom domain lookup failed for #{inspect(host)}: #{Exception.message(exception)}"
        )

        :not_found
    end
  end

  defp resolve_subdomain(host) do
    case String.split(host, ".") do
      [tenant, _domain, _tld | _rest] when tenant != "" ->
        if Regex.match?(@valid_slug_regex, tenant) and not restricted_tenant?(tenant) do
          {:ok, tenant}
        else
          :error
        end

      _ ->
        :error
    end
  end

  defp ignored_host?(host) do
    patterns = Application.get_env(:router_shared, :ignored_host_patterns, [])
    Enum.any?(patterns, &Regex.match?(&1, host))
  end

  defp restricted_tenant?(tenant) do
    patterns = Application.get_env(:router_shared, :restricted_tenant_patterns, [])
    Enum.any?(patterns, &Regex.match?(&1, tenant))
  end
end
