defmodule Router.Shared.TenantResolver do
  @moduledoc """
  Resolves incoming HTTP Host headers to a tenant identifier, supporting both
  mapped custom domains (apex or subdomain) and standard tenant subdomains.
  """

  @spec resolve(String.t() | nil) :: {:ok, String.t()} | :error
  def resolve(nil), do: :error

  def resolve(host_header) when is_binary(host_header) do
    host =
      host_header
      |> String.split(":")
      |> List.first()
      |> String.downcase()
      |> String.trim()

    cond do
      host == "" ->
        :error

      ignored_host?(host) ->
        :error

      true ->
        case resolve_custom_domain(host) do
          {:ok, tenant} -> {:ok, tenant}
          :not_found -> resolve_subdomain(host)
        end
    end
  end

  defp resolve_custom_domain(host) do
    case Application.get_env(:router_shared, :custom_domain_resolver) do
      {mod, fun} -> apply(mod, fun, [host])
      fun when is_function(fun, 1) -> fun.(host)
      _ -> :not_found
    end
  end

  defp resolve_subdomain(host) do
    case String.split(host, ".") do
      [tenant, _domain, _tld | _rest] when tenant != "" ->
        if restricted_tenant?(tenant), do: :error, else: {:ok, tenant}

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
