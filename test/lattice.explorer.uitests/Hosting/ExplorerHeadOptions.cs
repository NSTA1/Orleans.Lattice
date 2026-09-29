using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>What an <see cref="ExplorerHead"/> connects to and the hooks it is composed with.</summary>
internal sealed class ExplorerHeadOptions
{
    /// <summary>
    /// The cluster endpoint written to the head's configuration document, or
    /// <see langword="null"/> for a first-run head with no configuration at all.
    /// </summary>
    public string? Endpoint { get; init; }

    /// <summary>Whether a browser may save a connection (the first-run head needs it).</summary>
    public bool AllowInteractiveEndpointConfiguration { get; init; }

    /// <summary>Adds listeners beside the browser's HTTPS port, such as a gRPC port.</summary>
    public Action<KestrelServerOptions>? ConfigureKestrel { get; init; }

    /// <summary>Composes the host, for example with a co-hosted silo.</summary>
    public Action<WebApplicationBuilder>? ConfigureBuilder { get; init; }

    /// <summary>Registers services before the head, so they win its <c>TryAdd</c> registrations.</summary>
    public Action<IServiceCollection>? ConfigureServices { get; init; }

    /// <summary>Registers services after the head.</summary>
    public Action<IServiceCollection>? ConfigureServicesAfterHead { get; init; }

    /// <summary>Maps endpoints before the Explorer is mounted.</summary>
    public Action<WebApplication>? ConfigureApp { get; init; }
}
