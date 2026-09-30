using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The service key the Explorer's credential-aware facades are registered under,
/// and the one way the Explorer resolves them.
/// </summary>
/// <remarks>
/// <para>
/// The Explorer's facades call the cluster as the circuit's signed-in user over
/// the configured endpoint. A host that co-hosts the Explorer beside the cluster's
/// own gRPC services (a silo that also serves the console, as the Explorer sample
/// does) registers the <em>in-process</em> server facades under the same
/// interfaces. Those carry no caller credential: resolved by interface, the
/// Explorer would call them as nobody - or, for a facade that does not authorise
/// its caller, with the host's own authority, a confused deputy. Resolved the
/// other way round, the server's gRPC services would call back into the Explorer's
/// adapters and so into themselves.
/// </para>
/// <para>
/// So the Explorer's facades are keyed services under <see cref="Key"/> and never
/// registered by bare interface: the host's registrations and the Explorer's
/// cannot see each other, whichever order they are added in.
/// </para>
/// </remarks>
internal static class ShellFacades
{
    /// <summary>The service key of every Explorer facade.</summary>
    public const string Key = "Orleans.Lattice.Explorer.UI.Transport";

    /// <summary>Resolves the Explorer's own <typeparamref name="TFacade"/>, or <see langword="null"/> when the head serves none.</summary>
    /// <typeparam name="TFacade">The facade interface.</typeparam>
    /// <param name="services">The circuit's services.</param>
    /// <returns>The Explorer's facade, never a host's in-process one.</returns>
    public static TFacade? GetShellFacade<TFacade>(this IServiceProvider services)
        where TFacade : class =>
        services.GetKeyedService<TFacade>(Key);

    /// <summary>Resolves the Explorer's own <typeparamref name="TFacade"/>.</summary>
    /// <typeparam name="TFacade">The facade interface.</typeparam>
    /// <param name="services">The circuit's services.</param>
    /// <returns>The Explorer's facade, never a host's in-process one.</returns>
    /// <exception cref="InvalidOperationException">The head serves no such facade.</exception>
    public static TFacade GetRequiredShellFacade<TFacade>(this IServiceProvider services)
        where TFacade : class =>
        services.GetRequiredKeyedService<TFacade>(Key);
}
