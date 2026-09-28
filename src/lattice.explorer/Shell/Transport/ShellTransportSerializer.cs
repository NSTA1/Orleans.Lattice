using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Explorer.Shell.Transport;

/// <summary>
/// The private Orleans serializer provider every gRPC client in the Shell builds
/// its wire marshallers from. The Explorer's application root has no Orleans
/// serialization registered, so the transport owns its own provider, exactly as
/// the retired plugin adapters did.
/// </summary>
/// <remarks>
/// It holds codecs only - no endpoint, credential or user state - so one instance
/// is shared by every circuit as a singleton. The plugin adapters built one per
/// adapter per circuit; sharing it removes that cost without widening what any
/// circuit can reach.
/// </remarks>
internal sealed class ShellTransportSerializer : IDisposable
{
    private readonly ServiceProvider _services = new ServiceCollection().AddSerializer().BuildServiceProvider();

    /// <summary>A service provider with Orleans serialization registered.</summary>
    public IServiceProvider Services => _services;

    /// <inheritdoc />
    public void Dispose() => _services.Dispose();
}
