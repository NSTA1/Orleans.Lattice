using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Domain;

namespace MultiSiteManufacturing.Host.Federation;

/// <summary>
/// Fans every emitted <see cref="Fact"/> out to every registered
/// <see cref="IFactBackend"/> in parallel. This is the single ingress
/// point for facts; every producer (bulk loader, operator UI, gRPC
/// client) goes through the router.
/// </summary>
/// <remarks>
/// M3 ships the plain fan-out router. M4 wires in per-site chaos grains so
/// the router can pause, delay, or reorder outgoing facts to model site
/// availability issues.
/// </remarks>
public sealed class FederationRouter(
    IEnumerable<IFactBackend> backends,
    ILogger<FederationRouter> logger)
{
    private readonly IReadOnlyList<IFactBackend> _backends = [.. backends];

    /// <summary>Backends that receive every emitted fact, indexed by <see cref="IFactBackend.Name"/>.</summary>
    public IReadOnlyList<IFactBackend> Backends => _backends;

    /// <summary>Returns the backend with the given name, or throws if no such backend is registered.</summary>
    public IFactBackend GetBackend(string name)
    {
        foreach (var backend in _backends)
        {
            if (string.Equals(backend.Name, name, StringComparison.Ordinal))
            {
                return backend;
            }
        }
        throw new InvalidOperationException($"No backend registered with name '{name}'.");
    }

    /// <summary>
    /// Fans <paramref name="fact"/> out to every backend in parallel and
    /// awaits them all. Throws an <see cref="AggregateException"/> if any
    /// backend fails.
    /// </summary>
    public async Task EmitAsync(Fact fact, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fact);

        logger.LogDebug(
            "Routing fact {FactId} for part {Serial} to {BackendCount} backends",
            fact.FactId, fact.Serial, _backends.Count);

        var tasks = new Task[_backends.Count];
        for (var i = 0; i < _backends.Count; i++)
        {
            tasks[i] = _backends[i].EmitAsync(fact, cancellationToken);
        }
        await Task.WhenAll(tasks);
    }
}
