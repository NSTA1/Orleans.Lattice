using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>An in-memory <see cref="IAppActivationStatusStore"/>.</summary>
internal sealed class InMemoryActivationStatusStore : IAppActivationStatusStore
{
    private readonly Dictionary<string, AppActivationStatus> _statuses = new(StringComparer.Ordinal);

    public bool FailWrites { get; set; }

    public bool FailReads { get; set; }

    public int Writes { get; private set; }

    public Task<AppActivationStatus?> GetAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken) =>
        FailReads
            ? throw new InvalidOperationException("status store unreadable")
            : Task.FromResult(_statuses.TryGetValue(AppRegistryTreeNames.ComposeKey(tenant, slug), out var status) ? status : null);

    public Task SetAsync(AppActivationStatus status, CancellationToken cancellationToken)
    {
        if (FailWrites)
            throw new InvalidOperationException("status store unavailable");
        Writes++;
        _statuses[AppRegistryTreeNames.ComposeKey(status.Tenant, status.Slug)] = status;
        return Task.CompletedTask;
    }
}
