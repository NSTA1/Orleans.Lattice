using System.Collections.Concurrent;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;

namespace MultiSiteManufacturing.Host.Baseline;

/// <summary>
/// Baseline backend — delegates per-part state to <see cref="IBaselinePartGrain"/>
/// and keeps an in-process directory of known parts so <see cref="ListPartsAsync"/>
/// doesn't require a query-by-convention over grain storage.
/// </summary>
/// <remarks>
/// <para>
/// The in-process directory is rebuilt on every host start (facts replayed
/// through <see cref="EmitAsync"/> repopulate it). Grain state itself is
/// durable — the directory is a cache, not the source of truth.
/// </para>
/// <para>
/// On peer clusters, <see cref="EmitAsync"/> is also called from the
/// inbound replication endpoint (<c>ReplicationInboundEndpoint</c>) for
/// every replicated <c>mfg-facts</c> Set entry. This models naive
/// event-log replication for the baseline backend so a peer's baseline
/// view matches the seeding cluster on cold start. Deletes are not
/// replayed — the baseline is an append-only fold with no retraction
/// concept.
/// </para>
/// </remarks>
public sealed class BaselineFactBackend(IGrainFactory grainFactory) : IFactBackend
{
    private readonly ConcurrentDictionary<PartSerialNumber, byte> _knownParts = new();

    /// <inheritdoc />
    public string Name => "baseline";

    /// <inheritdoc />
    public async Task EmitAsync(Fact fact, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fact);
        _knownParts.TryAdd(fact.Serial, 0);
        var grain = grainFactory.GetGrain<IBaselinePartGrain>(fact.Serial.Value);
        await grain.AppendAsync(fact);
    }

    /// <inheritdoc />
    public Task<ComplianceState> GetStateAsync(PartSerialNumber serial, CancellationToken cancellationToken = default)
    {
        var grain = grainFactory.GetGrain<IBaselinePartGrain>(serial.Value);
        return grain.GetStateAsync();
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<Fact>> GetFactsAsync(PartSerialNumber serial, CancellationToken cancellationToken = default)
    {
        var grain = grainFactory.GetGrain<IBaselinePartGrain>(serial.Value);
        return grain.GetFactsAsync();
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<PartSerialNumber>> ListPartsAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyList<PartSerialNumber>>([.. _knownParts.Keys]);
}
