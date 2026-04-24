using MultiSiteManufacturing.Host.Domain;

namespace MultiSiteManufacturing.Host.Federation;

/// <summary>
/// Common contract implemented by each federation backend. The router fans
/// every fact out to every backend; reads choose one backend by name.
/// </summary>
/// <remarks>
/// The sample ships two implementations — <c>BaselineFactBackend</c>
/// (arrival-order fold over an unreconciled append log) and
/// <c>LatticeFactBackend</c> (Orleans.Lattice fact log, HLC-ordered fold)
/// — so the dashboard can contrast their answers when concurrent writes or
/// chaos-induced reorder make them diverge.
/// </remarks>
public interface IFactBackend
{
    /// <summary>Stable short name used in divergence reports and diagnostics.</summary>
    string Name { get; }

    /// <summary>Ingests a single fact. Must be safe to invoke concurrently for distinct parts.</summary>
    Task EmitAsync(Fact fact, CancellationToken cancellationToken = default);

    /// <summary>Returns the current <see cref="ComplianceState"/> for <paramref name="serial"/>.</summary>
    Task<ComplianceState> GetStateAsync(PartSerialNumber serial, CancellationToken cancellationToken = default);

    /// <summary>Returns all facts recorded for the part, in the backend's native order.</summary>
    Task<IReadOnlyList<Fact>> GetFactsAsync(PartSerialNumber serial, CancellationToken cancellationToken = default);

    /// <summary>Enumerates every part this backend has observed at least one fact for.</summary>
    Task<IReadOnlyList<PartSerialNumber>> ListPartsAsync(CancellationToken cancellationToken = default);
}
