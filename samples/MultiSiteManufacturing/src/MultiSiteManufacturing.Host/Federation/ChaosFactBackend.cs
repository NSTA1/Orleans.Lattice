using MultiSiteManufacturing.Host.Domain;

namespace MultiSiteManufacturing.Host.Federation;

/// <summary>
/// Decorator over an <see cref="IFactBackend"/> that consults the
/// per-backend <see cref="IBackendChaosGrain"/> on every emit and
/// applies jitter, transient failures, and write amplification as
/// configured.
/// </summary>
/// <remarks>
/// Plan §4.3 Tier 2. Read paths (<see cref="GetStateAsync"/>,
/// <see cref="GetFactsAsync"/>, <see cref="ListPartsAsync"/>) pass
/// straight through to the inner backend — chaos only affects writes,
/// mirroring real storage failure modes (partial writes, retry
/// amplification, and latency) without corrupting the read layer.
/// </remarks>
public sealed class ChaosFactBackend : IFactBackend
{
    private readonly IFactBackend _inner;
    private readonly IGrainFactory _grains;
    private readonly Func<Random> _rngFactory;

    /// <summary>Creates a decorator with a per-thread <see cref="Random"/>.</summary>
    public ChaosFactBackend(IFactBackend inner, IGrainFactory grains)
        : this(inner, grains, () => Random.Shared)
    {
    }

    /// <summary>Creates a decorator with a caller-supplied RNG factory (used by tests for determinism).</summary>
    public ChaosFactBackend(IFactBackend inner, IGrainFactory grains, Func<Random> rngFactory)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(grains);
        ArgumentNullException.ThrowIfNull(rngFactory);
        _inner = inner;
        _grains = grains;
        _rngFactory = rngFactory;
    }

    /// <inheritdoc />
    public string Name => _inner.Name;

    /// <summary>Access to the undecorated backend (useful for test assertions).</summary>
    public IFactBackend Inner => _inner;

    /// <inheritdoc />
    public async Task EmitAsync(Fact fact, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fact);

        var config = await ChaosGrain().GetConfigAsync();
        var rng = _rngFactory();

        if (config.JitterMsMax > 0)
        {
            var delay = rng.Next(config.JitterMsMin, config.JitterMsMax + 1);
            if (delay > 0)
            {
                await Task.Delay(delay, cancellationToken);
            }
        }

        if (config.TransientFailureRate > 0 && rng.NextDouble() < config.TransientFailureRate)
        {
            throw new ChaosTransientFailureException(
                $"Chaos: transient failure injected on backend '{Name}' for fact {fact.FactId}.");
        }

        await _inner.EmitAsync(fact, cancellationToken);

        if (config.WriteAmplificationRate > 0 && rng.NextDouble() < config.WriteAmplificationRate)
        {
            await _inner.EmitAsync(fact, cancellationToken);
        }
    }

    /// <inheritdoc />
    public Task<ComplianceState> GetStateAsync(PartSerialNumber serial, CancellationToken cancellationToken = default) =>
        _inner.GetStateAsync(serial, cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<Fact>> GetFactsAsync(PartSerialNumber serial, CancellationToken cancellationToken = default) =>
        _inner.GetFactsAsync(serial, cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<PartSerialNumber>> ListPartsAsync(CancellationToken cancellationToken = default) =>
        _inner.ListPartsAsync(cancellationToken);

    private IBackendChaosGrain ChaosGrain() =>
        _grains.GetGrain<IBackendChaosGrain>(_inner.Name);
}

/// <summary>Thrown by <see cref="ChaosFactBackend"/> when a transient-failure roll lands on a write.</summary>
public sealed class ChaosTransientFailureException : Exception
{
    /// <summary>Creates a new exception with the given message.</summary>
    public ChaosTransientFailureException(string message) : base(message) { }
}
