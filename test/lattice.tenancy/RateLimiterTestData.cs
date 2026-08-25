using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Deterministic helpers for the request-rate limiter tests: a manually-advanced
/// <see cref="TimeProvider"/> (so token-bucket refill is driven by logical time,
/// never a sleep or the wall clock), a tenant-record builder that carries a
/// configured rate, and in-memory fakes for the budget coordinator's
/// collaborators.
/// </summary>
internal static class RateLimiterTestData
{
    /// <summary>
    /// A representative timestamp frequency: ten million ticks per second, matching
    /// the 100-nanosecond tick the rest of the tenancy tests reason in.
    /// </summary>
    internal const long Frequency = 10_000_000;

    /// <summary>
    /// A large positive starting timestamp, so a fresh bucket (whose theoretical
    /// arrival time starts at zero) does not see "now" below its allow-at floor.
    /// </summary>
    internal const long StartTimestamp = 1_000_000_000_000;

    /// <summary>
    /// Builds an active tenant record whose quota carries the given sustained rate
    /// and burst allowance.
    /// </summary>
    /// <param name="tenantId">The tenant id text.</param>
    /// <param name="maxOpsPerSecond">The configured operations-per-second ceiling, or <c>null</c> for unbounded.</param>
    /// <param name="burstPercent">The burst allowance percentage.</param>
    /// <param name="status">The tenant status (defaults to active).</param>
    /// <returns>The built tenant record.</returns>
    internal static TenantRecord RecordWithRate(
        string tenantId,
        long? maxOpsPerSecond,
        int burstPercent = 0,
        TenantStatus status = TenantStatus.Active)
    {
        var quotas = new TenantQuotas
        {
            MaxOpsPerSecond = maxOpsPerSecond,
            BurstPercent = burstPercent,
        };

        return TenantRecord.Create(
            TenantId.Parse(tenantId),
            status,
            quotas,
            TenantPlacement.Shared,
            Clock(1),
            "test");
    }

    /// <summary>A <see cref="TimeProvider"/> whose timestamp is set explicitly by the test.</summary>
    internal sealed class ManualTimeProvider : TimeProvider
    {
        private long _timestamp;

        /// <summary>Initializes the provider at <see cref="StartTimestamp"/>.</summary>
        public ManualTimeProvider() => _timestamp = StartTimestamp;

        /// <inheritdoc />
        public override long TimestampFrequency => Frequency;

        /// <inheritdoc />
        public override long GetTimestamp() => Volatile.Read(ref _timestamp);

        /// <summary>Advances the timestamp by the given number of ticks.</summary>
        /// <param name="ticks">The number of ticks to advance.</param>
        public void Advance(long ticks) => Volatile.Write(ref _timestamp, _timestamp + ticks);

        /// <summary>Advances the timestamp by the given number of whole seconds.</summary>
        /// <param name="seconds">The number of seconds to advance.</param>
        public void AdvanceSeconds(long seconds) => Advance(seconds * Frequency);
    }

    /// <summary>An <see cref="ITenantRateProvider"/> serving a fixed list of specs.</summary>
    internal sealed class FakeRateProvider(params TenantRateSpec[] specs) : ITenantRateProvider
    {
        private readonly TenantRateSpec[] _specs = specs;

#pragma warning disable CS1998 // synchronous fake enumerator
        public async IAsyncEnumerable<TenantRateSpec> GetConfiguredRatesAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var spec in _specs)
            {
                yield return spec;
            }
        }
#pragma warning restore CS1998
    }

    /// <summary>An <see cref="ILiveSiloCountProvider"/> returning a fixed count.</summary>
    internal sealed class FakeSiloCountProvider(int count) : ILiveSiloCountProvider
    {
        public ValueTask<int> GetLiveSiloCountAsync(CancellationToken cancellationToken = default) => new(count);
    }

    /// <summary>
    /// An <see cref="ITenantClusterDemandExchange"/> returning a fixed cluster total
    /// (or <c>null</c> to force the static-even fallback) and recording the local
    /// demand it was handed.
    /// </summary>
    internal sealed class FakeDemandExchange(long? clusterTotal) : ITenantClusterDemandExchange
    {
        /// <summary>The most recent local demand passed to <see cref="ExchangeAsync"/>.</summary>
        public long LastLocalDemand { get; private set; }

        public ValueTask<long?> ExchangeAsync(TenantId tenant, long localDemand, CancellationToken cancellationToken = default)
        {
            LastLocalDemand = localDemand;
            return new(clusterTotal);
        }
    }
}
