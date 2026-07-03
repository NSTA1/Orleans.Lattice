using Microsoft.Extensions.Options;
using Orleans.Lattice;

namespace Orleans.Lattice.Benchmark.LeafCacheGrowth;

/// <summary>
/// Minimal <see cref="IOptionsMonitor{TOptions}"/> returning a fixed
/// <see cref="LatticeOptions"/> instance for every tree name. Hand-rolled
/// rather than an NSubstitute mock because the probe's steady-state read hot
/// path dereferences this monitor on every read (via
/// <c>LeafCacheGrain.GetCacheTtlAsync</c>); a real object keeps that path free
/// of per-call interception overhead so the reported read latency reflects the
/// cache's own cost, not the mock framework's.
/// </summary>
internal sealed class FixedOptionsMonitor(LatticeOptions options) : IOptionsMonitor<LatticeOptions>
{
    public LatticeOptions CurrentValue => options;

    public LatticeOptions Get(string? name) => options;

    public IDisposable? OnChange(Action<LatticeOptions, string?> listener) => NoopDisposable.Instance;

    private sealed class NoopDisposable : IDisposable
    {
        public static readonly NoopDisposable Instance = new();

        public void Dispose()
        {
        }
    }
}
