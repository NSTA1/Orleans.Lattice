using System;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Allocation-free <see cref="IOptionsMonitor{TOptions}"/> serving one fixed
/// value. It replaces the previous <c>Substitute.For&lt;IOptionsMonitor&lt;LatticeOptions&gt;&gt;()</c>
/// so the measured allocation profile reflects product code only. The resolver
/// reads <see cref="Get"/> on every <c>ResolveAsync</c> (i.e. once per write on
/// the saga hot path), so routing that through NSubstitute's dynamic-proxy
/// pipeline charged the mock framework's per-call overhead to BenchmarkDotNet's
/// <c>Allocated</c> figure. Returning a single cached instance allocates
/// nothing per call.
/// </summary>
/// <remarks>
/// Every benchmark configured the monitor with the same defaulted
/// <c>new LatticeOptions()</c>, so a single shared value is faithful; there is
/// no per-tree options divergence to preserve. <see cref="OnChange"/> returns
/// <see langword="null"/> (no change source), matching the prior substitute.
/// </remarks>
internal sealed class FakeOptionsMonitor<TOptions>(TOptions value) : IOptionsMonitor<TOptions>
{
    public TOptions CurrentValue { get; } = value;

    public TOptions Get(string? name) => CurrentValue;

    public IDisposable? OnChange(Action<TOptions, string?> listener) => null;
}
