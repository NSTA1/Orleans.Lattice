using Deque.AxeCore.Commons;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>A checkpoint's timed, bounded axe run (#5565).</summary>
/// <param name="Result">The axe result, or <see langword="null"/> when the run never finished.</param>
/// <param name="Elapsed">How long the checkpoint waited for it.</param>
/// <param name="WithinBound">Whether it finished within the bound.</param>
/// <param name="Rules">Each rule's time run alone, slowest first; empty unless the run was slow.</param>
/// <param name="Failure">Why the checkpoint fails on the bound, or <see langword="null"/>.</param>
internal sealed record AxeTimedRun(AxeResult? Result, TimeSpan Elapsed, bool WithinBound, IReadOnlyList<AxeRuleTiming> Rules, string? Failure);
