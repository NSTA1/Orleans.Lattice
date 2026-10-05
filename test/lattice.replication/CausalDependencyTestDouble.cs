using NSubstitute;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Gives a substituted <see cref="IReplicationHighWaterMarkGrain"/> the
/// production dependency-check semantics of
/// <see cref="IReplicationHighWaterMarkGrain.CheckDependenciesAsync"/>: a
/// dependency on a write recorded as lost (#4603) is
/// <see cref="CausalDependencyVerdict.Lost"/>; otherwise a vector the harness's
/// local vector clock dominates is <see cref="CausalDependencyVerdict.Met"/>,
/// and anything else <see cref="CausalDependencyVerdict.Unmet"/>.
/// </summary>
internal static class CausalDependencyTestDouble
{
    /// <summary>
    /// Wires <see cref="IReplicationHighWaterMarkGrain.CheckDependenciesAsync"/>
    /// and <see cref="IReplicationHighWaterMarkGrain.RecordLostAsync"/> onto
    /// <paramref name="hwm"/>, reading the live <paramref name="localVc"/> on
    /// every call, and returns the lost set they share.
    /// </summary>
    public static HashSet<(string Origin, HybridLogicalClock Clock)> Wire(IReplicationHighWaterMarkGrain hwm, VersionVector localVc)
    {
        var lost = new HashSet<(string Origin, HybridLogicalClock Clock)>();

        hwm.RecordLostAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                lost.Add(((string)call[0], (HybridLogicalClock)call[1]));
                return Task.CompletedTask;
            });

        hwm.CheckDependenciesAsync(Arg.Any<IReadOnlyList<VersionVector>>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(Verdicts(localVc, lost, (IReadOnlyList<VersionVector>)call[0])));

        return lost;
    }

    /// <summary>Evaluates <paramref name="dependencies"/> as the grain does.</summary>
    public static CausalDependencyVerdict[] Verdicts(
        VersionVector localVc,
        IReadOnlySet<(string Origin, HybridLogicalClock Clock)> lost,
        IReadOnlyList<VersionVector> dependencies)
    {
        var verdicts = new CausalDependencyVerdict[dependencies.Count];
        for (var i = 0; i < dependencies.Count; i++)
        {
            var verdict = CausalDependencyVerdict.Met;
            foreach (var (origin, required) in dependencies[i].Entries)
            {
                if (lost.Contains((origin, required)))
                {
                    verdict = CausalDependencyVerdict.Lost;
                    break;
                }

                if (localVc.GetClock(origin) < required)
                {
                    verdict = CausalDependencyVerdict.Unmet;
                }
            }

            verdicts[i] = verdict;
        }

        return verdicts;
    }
}
