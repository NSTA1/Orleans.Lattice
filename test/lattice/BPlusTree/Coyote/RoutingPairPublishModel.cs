using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Which half of the publish rule a <see cref="RoutingPairPublishModel"/> run
/// removes.
/// </summary>
public enum RoutingPairPublishGuard
{
    /// <summary>The shipping rule.</summary>
    None,

    /// <summary>A resolve publishes whatever version it read, even over a newer published pair.</summary>
    NoVersionCheck,

    /// <summary>A resolve publishes even when an invalidation happened while it was in flight.</summary>
    NoEpochCheck,
}

/// <summary>
/// The assertions a <see cref="RoutingPairPublishModel"/> run checks.
/// </summary>
[Flags]
public enum RoutingPairPublishAssertions
{
    /// <summary>Check nothing.</summary>
    None = 0,

    /// <summary>A publish never replaces a pair with one read from an older registry row.</summary>
    NeverRegresses = 1,

    /// <summary>After an invalidation, nothing older than the registry row of that moment is published again.</summary>
    NoStaleRepublish = 2,

    /// <summary>Both assertions.</summary>
    All = NeverRegresses | NoStaleRepublish,
}

/// <summary>
/// A Coyote concurrency model of one stateless routing activation
/// (<c>LatticeGrain</c>) caching the (physical copy, map) pair it resolves from the
/// registry, while routing calls interleave on it (<c>GetRoutingSlowAsync</c>), the
/// registry moves on (an alias swap or a split re-versions the map), and a
/// stale-routing refusal invalidates the cache (<c>InvalidateShardMap</c> /
/// <c>TryInvalidateStaleAlias</c>).
/// <para>
/// Whether a resolve publishes is decided by the real
/// <see cref="RoutingPairPublishGate.ShouldPublish"/>. A resolve is two steps - it
/// reads the registry row when it starts and offers the pair when it finishes -
/// so the runtime can interleave another resolve, a registry change, or an
/// invalidation between them. This is the implementation-level counterpart of the
/// shard-ownership specification's routing assumption: the set of pairs a router
/// may hold only ever grows by what the registry published, and a router that a
/// copy refuses converges once it re-resolves (<c>RoutingConverges</c>).
/// </para>
/// </summary>
public sealed class RoutingPairPublishModel : ICoyoteModel
{
    private const int Resolvers = 2;
    private const int RegistryChanges = 2;

    private readonly RoutingPairPublishGuard _guard;
    private readonly RoutingPairPublishAssertions _assertions;

    /// <summary>Creates the model.</summary>
    public RoutingPairPublishModel(RoutingPairPublishGuard guard, RoutingPairPublishAssertions assertions = RoutingPairPublishAssertions.All)
    {
        _guard = guard;
        _assertions = assertions;
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        long registryVersion = 1;
        long epoch = 0;
        long? published = null;
        long floor = 0;

        var started = new bool[Resolvers];
        var finished = new bool[Resolvers];
        var readVersion = new long[Resolvers];
        var startEpoch = new long[Resolvers];
        var changes = 0;
        var invalidated = false;

        while (finished.Any(f => !f))
        {
            if (changes < RegistryChanges && runtime.RandomBoolean())
            {
                registryVersion++;
                changes++;
                continue;
            }

            if (!invalidated && published is { } cached && cached < registryVersion && runtime.RandomBoolean())
            {
                // A copy refused the stale pair: the activation drops it.
                epoch++;
                published = null;
                floor = registryVersion;
                invalidated = true;
                continue;
            }

            var acted = false;
            for (var r = 0; r < Resolvers && !acted; r++)
            {
                if (finished[r] || !runtime.RandomBoolean())
                {
                    continue;
                }

                Step(r);
                acted = true;
            }

            if (!acted)
            {
                // Every choice declined: advance the first unfinished resolve, so
                // each iteration makes progress and every schedule terminates.
                Step(Array.IndexOf(finished, false));
            }
        }

        void Step(int r)
        {
            if (!started[r])
            {
                started[r] = true;
                readVersion[r] = registryVersion;
                startEpoch[r] = epoch;
                return;
            }

            finished[r] = true;
            var publish = RoutingPairPublishGate.ShouldPublish(
                // NoEpochCheck pins the start epoch to the current one, so the gate can
                // never see an invalidation that happened while this resolve was in
                // flight: that is the guard's defect, not a scope limit.
                _guard == RoutingPairPublishGuard.NoEpochCheck ? epoch : startEpoch[r],
                epoch,
                // NoVersionCheck pins the published version to null (nothing published),
                // so the gate cannot refuse a regression: again the guard's defect.
                _guard == RoutingPairPublishGuard.NoVersionCheck ? null : published,
                readVersion[r]);
            if (!publish)
            {
                return;
            }

            if ((_assertions & RoutingPairPublishAssertions.NeverRegresses) != 0)
            {
                Specification.Assert(
                    published is not { } prior || prior <= readVersion[r],
                    $"a resolve of registry version {readVersion[r]} replaced the published version {published}");
            }

            if ((_assertions & RoutingPairPublishAssertions.NoStaleRepublish) != 0)
            {
                Specification.Assert(
                    readVersion[r] >= floor,
                    $"a resolve of registry version {readVersion[r]} was published after an invalidation at version {floor}");
            }

            published = readVersion[r];
        }
    }
}
