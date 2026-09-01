using NSubstitute;
using Orleans.Runtime;
using Orleans.Serialization;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Regression coverage for the enumerator-abort defect that stopped the
/// approximate index ever completing a build on a real deployment (#1844).
/// </summary>
/// <remarks>
/// <para>
/// WHAT WENT WRONG. <see cref="RepoContextVectorSource.CountAsync"/> walked the
/// repository's entire vector prefix through the RAW
/// <see cref="ILattice.KeysAsync"/> stream. That stream is documented to surface
/// <see cref="EnumerationAbortedException"/> when the remote enumerator is
/// reclaimed mid-scan (silo failover, cold start, idle expiry, scale-down), and
/// is marked <c>EditorBrowsable(Never)</c> precisely so callers reach for
/// <see cref="LatticeExtensions.ScanKeysAsync"/> instead. Walking the whole
/// prefix activates every leaf of the vector-metadata tree, which on a real
/// corpus takes long enough to outlive the enumerator.
/// </para>
/// <para>
/// WHY IT WAS SO EXPENSIVE. A build calls the count BEFORE it streams, so the
/// abort took down the entire index build - via the one call in it whose own
/// contract says the figure "sizes the index's initial reservation and reports
/// progress, and nothing depends on it for correctness". The build then retried
/// on the next query and aborted again, forever, so no index was ever persisted
/// and query cost stayed proportional to corpus size. Retrieval remained correct
/// throughout, because the exact scan answers while a build is in flight, which
/// is what made the failure silent.
/// </para>
/// <para>
/// These tests script the abort directly on a substituted tree, so the
/// resilience is proven without a cluster and without waiting for an enumerator
/// to age out. They fail against the raw-stream implementation.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextVectorSourceResilienceTests
{
    private const string RepoId = "acme";
    private static readonly string Prefix = RepoContextKeys.VectorsPrefix(RepoId);

    private static readonly Serializer Serializer = new ServiceCollection()
        .AddSerializer()
        .BuildServiceProvider()
        .GetRequiredService<Serializer>();

    private static readonly EmbeddingSpaceTag Space =
        new("test-model", 4, VectorNormalization.UnitL2);

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    [Test]
    public async Task CountAsync_recovers_from_a_mid_scan_enumerator_abort()
    {
        var callIndex = 0;
        var starts = new List<string?>();
        var tree = Substitute.For<ILattice>();
        tree.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                starts.Add(ci.ArgAt<string?>(0));
                var index = callIndex++;
                return index == 0
                    ? ScriptedKeys([Prefix + "vec-a", Prefix + "vec-b"], abortAfter: 2)
                    : ScriptedKeys([Prefix + "vec-c"], abortAfter: int.MaxValue);
            });

        var source = new RepoContextVectorSource(FactoryFor(tree), Serializer, RepoId, Space);

        var count = await source.CountAsync(Ct);

        Assert.Multiple(() =>
        {
            Assert.That(count, Is.EqualTo(3),
                "every key across the abort must be counted exactly once - no gap, no duplicate");
            Assert.That(callIndex, Is.EqualTo(2), "the aborted scan must be reopened exactly once");
            Assert.That(starts[1], Is.EqualTo(Prefix + "vec-b\u0000"),
                "the reopen must resume at the successor of the last counted key");
        });
    }

    [Test]
    public void CountAsync_does_not_surface_a_transient_abort_to_the_index_build()
    {
        var callIndex = 0;
        var tree = Substitute.For<ILattice>();
        tree.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ => callIndex++ == 0
                ? ScriptedKeys([Prefix + "vec-a"], abortAfter: 1)
                : ScriptedKeys([], abortAfter: int.MaxValue));

        var source = new RepoContextVectorSource(FactoryFor(tree), Serializer, RepoId, Space);

        // The whole point: an abort here must never reach the caller, because the
        // caller is the index build and it treats any exception as "the build
        // failed", discarding a build that was otherwise fine.
        Assert.DoesNotThrowAsync(async () => await source.CountAsync(Ct));
    }

    [Test]
    public void CountAsync_keeps_reconnecting_past_the_shared_default_budget()
    {
        // The shared default is eight reconnects, and that was measured to be too
        // small on a real corpus: every abort is a cold leaf activation outrunning
        // the enumerator's idle expiry, and a tree holding a whole repository's
        // vectors has far more than eight leaves to activate. Script more aborts
        // than the default allows and require the walk to survive them.
        var callIndex = 0;
        var abortsToScript = LatticeExtensions.DefaultScanReconnectAttempts + 4;
        var tree = Substitute.For<ILattice>();
        tree.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                var index = callIndex++;
                return index < abortsToScript
                    ? ScriptedKeys([$"{Prefix}vec-{index:D4}"], abortAfter: 1)
                    : ScriptedKeys([], abortAfter: int.MaxValue);
            });

        var source = new RepoContextVectorSource(FactoryFor(tree), Serializer, RepoId, Space);

        int? count = null;
        Assert.DoesNotThrowAsync(async () => count = await source.CountAsync(Ct));
        Assert.That(count, Is.EqualTo(abortsToScript),
            "every key yielded before an abort must still be counted exactly once");
    }

    [Test]
    public async Task CountAsync_counts_a_settled_prefix_without_reopening()
    {
        var callIndex = 0;
        var tree = Substitute.For<ILattice>();
        tree.KeysAsync(
            Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                callIndex++;
                return ScriptedKeys([Prefix + "vec-a", Prefix + "vec-b"], abortAfter: int.MaxValue);
            });

        var source = new RepoContextVectorSource(FactoryFor(tree), Serializer, RepoId, Space);

        var count = await source.CountAsync(Ct);

        Assert.Multiple(() =>
        {
            Assert.That(count, Is.EqualTo(2));
            Assert.That(callIndex, Is.EqualTo(1),
                "a scan that never aborts must cost exactly one enumeration, so resilience is not paid for twice");
        });
    }

    private static IGrainFactory FactoryFor(ILattice tree)
    {
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILattice>(Arg.Any<string>(), Arg.Any<string>()).Returns(tree);
        return factory;
    }

    private static async IAsyncEnumerable<string> ScriptedKeys(string[] keys, int abortAfter)
    {
        var yielded = 0;
        foreach (var key in keys)
        {
            if (yielded >= abortAfter) throw new EnumerationAbortedException();
            yielded++;
            yield return key;
            await Task.Yield();
        }

        if (yielded < abortAfter) yield break;
        throw new EnumerationAbortedException();
    }
}
