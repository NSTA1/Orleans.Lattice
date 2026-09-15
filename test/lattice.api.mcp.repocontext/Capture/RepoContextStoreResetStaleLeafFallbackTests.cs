using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Tests for the whole-tree fallback <c>repocontext_reset_index</c> takes when a
/// code-index tree's subtree cannot be enumerated at all.
/// <para>
/// The reset's sweep is a range delete, and a range delete enumerates. Enumeration
/// activates leaves, so on a tree holding a leaf whose durable projection
/// checkpoint was trimmed with no covering snapshot - the state that surfaces as
/// <see cref="LeafProjectionStaleException"/> - the sweep cannot advance a single
/// step. That is precisely the damage an operator invokes a reset to escape, so
/// without a fallback the one verb that exists to recover a wedged index is
/// disabled by the wedge. <see cref="ILattice.DeleteTreeAsync"/> is the only
/// public primitive that makes progress there, because it marks shard roots
/// deleted through shard-root state alone and never activates the throwing leaf.
/// </para>
/// <para>
/// It drops the <b>whole</b> tree, though, and the context trees are shared by
/// every registered repository - so the interesting behaviour is not that the
/// fallback exists but that it is fail-closed: refused on a shared tree, refused
/// unconditionally on the structural tree, and taken only when this repository is
/// demonstrably the sole registered one.
/// </para>
/// </summary>
/// <remarks>
/// <para>
/// Marked <c>Integration</c>: a real silo backs every tree except the one under
/// test, so the registration census the fallback consults is derived from real
/// seeded markers through the real key-cursor machinery rather than from a
/// hand-mocked listing. That matters here - the census is the whole safety
/// condition, and a mocked one would prove only that the code reads a stub.
/// </para>
/// <para>
/// The faulting tree itself has to be a substitute: the fault under test is a leaf
/// that cannot be activated, which the in-memory harness silo has no way to produce
/// on demand. Injecting it at <see cref="ILattice.OpenDeleteRangeCursorAsync"/> is
/// faithful to where the real fault surfaces, because that call is what opens the
/// enumeration and therefore what activates the leaf.
/// </para>
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextStoreResetStaleLeafFallbackTests
{
    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static async Task SeedMarkerAsync(RepoContextMcpHarness harness, string repoId, CancellationToken ct)
    {
        var serializer = harness.Services.GetRequiredService<Serializer<RepoNode>>();
        var bytes = serializer.SerializeToArray(new RepoNode { RepoId = repoId });
        await harness.GrainFactory
            .GetGrain<ILattice>(RepoContextTrees.Structural)
            .SetAsync(RepoContextKeys.Repo(repoId), bytes, ct);
    }

    /// <summary>
    /// Builds a store over the harness's real grains, substituting exactly one tree
    /// whose delete-cursor open throws the terminal stale-leaf fault. Every other
    /// tree, and every control-plane grain the reset drives, is the real one.
    /// </summary>
    private static (RepoContextStore Store, ILattice Faulting) StoreWithStaleTree(
        RepoContextMcpHarness harness, string staleTree)
    {
        var real = harness.GrainFactory;

        var faulting = Substitute.For<ILattice>();
        faulting.OpenDeleteRangeCursorAsync(default!, default!, default)
            .ThrowsAsyncForAnyArgs(new LeafProjectionStaleException(
                "simulated terminal stale leaf projection on '" + staleTree + "'"));

        // NSubstitute auto-returns an empty array rather than null for a byte[]
        // result, which the marker read would then try to deserialize. Return the
        // absent-record null explicitly so the substitute behaves like a tree with
        // no marker rather than one holding a zero-length record.
        faulting.GetAsync(default!, default).ReturnsForAnyArgs(Task.FromResult<byte[]?>(null));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILattice>(Arg.Any<string>()).Returns(call =>
        {
            var name = call.ArgAt<string>(0);
            return string.Equals(name, staleTree, StringComparison.Ordinal)
                ? faulting
                : real.GetGrain<ILattice>(name);
        });
        factory.GetGrain<IRepoIndexJobGrain>(Arg.Any<string>())
            .Returns(call => real.GetGrain<IRepoIndexJobGrain>(call.ArgAt<string>(0)));
        factory.GetGrain<IRepoContextSelfIndexGrain>(Arg.Any<string>())
            .Returns(call => real.GetGrain<IRepoContextSelfIndexGrain>(call.ArgAt<string>(0)));

        var store = new RepoContextStore(
            factory,
            harness.Services.GetRequiredService<IRepoIndexRunner>(),
            harness.Services.GetRequiredService<Serializer>(),
            harness.Services.GetRequiredService<RepoContextVectorWriter>(),
            harness.Services.GetRequiredService<IOptionsMonitor<RepoContextTtlOptions>>(),
            TimeProvider.System);

        return (store, faulting);
    }

    /// <summary>
    /// The unblock itself. A vector-index tree whose leaf cannot be activated must
    /// not sink the reset: with this repository the only registered one, the
    /// whole-tree drop is exactly equivalent to the requested subtree delete, so
    /// the sweep takes it and the reset completes.
    /// </summary>
    [Test]
    public async Task Reset_drops_a_terminally_stale_tree_whole_when_the_repository_is_the_only_one()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await SeedMarkerAsync(harness, "acme", Ct);

        var (store, faulting) = StoreWithStaleTree(harness, RepoContextTrees.VectorIndex);

        var result = await store.ResetIndexAsync("acme", Ct);

        await faulting.Received(1).DeleteTreeAsync(Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(result.MemoryPreserved, Is.True,
                "The fallback is scoped to code-index trees; the memory tree is never dropped.");
            Assert.That(result.TreesSwept, Has.Count.EqualTo(RepoContextTrees.CodeIndexTrees.Count),
                "Every code-index tree must still be accounted for when one of them took the fallback.");
        });
    }

    /// <summary>
    /// The fail-closed half, and the load-bearing test of this fixture. A second
    /// registered repository shares the tree, so the whole-tree drop would delete
    /// its index records too - data loss on behalf of a caller who asked only for
    /// their own subtree. The fault must be kept, and the refusal must say why.
    /// <para>
    /// Delete the census check in <c>SweepTreeForResetAsync</c> and this test goes
    /// red while
    /// <see cref="Reset_drops_a_terminally_stale_tree_whole_when_the_repository_is_the_only_one"/>
    /// stays green.
    /// </para>
    /// </summary>
    [Test]
    public async Task Reset_refuses_the_whole_tree_drop_when_another_repository_shares_the_tree()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await SeedMarkerAsync(harness, "acme", Ct);
        await SeedMarkerAsync(harness, "widgets", Ct);

        var (store, faulting) = StoreWithStaleTree(harness, RepoContextTrees.VectorIndex);

        var ex = Assert.ThrowsAsync<McpException>(async () => await store.ResetIndexAsync("acme", Ct));

        await faulting.DidNotReceive().DeleteTreeAsync(Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(ex!.InnerException, Is.InstanceOf<LeafProjectionStaleException>(),
                "The refusal must carry the fault it declined to widen, so the cause is not lost.");
            Assert.That(ex.Message, Does.Contain("repocontext_remove_repo"),
                "An operator who is blocked needs the route out named in the refusal.");
        });
    }

    /// <summary>
    /// The structural tree is excluded from the fallback even when this repository
    /// is the only registered one. It carries the separator-free
    /// <c>repo/{repoId}</c> marker the reset deliberately preserves to keep the
    /// repository enumerable, and that marker sits outside the swept range
    /// precisely so the sweep cannot take it. A whole-tree drop is not bounded by
    /// that range, so taking it here would turn a reset into the disappearance the
    /// preserve branch exists to prevent.
    /// </summary>
    [Test]
    public async Task Reset_never_drops_the_structural_tree_whole_even_for_a_sole_repository()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        await SeedMarkerAsync(harness, "acme", Ct);

        var (store, faulting) = StoreWithStaleTree(harness, RepoContextTrees.Structural);

        Assert.ThrowsAsync<LeafProjectionStaleException>(async () => await store.ResetIndexAsync("acme", Ct));

        await faulting.DidNotReceive().DeleteTreeAsync(Arg.Any<CancellationToken>());
    }
}
