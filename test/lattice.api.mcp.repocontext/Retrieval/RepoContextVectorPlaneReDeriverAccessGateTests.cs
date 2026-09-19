using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Integration tests for <see cref="RepoContextVectorPlaneReDeriver"/> running
/// against a <b>real deny-by-default access gate</b> (issue #2737).
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this fixture exists at all.</b> The re-derivation reset issues
/// <see cref="ILattice.DeleteTreeAsync"/>, which the core enforcement seam
/// authorizes as <see cref="LatticeOperation.TreeLifecycle"/>. In production the
/// ambient subject held no matching rule, so every reset was refused and a tree
/// that had fallen terminally off its write-ahead log could never self-heal. No
/// unit test could have seen that: substituting <see cref="IGrainFactory"/>
/// bypasses the grain, and the harness's default gate is
/// <see cref="NullLatticeAccessGate"/>, which the enforcement seam short-circuits
/// entirely. Both configurations are ones in which the defect is not expressible,
/// so a green suite built on either proves nothing.
/// </para>
/// <para>
/// Every test here therefore registers <see cref="DenyTreeLifecycleAccessGate"/>
/// - a genuine deny-by-default evaluator that grants the ordinary data-plane
/// operations and refuses <see cref="LatticeOperation.TreeLifecycle"/>, exactly
/// reproducing the reported posture. The gate's liveness is not assumed: a
/// control test asserts a caller-origin delete on the same tree is still refused,
/// so a gate that had silently degraded to allow-all could not let these tests
/// pass.
/// </para>
/// <para>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>, so it is excluded from the fast unit dev
/// loop.
/// </para>
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextVectorPlaneReDeriverAccessGateTests
{
    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static LeafProjectionStaleException Stale(string treeId)
        => new($"leaf projection for tree '{treeId}' has fallen off the write-ahead log");

    private static async Task<(RepoContextMcpHarness Harness, DenyTreeLifecycleAccessGate Gate)> StartAsync(
        CancellationToken ct)
    {
        var gate = new DenyTreeLifecycleAccessGate();
        var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions
            {
                // Registered after AddLattice has contributed the null gate, so
                // this wins for the single-service resolution the enforcement
                // seam performs.
                ConfigureSilo = silo => silo.Services.AddSingleton<ILatticeAccessGate>(gate),
            },
            ct);
        return (harness, gate);
    }

    private sealed record Measurement(long Value, string? Tree, string? Outcome);

    private static (List<Measurement> Measurements, MeterListener Listener) StartCapture()
    {
        var measurements = new List<Measurement>();
        var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RepoContextUsageRecorder.MeterName
                && instrument.Name == RepoContextVectorPlaneReDeriver.ReDeriveInstrumentName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
        {
            string? tree = null;
            string? outcome = null;
            foreach (var tag in tags)
            {
                if (tag.Key == RepoContextVectorPlaneReDeriver.TreeTagKey)
                {
                    tree = tag.Value as string;
                }
                else if (tag.Key == RepoContextVectorPlaneReDeriver.OutcomeTagKey)
                {
                    outcome = tag.Value as string;
                }
            }

            lock (measurements)
            {
                measurements.Add(new Measurement(measurement, tree, outcome));
            }
        });
        listener.Start();
        return (measurements, listener);
    }

    private static long Total(IEnumerable<Measurement> measurements, string tree, string outcome)
    {
        lock (measurements)
        {
            return measurements.Where(m => m.Tree == tree && m.Outcome == outcome).Sum(m => m.Value);
        }
    }

    /// <summary>
    /// The liveness control for every other test in this fixture, and the direct
    /// evidence that the gate is real rather than the null one. A caller-origin
    /// delete of the very tree the re-deriver resets must still be refused, so an
    /// accidental regression to an allow-all gate (or to
    /// <see cref="NullLatticeAccessGate"/>) reddens here instead of quietly
    /// making the other assertions vacuous.
    /// </summary>
    [Test]
    public async Task CallerOriginTreeDeleteIsDeniedByTheGate()
    {
        var (harness, gate) = await StartAsync(Ct);
        await using var _ = harness;

        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMetadata);
        await tree.SetAsync("k", new byte[] { 1 }, Ct);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await tree.DeleteTreeAsync(Ct));

        Assert.That(
            gate.DeniedOperations.Any(r => r.Operation == LatticeOperation.TreeLifecycle),
            Is.True,
            "the gate must have been consulted and have refused the lifecycle operation");
    }

    /// <summary>
    /// The defect's regression test. Under the same deny-by-default gate that
    /// refuses a caller-origin delete, the re-derivation reset must complete: the
    /// maintenance path establishes a system origin, so the destructive reset is
    /// not evaluated against a caller's rules at all.
    /// </summary>
    [Test]
    public async Task ReDerivationCompletesUnderADenyByDefaultGate()
    {
        var (harness, gate) = await StartAsync(Ct);
        await using var _ = harness;

        const string treeName = RepoContextTrees.VectorMetadata;
        var tree = harness.GrainFactory.GetGrain<ILattice>(treeName);
        await tree.SetAsync("vector/1", new byte[] { 1, 2, 3 }, Ct);
        Assert.That(await tree.GetAsync("vector/1", Ct), Is.Not.Null, "arrange failed: the seed write did not land");

        var reDeriver = harness.Services.GetRequiredService<RepoContextVectorPlaneReDeriver>();
        var (measurements, listener) = StartCapture();
        using var __ = listener;

        await reDeriver.ObserveAndReDeriveAsync(treeName, Stale(treeName), Ct);

        Assert.Multiple(() =>
        {
            Assert.That(
                Total(measurements, treeName, RepoContextVectorPlaneReDeriver.OutcomeCompleted),
                Is.EqualTo(1),
                "the reset must complete under a gate that denies TreeLifecycle");
            Assert.That(
                Total(measurements, treeName, RepoContextVectorPlaneReDeriver.OutcomeDenied),
                Is.Zero,
                "the reset must not be refused");
            Assert.That(
                Total(measurements, treeName, RepoContextVectorPlaneReDeriver.OutcomeFailed),
                Is.Zero,
                "the reset must not fail");
        });

        // The remediation actually happened: the tree's contents are gone, so the
        // ingest path re-derives a clean tree rather than the counter merely
        // reporting a success that did no work.
        Assert.That(await tree.GetAsync("vector/1", Ct), Is.Null, "the reset did not drop the tree contents");

        // And it reached that outcome by bypassing the gate rather than by the
        // gate having allowed it - the observable signature of a system origin.
        Assert.That(
            gate.TreeLifecycleEvaluations(treeName),
            Is.Zero,
            "the maintenance reset must not be evaluated against a caller's rules");
    }

    /// <summary>
    /// The system-origin scope is lexical and must not outlive the reset. After a
    /// completed re-derivation the ambient origin is cleared and a caller-origin
    /// delete is refused exactly as before, so the bypass cannot leak into a
    /// caller's turn.
    /// </summary>
    [Test]
    public async Task SystemOriginDoesNotLeakPastTheReset()
    {
        var (harness, gate) = await StartAsync(Ct);
        await using var _ = harness;

        const string treeName = RepoContextTrees.VectorMetadata;
        var reDeriver = harness.Services.GetRequiredService<RepoContextVectorPlaneReDeriver>();
        await reDeriver.ObserveAndReDeriveAsync(treeName, Stale(treeName), Ct);

        Assert.That(LatticeSystemOrigin.IsActive, Is.False, "the system-origin scope outlived the reset");

        var other = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Structural);
        await other.SetAsync("k", new byte[] { 1 }, Ct);
        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await other.DeleteTreeAsync(Ct),
            "a caller-origin delete must still be refused after a re-derivation");

        Assert.That(
            gate.DeniedOperations.Any(r =>
                r.Operation == LatticeOperation.TreeLifecycle
                && r.TreeId == RepoContextTrees.Structural),
            Is.True);
    }
}
