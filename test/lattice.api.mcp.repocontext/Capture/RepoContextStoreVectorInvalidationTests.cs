using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Runtime;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Integration test for the best-effort swallow in
/// <see cref="RepoContextStore"/>'s memory-vector invalidation. A remember of a
/// memory entry retires that entry's vector as a side effect; when the underlying
/// vector-metadata range delete faults, that failure must be swallowed so the
/// durable capture still succeeds rather than being sunk by a derived-projection
/// error. The fault is injected at the real grain call so the swallow is exercised
/// end to end.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: it co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/> and injects a grain-call fault via
/// <see cref="LatticeTreeFaultInjector"/>, so it is excluded from the fast unit dev loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextStoreVectorInvalidationTests
{
    private const string RepoId = "acme";

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    [Test]
    public async Task Remember_swallows_a_faulted_vector_invalidation_and_still_captures()
    {
        var injector = new LatticeTreeFaultInjector
        {
            TreeId = RepoContextTrees.VectorMetadata,
            Method = nameof(ILattice.DeleteRangeAsync),
            FailFirst = int.MaxValue,
            IncludeShardGrains = true,
        };
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions
            {
                Posture = RepoContextMcpAuthPosture.Writer,
                ConfigureSilo = silo =>
                {
                    silo.Services.AddSingleton(injector);
                    silo.Services.AddSingleton<IIncomingGrainCallFilter, LatticeTreeFaultInjectingFilter>();
                },
            }, Ct);
        var store = harness.Services.GetRequiredService<RepoContextStore>();

        // RememberAsync retires the entry's vector (a metadata range delete). That
        // delete is faulted, but the capture is the durable act and must not fail.
        var result = await store.RememberAsync(
            RepoId, "notes", id: "m1", MemoryKind.Note, title: "t", body: "b",
            author: null, provenance: null, tags: null, addLinks: null, removeLinks: null, ttlSeconds: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.Created, Is.True,
                "The capture succeeds even though the derived vector invalidation faulted.");
            Assert.That(injector.Failed, Is.GreaterThan(0),
                "The vector-metadata range delete was actually faulted.");
        });
    }
}
