using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Runtime;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Capture;

/// <summary>
/// Pins the post-commit fault-isolation contract on <see cref="RepoContextStore"/>:
/// once a write is durable, no later step may turn the call into a failure.
/// <para>
/// The defect is a reporting one, not a durability one. A repository-context write
/// commits, then does enrichment work - reading the committed expiry back, retiring
/// the entry's stale vector - and a fault in that work used to propagate. The caller
/// then sees an exception describing no effect while the effect is durable and
/// visible to every other reader. That is unrecoverable from the caller's side:
/// nothing in the error distinguishes a rejected write from a committed one, and the
/// default entry id is a fresh GUID, so the obvious remedy - retry - writes a SECOND
/// entry rather than converging on the first.
/// </para>
/// <para>
/// <b>Every test here pairs its fault with a positive control</b> asserting the
/// injector actually fired (<c>Failed &gt; 0</c>). Without it a green could equally
/// mean the selector never matched and no fault was ever injected, which is the
/// failure mode a fault-injection fixture is most prone to.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: it co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/> and injects the fault at the real grain call
/// rather than by substituting a collaborator the runtime never uses.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextStorePostCommitFaultIsolationTests
{
    private const string RepoId = "acme";

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static async Task<RepoContextMcpHarness> StartAsync(
        CancellationToken ct, params LatticeTreeFaultInjector[] injectors)
        => await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions
            {
                Posture = RepoContextMcpAuthPosture.Writer,
                ConfigureSilo = silo =>
                {
                    foreach (var injector in injectors)
                    {
                        silo.Services.AddSingleton(injector);
                    }

                    silo.Services.AddSingleton<IIncomingGrainCallFilter, LatticeTreeFaultInjectingFilter>();
                },
            }, ct);

    /// <summary>
    /// Faults the expiry read-back on the memory tree's facade grain. Shard grains
    /// are deliberately excluded so each logical call matches exactly once and the
    /// call-ordering arithmetic below stays unambiguous.
    /// </summary>
    private static LatticeTreeFaultInjector ExpiryReadBackFault(Func<string, Exception>? fault = null)
        => new()
        {
            TreeId = RepoContextTrees.Memory,
            Method = nameof(ILattice.GetWithVersionAsync),
            FailFirst = int.MaxValue,
            FaultFactory = fault,
        };

    // ---- remember -----------------------------------------------------------

    [Test]
    public async Task Remember_reports_the_committed_write_when_the_expiry_read_back_faults()
    {
        var injector = ExpiryReadBackFault();
        await using var harness = await StartAsync(Ct, injector);
        var store = harness.Services.GetRequiredService<RepoContextStore>();

        var result = await store.RememberAsync(
            RepoId, "notes", id: "m1", MemoryKind.Note, title: "t", body: "b",
            author: null, provenance: null, tags: null, addLinks: null, removeLinks: null,
            ttlSeconds: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(injector.Failed, Is.GreaterThan(0),
                "Positive control: the post-commit read-back was actually faulted.");
            Assert.That(result.Created, Is.True,
                "The write is durable, so the call must report it rather than throw.");
            Assert.That(result.Key, Is.EqualTo(RepoContextKeys.Memory(RepoId, "notes", "m1")));
            Assert.That(result.Expires, Is.Null,
                "Expiry was not evaluated. Reporting false would present an unmeasured value as a "
                + "measured one, which is indistinguishable from 'this entry is durable'.");
            Assert.That(result.ExpiresAtUtc, Is.Null);
        });
    }

    /// <summary>
    /// Positive control for the test above: with no fault, the same call reports a
    /// measured expiry. This is what proves <c>Expires == null</c> above is the
    /// isolation working rather than the field simply never being populated.
    /// </summary>
    [Test]
    public async Task Remember_reports_a_measured_expiry_when_nothing_faults()
    {
        await using var harness = await StartAsync(Ct);
        var store = harness.Services.GetRequiredService<RepoContextStore>();

        var never = await store.RememberAsync(
            RepoId, "notes", id: "m1", MemoryKind.Note, title: "t", body: "b",
            author: null, provenance: null, tags: null, addLinks: null, removeLinks: null,
            ttlSeconds: null, Ct);
        var expiring = await store.RememberAsync(
            RepoId, "notes", id: "m2", MemoryKind.Note, title: "t", body: "b",
            author: null, provenance: null, tags: null, addLinks: null, removeLinks: null,
            ttlSeconds: 600, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(never.Expires, Is.False,
                "Measured, and measured false: an entry with no TTL does not expire.");
            Assert.That(expiring.Expires, Is.True);
            Assert.That(expiring.ExpiresAtUtc, Is.Not.Null);
        });
    }

    /// <summary>
    /// Cancellation is the fault class that most needs covering, because it was the
    /// one deliberately re-thrown: the vector invalidation swallowed everything
    /// except <see cref="OperationCanceledException"/>. Cancelling a token cannot
    /// un-commit a durable write, so post-commit the honest answer is the result.
    /// </summary>
    [Test]
    public async Task Remember_reports_the_committed_write_when_the_vector_invalidation_is_cancelled()
    {
        var injector = new LatticeTreeFaultInjector
        {
            TreeId = RepoContextTrees.VectorMetadata,
            Method = nameof(ILattice.DeleteRangeAsync),
            FailFirst = int.MaxValue,
            IncludeShardGrains = true,
            FaultFactory = message => new OperationCanceledException(message),
        };
        await using var harness = await StartAsync(Ct, injector);
        var store = harness.Services.GetRequiredService<RepoContextStore>();

        var result = await store.RememberAsync(
            RepoId, "notes", id: "m1", MemoryKind.Note, title: "t", body: "b",
            author: null, provenance: null, tags: null, addLinks: null, removeLinks: null,
            ttlSeconds: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(injector.Failed, Is.GreaterThan(0),
                "Positive control: the post-commit vector invalidation was actually cancelled.");
            Assert.That(result.Created, Is.True,
                "A cancellation raised after the commit must not report 'no effect' about a durable write.");
        });
    }

    // ---- forget -------------------------------------------------------------

    [Test]
    public async Task Forget_lapse_reports_the_committed_lapse_when_the_expiry_read_back_faults()
    {
        var injector = ExpiryReadBackFault();
        injector.FailFirst = 0;
        await using var harness = await StartAsync(Ct, injector);
        var store = harness.Services.GetRequiredService<RepoContextStore>();
        var key = RepoContextKeys.Memory(RepoId, "notes", "m1");

        await store.RememberAsync(
            RepoId, "notes", id: "m1", MemoryKind.Note, title: "t", body: "b",
            author: null, provenance: null, tags: null, addLinks: null, removeLinks: null,
            ttlSeconds: null, Ct);

        // The lapse reads the record BEFORE writing it, and that read is pre-commit:
        // nothing has been written, so a fault there must fail the request. Let it
        // through and arm on the next match, which is the post-commit read-back.
        injector.FailAfterMatches = injector.Matched + 1;
        injector.FailFirst = int.MaxValue;

        var result = await store.ForgetAsync(key, lapse: true, lapseSeconds: 60, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(injector.Failed, Is.GreaterThan(0),
                "Positive control: the post-commit read-back was actually faulted.");
            Assert.That(result.Existed, Is.True);
            Assert.That(result.Mode, Is.EqualTo("lapse"),
                "The lapse is durable, so the call must report it rather than throw.");
            Assert.That(result.ExpiresAtUtc, Is.Null,
                "On a lapse result a null expiry is unambiguous: a lapsed entry always carries one, "
                + "so null here can only mean the read-back was not evaluated.");
        });
    }

    /// <summary>
    /// Positive control for the test above: unfaulted, the same lapse reports the
    /// expiry it just wrote.
    /// </summary>
    [Test]
    public async Task Forget_lapse_reports_a_measured_expiry_when_nothing_faults()
    {
        await using var harness = await StartAsync(Ct);
        var store = harness.Services.GetRequiredService<RepoContextStore>();
        var key = RepoContextKeys.Memory(RepoId, "notes", "m1");

        await store.RememberAsync(
            RepoId, "notes", id: "m1", MemoryKind.Note, title: "t", body: "b",
            author: null, provenance: null, tags: null, addLinks: null, removeLinks: null,
            ttlSeconds: null, Ct);

        var result = await store.ForgetAsync(key, lapse: true, lapseSeconds: 60, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(result.Mode, Is.EqualTo("lapse"));
            Assert.That(result.ExpiresAtUtc, Is.Not.Null,
                "Unfaulted, the lapse window is measured and reported.");
        });
    }

    [Test]
    public async Task Forget_delete_reports_the_committed_delete_when_the_vector_invalidation_is_cancelled()
    {
        var injector = new LatticeTreeFaultInjector
        {
            TreeId = RepoContextTrees.VectorMetadata,
            Method = nameof(ILattice.DeleteRangeAsync),
            FailFirst = 0,
            IncludeShardGrains = true,
            FaultFactory = message => new OperationCanceledException(message),
        };
        await using var harness = await StartAsync(Ct, injector);
        var store = harness.Services.GetRequiredService<RepoContextStore>();
        var key = RepoContextKeys.Memory(RepoId, "notes", "m1");

        await store.RememberAsync(
            RepoId, "notes", id: "m1", MemoryKind.Note, title: "t", body: "b",
            author: null, provenance: null, tags: null, addLinks: null, removeLinks: null,
            ttlSeconds: null, Ct);
        injector.FailFirst = int.MaxValue;

        var result = await store.ForgetAsync(key, lapse: false, lapseSeconds: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(injector.Failed, Is.GreaterThan(0),
                "Positive control: the post-commit vector invalidation was actually cancelled.");
            Assert.That(result.Existed, Is.True);
            Assert.That(result.Mode, Is.EqualTo("delete"));
        });
    }

    // ---- update -------------------------------------------------------------

    [Test]
    public async Task Update_reports_the_committed_patch_when_the_vector_invalidation_is_cancelled()
    {
        var injector = new LatticeTreeFaultInjector
        {
            TreeId = RepoContextTrees.VectorMetadata,
            Method = nameof(ILattice.DeleteRangeAsync),
            FailFirst = 0,
            IncludeShardGrains = true,
            FaultFactory = message => new OperationCanceledException(message),
        };
        await using var harness = await StartAsync(Ct, injector);
        var store = harness.Services.GetRequiredService<RepoContextStore>();
        var key = RepoContextKeys.Memory(RepoId, "notes", "m1");

        await store.RememberAsync(
            RepoId, "notes", id: "m1", MemoryKind.Note, title: "t", body: "b",
            author: null, provenance: null, tags: null, addLinks: null, removeLinks: null,
            ttlSeconds: null, Ct);
        injector.FailFirst = int.MaxValue;

        var result = await store.UpdateAsync(
            key,
            fields: new Dictionary<string, string> { ["title"] = "patched" },
            addTags: null, removeTags: null, addLinks: null, removeLinks: null, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(injector.Failed, Is.GreaterThan(0),
                "Positive control: the post-commit vector invalidation was actually cancelled.");
            Assert.That(result.FieldsUpdated, Is.EqualTo(1),
                "The patch is durable, so the call must report it rather than throw.");
            Assert.That(result.Key, Is.EqualTo(key));
        });
    }
}
