using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Guards that the gap back-fill's cached evidence is scoped to the index
/// incarnation it was gathered under (issue #2826).
/// <para>
/// The ingestor is a host singleton, and every cache that drives the gap
/// back-fill's stand-down is keyed by repository id alone. A repository id does
/// not identify an <em>index</em>: <c>reset_index</c> discards every derived plane
/// and deliberately KEEPS the repository, so the caches carried the previous
/// index's evidence straight into the new one. The back-fill for the fresh index
/// then consulted evidence about an index that no longer existed, concluded the
/// gap in front of it had already been served, and stood itself down - leaving the
/// new index permanently missing exactly the content the back-fill exists to
/// supply, while reporting itself converged. Silent, and self-certifying.
/// </para>
/// <para>
/// The stand-down it inherits is real and load-bearing, which is why
/// <see cref="A_repeat_backoff_still_stands_the_backfill_down_within_one_incarnation"/>
/// is carried here as a positive control: without it, a test asserting "the
/// back-fill ran" could pass simply because the stand-down never armed, and would
/// go on passing if the fix silently disabled the backoff altogether.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>, so it is excluded from the fast unit loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class EmbeddingRepoContextVectorIngestorIncarnationScopingTests
{
    private const string RepoId = "acme";

    /// <summary>
    /// Matched to the sibling saturation fixture so the repeat detector's majority
    /// threshold is crossed by a whole-set repeat rather than by a handful of
    /// stragglers.
    /// </summary>
    private const int SymbolCount = 96;

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static EmbeddingRepoContextVectorIngestor Ingestor(RepoContextMcpHarness harness)
        => new(
            harness.Services.GetRequiredService<RepoContextVectorWriter>(),
            harness.GrainFactory,
            harness.Services.GetRequiredService<Serializer>(),
            NullLogger<EmbeddingRepoContextVectorIngestor>.Instance,
            new FakeEmbeddingProvider());

    private static RepoContextMcpHarnessOptions Options() => new()
    {
        Posture = RepoContextMcpAuthPosture.Writer,
    };

    private static async Task SeedSymbolsAsync(RepoContextMcpHarness harness, CancellationToken ct)
    {
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Symbol);
        for (var i = 0; i < SymbolCount; i++)
        {
            var fqn = $"Acme.Generated.Type{i:D3}";
            var record = new SymbolRecord { RepoId = RepoId, FullyQualifiedName = fqn, Kind = SymbolKind.Type };
            await tree.SetAsync(RepoContextKeys.Symbol(RepoId, fqn), serializer.SerializeToArray(record), ct);
        }
    }

    /// <summary>
    /// Deletes the membership flags a pass just wrote. This is the deterministic
    /// model of the live symptom the repeat detector exists for: a write that
    /// reports success but is not readable afterwards, so the next pass selects the
    /// identical gap.
    /// </summary>
    private static Task LoseTheFlagsAsync(RepoContextMcpHarness harness, CancellationToken ct)
    {
        var membership = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMembership);
        var prefix = RepoContextKeys.VectorMembershipsPrefix(RepoId);

        // The upper bound is nullable only for a prefix with no successor; a
        // membership prefix always has one.
        return membership.DeleteRangeAsync(prefix, RepoContextPortability.PrefixUpperBound(prefix)!, ct);
    }

    /// <summary>
    /// Arms the symbol arm's repeat backoff exactly as the live box does: two full
    /// passes whose membership flags never become observable, so the second pass
    /// re-selects everything the first one landed and the detector fires.
    /// </summary>
    private static async Task ArmTheRepeatBackoffAsync(
        RepoContextMcpHarness harness, EmbeddingRepoContextVectorIngestor ingestor, CancellationToken ct)
    {
        var first = await ingestor.IngestSymbolsAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), ct);
        Assert.That(first, Is.EqualTo(SymbolCount),
            "Precondition: a clean pass embeds and records every symbol.");

        await LoseTheFlagsAsync(harness, ct);

        var second = await ingestor.IngestSymbolsAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), ct);
        Assert.That(second, Is.EqualTo(SymbolCount),
            "Precondition: the repeat pass re-embeds the same symbols, which is what the detector reads "
            + "as the loop - the backoff is now armed and the landed set is now recorded.");
    }

    /// <summary>
    /// The regression. A reset discards the index the cached evidence was about, so
    /// the re-added index must back-fill its own gap rather than inherit a
    /// stand-down from the incarnation before it.
    /// </summary>
    [Test]
    public async Task A_reset_discards_the_gap_evidence_so_the_re_added_index_still_backfills()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(Options(), Ct);
        await SeedSymbolsAsync(harness, Ct);

        // One instance across every pass is the faithful model: the ingestor is a
        // singleton in the host, so its caches are deliberately carried forward.
        var ingestor = Ingestor(harness);
        await ArmTheRepeatBackoffAsync(harness, ingestor, Ct);

        // The operation whose entire purpose is to discard the derived state.
        var store = harness.Services.GetRequiredService<RepoContextStore>();
        await store.ResetIndexAsync(RepoId, Ct);

        // Re-add: the reset swept the symbol records too, so the new incarnation is
        // seeded with the same content over an empty index.
        await SeedSymbolsAsync(harness, Ct);

        var afterReset = await ingestor.IngestSymbolsAsync(
            RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);

        Assert.That(afterReset, Is.EqualTo(SymbolCount),
            "The re-added index back-fills its own gap. Before the fix this pass embedded 0: the backoff "
            + "and the landed-set record were keyed by repository id alone, so they survived the reset, "
            + "and the new index stood its back-fill down on the previous incarnation's evidence - "
            + "permanently missing content while reporting itself converged (issue #2826).");
    }

    /// <summary>
    /// The positive control for the regression above. The stand-down path must
    /// still fire on genuinely current, same-incarnation evidence - otherwise the
    /// regression test would pass for the wrong reason (a back-fill that simply has
    /// no evidence to consult is not a back-fill that correctly rejected stale
    /// evidence), and a fix that disabled the backoff wholesale would look correct.
    /// </summary>
    [Test]
    public async Task A_repeat_backoff_still_stands_the_backfill_down_within_one_incarnation()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(Options(), Ct);
        await SeedSymbolsAsync(harness, Ct);

        var ingestor = Ingestor(harness);
        await ArmTheRepeatBackoffAsync(harness, ingestor, Ct);

        // Identical to the regression test except that no reset intervenes, so the
        // incarnation is unchanged and the evidence is still about this index.
        await LoseTheFlagsAsync(harness, Ct);

        var third = await ingestor.IngestSymbolsAsync(
            RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);

        Assert.That(third, Is.EqualTo(0),
            "Current evidence is still honoured: the third pass stands the back-fill down rather than "
            + "re-embedding the same symbols a third time. Scoping evidence to an incarnation must not "
            + "become a way of never having any.");
    }

    /// <summary>
    /// The token itself. It must be stable while the index is - otherwise the
    /// eviction would fire every pass and destroy the cross-pass evidence the
    /// backoff depends on - and it must change across a reset, which is what makes
    /// the regression test above non-vacuous.
    /// </summary>
    [Test]
    public async Task EnsureIndexIncarnationAsync_is_stable_across_reads_and_changes_across_a_reset()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(Options(), Ct);
        var job = harness.GrainFactory.GetGrain<IRepoIndexJobGrain>(RepoId);

        var minted = await job.EnsureIndexIncarnationAsync();
        var reread = await job.EnsureIndexIncarnationAsync();

        await harness.Services.GetRequiredService<RepoContextStore>().ResetIndexAsync(RepoId, Ct);
        var afterReset = await job.EnsureIndexIncarnationAsync();

        Assert.Multiple(() =>
        {
            Assert.That(minted, Is.Not.Empty, "A token is minted on first read rather than left absent,");
            Assert.That(reread, Is.EqualTo(minted),
                "re-reading returns the same token - minting per pass would invalidate the cross-pass "
                + "evidence the token exists to protect,");
            Assert.That(afterReset, Is.Not.EqualTo(minted),
                "and a reset re-mints it, which is the signal that makes every cached assertion about the "
                + "previous index discardable.");
        });
    }

    /// <summary>
    /// A removal is the other way the derived state is discarded. It clears the job
    /// grain's whole state rather than re-minting in place, so the token must come
    /// back different on the re-add - the lazy mint covers this without the removal
    /// path needing to know the token exists.
    /// </summary>
    [Test]
    public async Task EnsureIndexIncarnationAsync_changes_across_a_remove_and_re_add()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(Options(), Ct);
        var job = harness.GrainFactory.GetGrain<IRepoIndexJobGrain>(RepoId);

        var before = await job.EnsureIndexIncarnationAsync();
        await harness.Services.GetRequiredService<RepoContextStore>().RemoveRepoAsync(RepoId, Ct);
        var after = await job.EnsureIndexIncarnationAsync();

        Assert.That(after, Is.Not.EqualTo(before),
            "The re-added repository is a new index even though it reuses the id, so evidence gathered "
            + "before the removal must not be admissible after it.");
    }
}
