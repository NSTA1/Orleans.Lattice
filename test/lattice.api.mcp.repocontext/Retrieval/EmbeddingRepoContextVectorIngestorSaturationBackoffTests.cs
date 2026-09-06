using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Guards the two behaviours that stop a saturated vector plane from holding the
/// ingestor in a permanent re-embed loop (issue #2071): the memory arm's orphan
/// sweep only runs on evidence it actually finished reading, and the symbol arm
/// stops re-driving a membership tree that just refused its writes.
/// <para>
/// The reported symptom was a converged repository - <c>0 added, 0 updated,
/// 0 removed</c> - that still re-embedded the same few hundred symbol passages on
/// every single reconcile pass, forever. The engine was circular: the symbol arm's
/// gap back-fill drove the membership tree hard enough that its own membership
/// writes timed out, the flags it needed never landed, and the next pass selected
/// exactly the same symbols and drove the tree just as hard again.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>, so it is excluded from the fast unit loop.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class EmbeddingRepoContextVectorIngestorSaturationBackoffTests
{
    private const string RepoId = "acme";

    /// <summary>
    /// Enough symbols to fill more batches than
    /// <see cref="EmbeddingRepoContextVectorIngestor.MaxConsecutiveBatchFailures"/>,
    /// so a pass whose membership writes all fail reaches the saturation break
    /// rather than merely failing one unlucky batch.
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

    private static RepoContextMcpHarnessOptions Options(
        LatticeTreeFaultInjector injector, LatticeTreeCallCounter counter) => new()
        {
            Posture = RepoContextMcpAuthPosture.Writer,
            ConfigureSilo = silo =>
            {
                silo.Services.AddSingleton(injector);
                silo.Services.AddSingleton(counter);
                silo.Services.AddSingleton<IIncomingGrainCallFilter, LatticeTreeFaultInjectingFilter>();
                silo.Services.AddSingleton<IIncomingGrainCallFilter, LatticeTreeCallCountingFilter>();
            },
        };

    private static async Task<string[]> SeedSymbolsAsync(RepoContextMcpHarness harness, CancellationToken ct)
    {
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Symbol);
        var keys = new string[SymbolCount];
        for (var i = 0; i < SymbolCount; i++)
        {
            var fqn = $"Acme.Generated.Type{i:D3}";
            var record = new SymbolRecord { RepoId = RepoId, FullyQualifiedName = fqn, Kind = SymbolKind.Type };
            keys[i] = RepoContextKeys.Symbol(RepoId, fqn);
            await tree.SetAsync(keys[i], serializer.SerializeToArray(record), ct);
        }

        return keys;
    }

    /// <summary>
    /// A membership tree that refuses every presence write is the saturated plane
    /// from the issue. The pass after it must not walk the whole symbol space
    /// probing that same tree again: it embeds only what the reconcile named as
    /// changed and leaves the back-fill alone, so the tree gets a pass to drain.
    /// </summary>
    [Test]
    public async Task A_saturated_pass_makes_the_next_pass_skip_its_gap_backfill()
    {
        var injector = new LatticeTreeFaultInjector
        {
            TreeId = RepoContextTrees.VectorMembership,
            Method = "ApplyCrdtDeltaManyAsync",
            FailFirst = int.MaxValue,
        };
        var counter = new LatticeTreeCallCounter { TreeId = RepoContextTrees.VectorMembership };
        await using var harness = await RepoContextMcpHarness.StartAsync(Options(injector, counter), Ct);
        await SeedSymbolsAsync(harness, Ct);

        // The ingestor is a singleton in the host, so its backoff is deliberately
        // carried across passes - one instance is the faithful model.
        var ingestor = Ingestor(harness);

        Assert.That(
            async () => await ingestor.IngestSymbolsAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct),
            Throws.InstanceOf<TimeoutException>(),
            "Precondition: nothing landed, so the arm reports the fault - and records the saturation.");

        counter.Reset();
        var deferred = await ingestor.IngestSymbolsAsync(
            RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);

        Assert.Multiple(() =>
        {
            Assert.That(deferred, Is.EqualTo(0), "The deferred pass embeds nothing, because nothing changed,");
            Assert.That(counter.Count("GetManyAsync"), Is.EqualTo(0),
                "and - the point of the fix - it does not probe the membership tree at all. Without the "
                + "backoff this pass re-probed the whole symbol space and re-embedded the same passages, "
                + "which is the load that kept the writes failing.");
        });
    }

    /// <summary>
    /// The case the failure-counting backoff cannot see, and the one the live box
    /// actually exhibited (issue #2078): every batch <em>succeeds</em> - the embed
    /// completes, the vectors store, the membership write returns - and yet the
    /// next pass selects exactly the same symbols again, because the flags never
    /// become observable. No batch failed, so no failure counter trips; the loop is
    /// only visible by comparing one pass's selection against the last pass's
    /// landed work.
    /// <para>
    /// Non-observability is modelled by deleting the membership flags between
    /// passes. That is the observable condition on the box - a write that reports
    /// success but is not readable afterwards - reproduced deterministically
    /// instead of by saturating a tree until its leaves stop answering.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_pass_that_reselects_what_the_last_pass_landed_backs_off_though_every_batch_succeeded()
    {
        var injector = new LatticeTreeFaultInjector
        {
            TreeId = RepoContextTrees.VectorMembership,
            Method = "ApplyCrdtDeltaManyAsync",
            FailFirst = 0,
        };
        var counter = new LatticeTreeCallCounter { TreeId = RepoContextTrees.VectorMembership };
        await using var harness = await RepoContextMcpHarness.StartAsync(Options(injector, counter), Ct);
        await SeedSymbolsAsync(harness, Ct);
        var ingestor = Ingestor(harness);

        var membership = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMembership);
        var membershipPrefix = RepoContextKeys.VectorMembershipsPrefix(RepoId);

        async Task LoseTheFlagsAsync() => await membership.DeleteRangeAsync(
            membershipPrefix, RepoContextPortability.PrefixUpperBound(membershipPrefix), Ct);

        var first = await ingestor.IngestSymbolsAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);
        Assert.That(first, Is.EqualTo(SymbolCount),
            "Precondition: a clean pass embeds and records every symbol, with no batch failing.");

        // The writes reported success but are not readable, so the next pass sees
        // the identical gap - the exact shape of the loop on the live box.
        await LoseTheFlagsAsync();

        var second = await ingestor.IngestSymbolsAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);
        Assert.That(second, Is.EqualTo(SymbolCount),
            "The repeat pass re-embeds the same symbols - this is the wasted work - and detects the repeat.");

        await LoseTheFlagsAsync();
        counter.Reset();

        var third = await ingestor.IngestSymbolsAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);

        Assert.Multiple(() =>
        {
            Assert.That(third, Is.EqualTo(0),
                "The third pass stands the back-fill down instead of re-embedding the same symbols a third "
                + "time. Without the repeat detector this pass embeds all "
                + $"{SymbolCount} again, forever, which is issue #2078.");
            Assert.That(counter.Count("GetManyAsync"), Is.EqualTo(0),
                "and it does not probe the membership tree either, so the tree gets a pass to drain - the "
                + "load that sustains the loop is what has to stop.");
        });
    }

    /// <summary>
    /// The detector must not fire on a healthy repository. When the flags written
    /// by one pass are readable by the next, the gap selection is empty and there
    /// is nothing to repeat, so the back-fill keeps running pass after pass.
    /// </summary>
    [Test]
    public async Task A_pass_whose_flags_are_observable_never_engages_the_repeat_backoff()
    {
        var injector = new LatticeTreeFaultInjector
        {
            TreeId = RepoContextTrees.VectorMembership,
            Method = "ApplyCrdtDeltaManyAsync",
            FailFirst = 0,
        };
        var counter = new LatticeTreeCallCounter { TreeId = RepoContextTrees.VectorMembership };
        await using var harness = await RepoContextMcpHarness.StartAsync(Options(injector, counter), Ct);
        var keys = await SeedSymbolsAsync(harness, Ct);
        var ingestor = Ingestor(harness);

        await ingestor.IngestSymbolsAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);
        await ingestor.IngestSymbolsAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);

        counter.Reset();
        var embedded = await ingestor.IngestSymbolsAsync(RepoId, new[] { keys[0] }, Array.Empty<string>(), Ct);

        Assert.Multiple(() =>
        {
            Assert.That(embedded, Is.EqualTo(1), "The changed symbol is embedded normally,");
            Assert.That(counter.Count("GetManyAsync"), Is.GreaterThan(0),
                "and the back-fill still probes coverage, proving the repeat detector left a healthy "
                + "repository alone rather than standing its back-fill down.");
        });
    }

    /// <summary>
    /// Backing off must defer opportunistic work only. A symbol the reconcile
    /// reported as changed is stale in the index until it is re-embedded, so it is
    /// embedded on every pass regardless of the backoff.
    /// </summary>
    [Test]
    public async Task A_deferred_pass_still_embeds_the_symbols_the_reconcile_changed()
    {
        var injector = new LatticeTreeFaultInjector
        {
            TreeId = RepoContextTrees.VectorMembership,
            Method = "ApplyCrdtDeltaManyAsync",
            FailFirst = int.MaxValue,
        };
        var counter = new LatticeTreeCallCounter { TreeId = RepoContextTrees.VectorMembership };
        await using var harness = await RepoContextMcpHarness.StartAsync(Options(injector, counter), Ct);
        var keys = await SeedSymbolsAsync(harness, Ct);
        var ingestor = Ingestor(harness);

        Assert.That(
            async () => await ingestor.IngestSymbolsAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct),
            Throws.InstanceOf<TimeoutException>(),
            "Precondition: the pass saturated.");

        // The plane recovers, but this pass has already been granted its skip.
        injector.FailFirst = 0;
        var embedded = await ingestor.IngestSymbolsAsync(RepoId, new[] { keys[0] }, Array.Empty<string>(), Ct);

        Assert.That(embedded, Is.EqualTo(1),
            "Exactly the one changed symbol is embedded: correctness is never deferred, only the "
            + $"opportunistic back-fill of the other {SymbolCount - 1}.");
    }

    /// <summary>
    /// The backoff is a pause, not a surrender: once the plane accepts writes again
    /// a full pass runs and clears it, so the back-fill resumes on its own.
    /// </summary>
    [Test]
    public async Task The_backoff_clears_after_a_pass_that_completes_without_saturation()
    {
        var injector = new LatticeTreeFaultInjector
        {
            TreeId = RepoContextTrees.VectorMembership,
            Method = "ApplyCrdtDeltaManyAsync",
            FailFirst = int.MaxValue,
        };
        var counter = new LatticeTreeCallCounter { TreeId = RepoContextTrees.VectorMembership };
        await using var harness = await RepoContextMcpHarness.StartAsync(Options(injector, counter), Ct);
        var keys = await SeedSymbolsAsync(harness, Ct);
        var ingestor = Ingestor(harness);

        Assert.That(
            async () => await ingestor.IngestSymbolsAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct),
            Throws.InstanceOf<TimeoutException>(),
            "Precondition: the pass saturated, granting one skip.");

        injector.FailFirst = 0;

        // Pass 2 consumes the skip, so it back-fills nothing.
        var skipped = await ingestor.IngestSymbolsAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);

        // Pass 3 has no skip left: the back-fill runs and, with the plane healthy,
        // completes without saturation and clears the backoff.
        var recovered = await ingestor.IngestSymbolsAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);

        // Pass 4 proves the backoff really is gone rather than merely exhausted:
        // everything is embedded now, so a running back-fill finds nothing to do.
        var settled = await ingestor.IngestSymbolsAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);

        Assert.Multiple(() =>
        {
            Assert.That(skipped, Is.EqualTo(0), "The granted skip is honoured even though the plane recovered,");
            Assert.That(recovered, Is.EqualTo(keys.Length),
                "the very next pass back-fills the whole symbol space,");
            Assert.That(settled, Is.EqualTo(0),
                "and the pass after that finds every symbol already embedded, which is convergence rather "
                + "than another deferral.");
        });
    }

    /// <summary>
    /// The orphan sweep retires the difference between what is recorded and what is
    /// live, so an unread page of the recorded set is indistinguishable from a
    /// retired entry. Running the sweep on a partial read would retire live
    /// embeddings; it must decline instead, and resume sweeping once the read
    /// completes.
    /// </summary>
    [Test]
    public async Task The_orphan_sweep_declines_to_run_while_the_marker_set_is_incomplete()
    {
        var injector = new LatticeTreeFaultInjector
        {
            TreeId = RepoContextTrees.VectorMembership,
            Method = "GetSortedEntriesBatchAsync",
            IncludeShardGrains = true,
            FailFirst = 0,
        };
        var counter = new LatticeTreeCallCounter { TreeId = RepoContextTrees.VectorMembership };
        await using var harness = await RepoContextMcpHarness.StartAsync(Options(injector, counter), Ct);
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();
        var store = harness.Services.GetRequiredService<RepoContextStore>();
        var ingestor = Ingestor(harness);

        await store.RememberAsync(
            RepoId, "gotchas", "sweepable", MemoryKind.Note,
            title: "T", body: "B", author: null, provenance: null,
            tags: null, addLinks: null, removeLinks: null, ttlSeconds: null, cancellationToken: Ct);
        var key = RepoContextKeys.Memory(RepoId, "gotchas", "sweepable");

        await ingestor.IngestMemoryAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);
        Assert.That(await IsEmbeddedAsync(writer, key, Ct), Is.True, "Precondition: embedded.");

        // Model a time-to-live expiry - the record simply vanishes, so only the
        // sweep can notice - and then make the marker set unreadable.
        await harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Memory).DeleteAsync(key, Ct);
        injector.FailFirst = int.MaxValue;

        await ingestor.IngestMemoryAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);

        // Restore reads before observing, or the observation itself would fault.
        injector.FailFirst = 0;
        var survivedIncompleteRead = await IsEmbeddedAsync(writer, key, Ct);

        await ingestor.IngestMemoryAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);
        var sweptOnceComplete = await IsEmbeddedAsync(writer, key, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(survivedIncompleteRead, Is.True,
                "An unreadable marker set is not an empty one: the sweep declines rather than retiring "
                + "an embedding it has no evidence is orphaned.");
            Assert.That(sweptOnceComplete, Is.False,
                "and once the set can be read in full the sweep runs and retires the orphan, so declining "
                + "is a deferral rather than a permanent loss of the sweep.");
        });
    }

    private static async Task<bool> IsEmbeddedAsync(
        RepoContextVectorWriter writer, string memoryKey, CancellationToken ct)
    {
        var members = await writer.LoadEmbeddedMembersAsync(RepoId, ct);
        return members.Contains(VectorCodec.SourceId(memoryKey));
    }
}
