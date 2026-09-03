using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Guards the memory arm's settling behaviour (issue #1934).
/// <para>
/// The arm decided what to re-embed solely from the source-id membership flag,
/// which is written by <c>AddMembersAsync</c> - the call that shares the
/// membership tree with the file and symbol arms' gap sweeps and times out under
/// that load. When it did not land, an entry looked un-embedded on every later
/// pass and was re-embedded forever, even though the arm had already written a
/// second, targeted marker recording exactly that embedding.
/// </para>
/// <para>
/// The companion defect: that marker was written for every source the pass
/// intended to embed rather than the ones that actually landed, so it could
/// assert an embedding a failed batch never stored. Since each batch gained its
/// own failure boundary the arm survives a failed batch and reaches the marking
/// step, which is what makes the distinction load-bearing.
/// </para>
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: each test co-hosts a real Orleans silo via
/// <see cref="RepoContextMcpHarness"/>.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class EmbeddingRepoContextVectorIngestorMemorySettlingTests
{
    private const string RepoId = "acme";

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    private static EmbeddingRepoContextVectorIngestor Ingestor(RepoContextMcpHarness harness)
        => new(
            harness.Services.GetRequiredService<RepoContextVectorWriter>(),
            harness.GrainFactory,
            harness.Services.GetRequiredService<Serializer>(),
            NullLogger<EmbeddingRepoContextVectorIngestor>.Instance,
            new FakeEmbeddingProvider());

    private static async Task<List<string>> SeedMemoryAsync(
        RepoContextMcpHarness harness, int count, CancellationToken ct)
    {
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.Memory);
        var clock = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks, Counter = 0 };
        var keys = new List<string>(count);

        for (var i = 0; i < count; i++)
        {
            var id = $"note-{i:D3}";
            var record = new MemoryRecord
            {
                RepoId = RepoId,
                Topic = "gotchas",
                Id = id,
                Kind = MemoryKind.Note,
                Title = RepoContextValues.Lww($"Note {i}", clock),
                Body = RepoContextValues.Lww(
                    $"A durable note with enough prose to embed as a passage. Index {i}.", clock),
                Tags = new OrSet(),
            };

            var key = RepoContextKeys.Memory(RepoId, "gotchas", id);
            await tree.SetAsync(key, MemoryRegisterTestEncoding.EncodeSingle(serializer, "A", record), ct);
            keys.Add(key);
        }

        return keys;
    }

    /// <summary>
    /// Disables a source's membership flag through the ordinary lattice API,
    /// modelling the <c>AddMembersAsync</c> write that timed out and never landed
    /// while the entry's vectors and its memory-key marker remain in place.
    /// </summary>
    private static Task LoseSourceIdFlagAsync(
        RepoContextMcpHarness harness, string memoryKey, CancellationToken ct)
    {
        var tree = harness.GrainFactory.GetGrain<ILattice>(RepoContextTrees.VectorMembership);
        var key = RepoContextKeys.VectorMembership(RepoId, VectorCodec.SourceId(memoryKey));
        return tree.OrFlag(key).DisableAsync(ct);
    }

    [Test]
    public async Task An_entry_settles_even_when_its_source_id_flag_never_landed()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var keys = await SeedMemoryAsync(harness, 3, Ct);

        var first = await Ingestor(harness).IngestMemoryAsync(RepoId, keys, Array.Empty<string>(), Ct);
        Assume.That(first, Is.EqualTo(3), "arranged: the first pass embeds every entry");

        // The vectors and the memory-key markers stay; only the source-id flags go,
        // which is precisely the state a timed-out AddMembersAsync leaves behind.
        foreach (var key in keys)
        {
            await LoseSourceIdFlagAsync(harness, key, Ct);
        }

        var second = await Ingestor(harness).IngestMemoryAsync(
            RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);

        Assert.That(second, Is.Zero,
            "The embedded-key marker is sufficient evidence on its own, so an entry whose source-id "
            + "flag was lost is not re-embedded on every pass forever.");
    }

    [Test]
    public async Task An_unchanged_entry_is_embedded_once_and_stays_settled()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var keys = await SeedMemoryAsync(harness, 4, Ct);
        var ingestor = Ingestor(harness);

        var first = await ingestor.IngestMemoryAsync(RepoId, keys, Array.Empty<string>(), Ct);
        var second = await ingestor.IngestMemoryAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);
        var third = await ingestor.IngestMemoryAsync(RepoId, Array.Empty<string>(), Array.Empty<string>(), Ct);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(4), "The first pass embeds every entry.");
            Assert.That(second, Is.Zero, "The second embeds none of them.");
            Assert.That(third, Is.Zero, "And it stays settled - this is issue #1922's acceptance criterion.");
        });
    }

    [Test]
    public async Task A_changed_entry_is_still_re_embedded()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer }, Ct);
        var keys = await SeedMemoryAsync(harness, 2, Ct);
        var ingestor = Ingestor(harness);

        await ingestor.IngestMemoryAsync(RepoId, keys, Array.Empty<string>(), Ct);

        // Settling must not become "never update": a revision still re-embeds.
        var again = await ingestor.IngestMemoryAsync(RepoId, new[] { keys[0] }, Array.Empty<string>(), Ct);

        Assert.That(again, Is.EqualTo(1),
            "An entry named as changed is re-embedded regardless of either marker.");
    }

    [Test]
    public async Task A_failed_marker_write_does_not_fail_a_pass_that_embedded_successfully()
    {
        // The marker write is bookkeeping: the vectors are already stored and their
        // membership recorded by the time it runs, so losing it costs one redundant
        // re-embed and must not discard a pass that genuinely succeeded.
        //
        // Order of ApplyCrdtDeltaManyAsync calls in this arm: one per embedding
        // batch (the membership write), then one for the marker. Two entries embed
        // in a single batch, so failing the second call isolates the marker write.
        var injector = new LatticeTreeFaultInjector
        {
            TreeId = RepoContextTrees.VectorMembership,
            Method = nameof(ILattice.ApplyCrdtDeltaManyAsync),
            FailAfterMatches = 1,
            FailFirst = 1,
        };

        var options = new RepoContextMcpHarnessOptions
        {
            Posture = RepoContextMcpAuthPosture.Writer,
            ConfigureSilo = silo =>
            {
                silo.Services.AddSingleton(injector);
                silo.Services.AddSingleton<IIncomingGrainCallFilter, LatticeTreeFaultInjectingFilter>();
            },
        };

        await using var harness = await RepoContextMcpHarness.StartAsync(options, Ct);
        var keys = await SeedMemoryAsync(harness, 2, Ct);
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();

        var embedded = await Ingestor(harness).IngestMemoryAsync(
            RepoId, keys, Array.Empty<string>(), Ct);

        var members = await writer.LoadEmbeddedMembersAsync(RepoId, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(injector.Failed, Is.EqualTo(1), "The marker write was faulted.");
            Assert.That(embedded, Is.EqualTo(2),
                "The pass still reports the work it actually did rather than failing outright.");
            Assert.That(members.Contains(VectorCodec.SourceId(keys[0])), Is.True,
                "And the embedding itself landed, which is why losing the marker is only bookkeeping.");
        });
    }

    [Test]
    public async Task A_failed_member_probe_falls_back_to_the_markers_for_that_page()
    {
        // The memory arm's own page probe - the last of the four membership reads
        // in this path to be guarded. It can fall back rather than skip, because
        // the marker set is an independent source of the same evidence.
        // The probe and the membership write both go through GetManyAsync, and the
        // probe runs first, so failing exactly the first call isolates the probe
        // while leaving the write that follows it healthy.
        var injector = new LatticeTreeFaultInjector
        {
            TreeId = RepoContextTrees.VectorMembership,
            Method = nameof(ILattice.GetManyAsync),
            FailFirst = 1,
        };

        var options = new RepoContextMcpHarnessOptions
        {
            Posture = RepoContextMcpAuthPosture.Writer,
            ConfigureSilo = silo =>
            {
                silo.Services.AddSingleton(injector);
                silo.Services.AddSingleton<IIncomingGrainCallFilter, LatticeTreeFaultInjectingFilter>();
            },
        };

        await using var harness = await RepoContextMcpHarness.StartAsync(options, Ct);
        var keys = await SeedMemoryAsync(harness, 2, Ct);

        var embedded = await Ingestor(harness).IngestMemoryAsync(
            RepoId, keys, Array.Empty<string>(), Ct);

        Assert.Multiple(() =>
        {
            Assert.That(injector.Failed, Is.EqualTo(1), "The page's member probe was faulted.");
            Assert.That(embedded, Is.EqualTo(2),
                "The arm still completes on the marker evidence alone rather than failing the run.");
        });
    }

    [Test]
    public async Task An_unreadable_marker_set_degrades_instead_of_failing_the_arm()
    {
        // The marker load is itself a whole-set range scan over the membership
        // tree, so it can time out under exactly the pressure it exists to
        // tolerate. Losing it must fall back to the source-id flag alone, not kill
        // the arm - the third instance of this seam in the same code path.
        var injector = new LatticeTreeFaultInjector
        {
            TreeId = RepoContextTrees.VectorMembership,
            // The scan reaches the tree through its shard root, so the fault is
            // aimed at the shard-level method the enumeration actually calls.
            Method = "GetSortedEntriesBatchAsync",
            IncludeShardGrains = true,
            FailFirst = int.MaxValue,
        };

        var options = new RepoContextMcpHarnessOptions
        {
            Posture = RepoContextMcpAuthPosture.Writer,
            ConfigureSilo = silo =>
            {
                silo.Services.AddSingleton(injector);
                silo.Services.AddSingleton<IIncomingGrainCallFilter, LatticeTreeFaultInjectingFilter>();
            },
        };

        await using var harness = await RepoContextMcpHarness.StartAsync(options, Ct);
        var keys = await SeedMemoryAsync(harness, 2, Ct);

        var embedded = await Ingestor(harness).IngestMemoryAsync(
            RepoId, keys, Array.Empty<string>(), Ct);

        Assert.Multiple(() =>
        {
            Assert.That(injector.Failed, Is.GreaterThan(0), "The marker read was faulted.");
            Assert.That(embedded, Is.EqualTo(2),
                "The arm still embeds rather than failing; it just loses the extra skip evidence "
                + "for this pass and may re-embed, which is the safe direction.");
        });
    }

    [Test]
    public async Task A_source_whose_membership_write_failed_is_not_marked_as_embedded()
    {
        // The marker is the recorded half of the orphan set (recorded - live), so
        // asserting an embedding that never stored leaves a phantom record behind.
        var injector = new LatticeTreeFaultInjector
        {
            TreeId = RepoContextTrees.VectorMembership,
            Method = nameof(ILattice.ApplyCrdtDeltaManyAsync),
            FailFirst = int.MaxValue,
        };

        var options = new RepoContextMcpHarnessOptions
        {
            Posture = RepoContextMcpAuthPosture.Writer,
            ConfigureSilo = silo =>
            {
                silo.Services.AddSingleton(injector);
                silo.Services.AddSingleton<IIncomingGrainCallFilter, LatticeTreeFaultInjectingFilter>();
            },
        };

        await using var harness = await RepoContextMcpHarness.StartAsync(options, Ct);
        var keys = await SeedMemoryAsync(harness, 2, Ct);
        var writer = harness.Services.GetRequiredService<RepoContextVectorWriter>();

        Assert.That(
            async () => await Ingestor(harness).IngestMemoryAsync(RepoId, keys, Array.Empty<string>(), Ct),
            Throws.InstanceOf<TimeoutException>(),
            "Nothing landed, so the arm still reports the fault rather than a clean zero.");

        var recorded = await writer.LoadEmbeddedMemoryKeysAsync(RepoId, Ct);
        Assert.Multiple(() =>
        {
            Assert.That(injector.Failed, Is.GreaterThan(0));
            Assert.That(recorded, Is.Empty,
                "No marker claims an embedding, because no membership write landed.");
        });
    }
}
