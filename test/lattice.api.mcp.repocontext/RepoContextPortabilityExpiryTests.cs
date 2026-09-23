using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Testing;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Regression tests for issue #2825: a snapshot/restore round trip must not
/// silently convert a time-to-live entry into a durable one.
/// <para>
/// The distinction is load-bearing. An entry with a time-to-live is one whose
/// silent disappearance the writer judged acceptable; a durable entry is one whose
/// loss would be a problem. Promoting the first into the second permanently
/// resurrects records the store was entitled to shed, and it does so invisibly, so
/// the corruption only surfaces once the store is full of records nobody can
/// justify. These tests pin both directions: a live time-to-live survives the
/// round trip as a time-to-live, and one that elapsed while the snapshot sat at
/// rest is dropped on restore rather than reinstated live.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextPortabilityExpiryTests
{
    private static string SourceTree => $"repocontext-expiry-src-{Guid.NewGuid():N}";
    private static string TargetTree => $"repocontext-expiry-dst-{Guid.NewGuid():N}";

    private static HybridLogicalClock Clock(long ticks) => new() { WallClockTicks = ticks };

    private static byte[] Node(Serializer serializer, string path) =>
        serializer.SerializeToArray(new FileNode
        {
            RepoId = "acme",
            Path = path,
            Language = RepoContextValues.Lww("csharp", Clock(100)),
        });

    [Test]
    public async Task Export_then_import_preserves_a_ttl_entry_as_a_ttl_entry()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var source = harness.GrainFactory.GetGrain<ILattice>(SourceTree);
        var target = harness.GrainFactory.GetGrain<ILattice>(TargetTree);

        var ttlKey = RepoContextKeys.File("acme", "src/ephemeral.cs");
        var ttl = TimeSpan.FromHours(6);
        await source.SetAsync(
            ttlKey, Node(serializer, "src/ephemeral.cs"), ttl,
            TestContext.CurrentContext.CancellationToken);

        var before = await source.GetWithVersionAsync(ttlKey, TestContext.CurrentContext.CancellationToken);
        Assert.That(before!.ExpiresAtTicks, Is.Not.Zero, "Guard: the seeded entry really does carry a TTL.");

        using var stream = new MemoryStream();
        await RepoContextPortability.ExportAsync(
            source, RepoContextKeys.FilesPrefix("acme"), stream, serializer,
            cancellationToken: TestContext.CurrentContext.CancellationToken);

        stream.Position = 0;
        var result = await RepoContextPortability.ImportAsync(
            target, stream, serializer, cancellationToken: TestContext.CurrentContext.CancellationToken);

        var restored = await target.GetWithVersionAsync(ttlKey, TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(result.RecordsRead, Is.EqualTo(1));
            Assert.That(result.RecordsExpired, Is.Zero, "A live TTL entry is not an expired one.");
            Assert.That(restored!.Value, Is.Not.Null, "The entry must still be readable after the restore.");
            Assert.That(
                restored.ExpiresAtTicks,
                Is.Not.Zero,
                "The restored entry must still carry a finite expiry. A zero here is issue #2825: the "
                + "round trip promoted a shedable entry into a durable one that nothing will ever reclaim.");
        });

        // The reinstated life is re-derived from a remaining duration at write
        // time, so it may drift by the import's own latency. It must stay close to
        // the original instant and must never be extended into a fresh full TTL.
        var drift = TimeSpan.FromTicks(Math.Abs(restored!.ExpiresAtTicks - before.ExpiresAtTicks));
        Assert.That(
            drift,
            Is.LessThan(TimeSpan.FromMinutes(5)),
            "The restored expiry must track the captured instant, not restart the time-to-live.");
    }

    [Test]
    public async Task Export_then_import_leaves_a_durable_entry_durable()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var source = harness.GrainFactory.GetGrain<ILattice>(SourceTree);
        var target = harness.GrainFactory.GetGrain<ILattice>(TargetTree);

        var key = RepoContextKeys.File("acme", "src/durable.cs");
        await source.SetAsync(
            key, Node(serializer, "src/durable.cs"), TestContext.CurrentContext.CancellationToken);

        using var stream = new MemoryStream();
        await RepoContextPortability.ExportAsync(
            source, RepoContextKeys.FilesPrefix("acme"), stream, serializer,
            cancellationToken: TestContext.CurrentContext.CancellationToken);

        stream.Position = 0;
        var result = await RepoContextPortability.ImportAsync(
            target, stream, serializer, cancellationToken: TestContext.CurrentContext.CancellationToken);

        var restored = await target.GetWithVersionAsync(key, TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(result.RecordsRead, Is.EqualTo(1));
            Assert.That(result.RecordsExpired, Is.Zero);
            Assert.That(restored!.Value, Is.Not.Null);
            Assert.That(
                restored.ExpiresAtTicks,
                Is.Zero,
                "Carrying expiry through the snapshot must not invent one for a durable entry.");
        });
    }

    [Test]
    public async Task Import_drops_a_record_whose_ttl_elapsed_while_the_snapshot_was_at_rest()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var source = harness.GrainFactory.GetGrain<ILattice>(SourceTree);
        var target = harness.GrainFactory.GetGrain<ILattice>(TargetTree);

        var key = RepoContextKeys.File("acme", "src/doomed.cs");
        await source.SetAsync(
            key, Node(serializer, "src/doomed.cs"), TimeSpan.FromMinutes(10),
            TestContext.CurrentContext.CancellationToken);

        var vectorApplied = 0;
        RepoContextVectorExport export = (_, _) =>
            ValueTask.FromResult<RepoContextVectorPayload?>(new RepoContextVectorPayload([1, 2, 3], "onyx-v1"));
        RepoContextVectorImport import = (_, _, _) =>
        {
            vectorApplied++;
            return ValueTask.CompletedTask;
        };

        using var stream = new MemoryStream();
        await RepoContextPortability.ExportAsync(
            source, RepoContextKeys.FilesPrefix("acme"), stream, serializer, export,
            cancellationToken: TestContext.CurrentContext.CancellationToken);

        // The snapshot now sits at rest for longer than the entry had left to live.
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow.AddHours(1));

        stream.Position = 0;
        var result = await RepoContextPortability.ImportAsync(
            target, stream, serializer, vectorImport: import, timeProvider: clock,
            cancellationToken: TestContext.CurrentContext.CancellationToken);

        var restored = await target.GetAsync(key, TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(result.RecordsRead, Is.EqualTo(1), "The record is still read - it is simply not written.");
            Assert.That(result.RecordsExpired, Is.EqualTo(1));
            Assert.That(result.RecordsMerged, Is.Zero);
            Assert.That(result.RecordsWritten, Is.Zero);
            Assert.That(
                restored,
                Is.Null,
                "Reinstating an entry whose life already ran out is a resurrection of its own.");
            Assert.That(
                vectorApplied,
                Is.Zero,
                "Dropping the record must drop its vector too, leaving no orphaned evidence behind.");
        });
    }

    [Test]
    public async Task Import_of_a_ttl_record_over_a_durable_entry_leaves_it_durable()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var source = harness.GrainFactory.GetGrain<ILattice>(SourceTree);
        var target = harness.GrainFactory.GetGrain<ILattice>(TargetTree);

        var key = RepoContextKeys.File("acme", "src/contested.cs");
        await source.SetAsync(
            key, Node(serializer, "src/contested.cs"), TimeSpan.FromHours(2),
            TestContext.CurrentContext.CancellationToken);

        // The target already holds the same key durably - a stronger claim on the
        // entry's survival than the snapshot's time-to-live.
        await target.SetAsync(
            key, Node(serializer, "src/contested.cs"), TestContext.CurrentContext.CancellationToken);

        using var stream = new MemoryStream();
        await RepoContextPortability.ExportAsync(
            source, RepoContextKeys.FilesPrefix("acme"), stream, serializer,
            cancellationToken: TestContext.CurrentContext.CancellationToken);

        stream.Position = 0;
        var result = await RepoContextPortability.ImportAsync(
            target, stream, serializer, cancellationToken: TestContext.CurrentContext.CancellationToken);

        var restored = await target.GetWithVersionAsync(key, TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(result.RecordsMerged, Is.EqualTo(1));
            Assert.That(result.RecordsExpired, Is.Zero);
            Assert.That(
                restored!.ExpiresAtTicks,
                Is.Zero,
                "Durable wins the expiry join: an import must never impose a time-to-live on an entry "
                + "the target keeps forever.");
        });
    }

    [Test]
    public async Task Import_of_a_later_expiry_over_a_sooner_one_keeps_the_later_instant()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var source = harness.GrainFactory.GetGrain<ILattice>(SourceTree);
        var target = harness.GrainFactory.GetGrain<ILattice>(TargetTree);

        var key = RepoContextKeys.File("acme", "src/extended.cs");
        await source.SetAsync(
            key, Node(serializer, "src/extended.cs"), TimeSpan.FromHours(8),
            TestContext.CurrentContext.CancellationToken);
        await target.SetAsync(
            key, Node(serializer, "src/extended.cs"), TimeSpan.FromMinutes(30),
            TestContext.CurrentContext.CancellationToken);

        var sooner = await target.GetWithVersionAsync(key, TestContext.CurrentContext.CancellationToken);

        using var stream = new MemoryStream();
        await RepoContextPortability.ExportAsync(
            source, RepoContextKeys.FilesPrefix("acme"), stream, serializer,
            cancellationToken: TestContext.CurrentContext.CancellationToken);

        stream.Position = 0;
        await RepoContextPortability.ImportAsync(
            target, stream, serializer, cancellationToken: TestContext.CurrentContext.CancellationToken);

        var restored = await target.GetWithVersionAsync(key, TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(restored!.ExpiresAtTicks, Is.Not.Zero, "Neither side was durable, so neither is the join.");
            Assert.That(
                restored.ExpiresAtTicks,
                Is.GreaterThan(sooner!.ExpiresAtTicks),
                "Two finite expiries join to the later instant, matching the core value model.");
        });
    }

    [Test]
    public async Task Import_of_a_legacy_version_one_snapshot_restores_records_as_durable()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);
        var serializer = harness.Services.GetRequiredService<Serializer>();
        var target = harness.GrainFactory.GetGrain<ILattice>(TargetTree);

        var key = RepoContextKeys.File("acme", "src/legacy.cs");

        // A version-1 stream: the header stamped 1, and records that carry no
        // expiry because the format predates the member.
        using var stream = new MemoryStream();
        await RepoContextSnapshotFormat.WriteHeaderAsync(
            stream, 1, TestContext.CurrentContext.CancellationToken);
        await RepoContextSnapshotFormat.WriteFrameAsync(
            stream,
            serializer.SerializeToArray(new RepoContextSnapshotRecord
            {
                Key = key,
                Value = Node(serializer, "src/legacy.cs"),
            }),
            TestContext.CurrentContext.CancellationToken);

        stream.Position = 0;
        var result = await RepoContextPortability.ImportAsync(
            target, stream, serializer, cancellationToken: TestContext.CurrentContext.CancellationToken);

        var restored = await target.GetWithVersionAsync(key, TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(result.FormatVersion, Is.EqualTo(1), "A version-1 stream must still be readable.");
            Assert.That(result.RecordsRead, Is.EqualTo(1));
            Assert.That(result.RecordsExpired, Is.Zero);
            Assert.That(restored!.Value, Is.Not.Null);
            Assert.That(
                restored.ExpiresAtTicks,
                Is.Zero,
                "A stream that never captured an expiry can only honestly restore durable.");
        });
    }
}
