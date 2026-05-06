using Microsoft.Extensions.Logging.Abstractions;
using MultiSiteManufacturing.Host.Baseline;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Host.Lattice;
using MultiSiteManufacturing.Host.Replication;
using MultiSiteManufacturing.Tests.Federation;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using static MultiSiteManufacturing.Tests.Federation.FactFixtures;

namespace MultiSiteManufacturing.Tests.Replication;

/// <summary>
/// Unit tests for <see cref="BaselineReplicationApplier"/>: the
/// <see cref="IReplicationApplier"/> decorator that observes every
/// receiver-side <c>mfg-facts</c> apply, mirrors the decoded
/// <see cref="Fact"/> into the local
/// <see cref="BaselineFactBackend"/>, and raises
/// <see cref="FederationRouter.FactReplicated"/> so the dashboard's
/// "Inventory By Activity" tab refreshes live across regions.
/// </summary>
[TestFixture]
public sealed class BaselineReplicationApplierTests
{
    private FederationTestClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FederationTestClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    /// <summary>
    /// Hand-rolled <see cref="IReplicationApplier"/> stub returning a
    /// fixed <see cref="ApplyResult"/>. The repo doesn't reference
    /// NSubstitute / Moq from the sample test project, so the stub is
    /// done explicitly.
    /// </summary>
    private sealed class StubInnerApplier(ApplyResult result) : IReplicationApplier
    {
        public int ApplyCalls { get; private set; }
        public int BatchCalls { get; private set; }
        public ReplogEntry? LastEntry { get; private set; }
        public IReadOnlyList<ReplogEntry>? LastBatch { get; private set; }

        public Task<ApplyResult> ApplyAsync(ReplogEntry entry, CancellationToken cancellationToken = default)
        {
            ApplyCalls++;
            LastEntry = entry;
            return Task.FromResult(result);
        }

        public Task<ApplyResult> ApplyBatchAsync(
            IReadOnlyList<ReplogEntry> entries,
            CancellationToken cancellationToken = default)
        {
            BatchCalls++;
            LastBatch = entries;
            return Task.FromResult(result);
        }
    }

    private (BaselineReplicationApplier Decorator,
             StubInnerApplier Inner,
             BaselineFactBackend Baseline,
             FederationRouter Router,
             List<Fact> Replicated,
             PartCrdtStore CrdtStore,
             List<PartSerialNumber> CrdtChanged) Build(ApplyResult innerResult)
    {
        var (router, baseline, _) = _fixture.NewRouter();
        var inner = new StubInnerApplier(innerResult);

        var replicated = new List<Fact>();
        router.FactReplicated += (_, fact) => replicated.Add(fact);

        var crdtStore = _fixture.NewPartCrdtStore();
        var crdtChanged = new List<PartSerialNumber>();
        crdtStore.PartChanged += serial => crdtChanged.Add(serial);

        var decorator = new BaselineReplicationApplier(
            inner,
            baseline,
            router,
            crdtStore,
            NullLogger<BaselineReplicationApplier>.Instance);

        return (decorator, inner, baseline, router, replicated, crdtStore, crdtChanged);
    }

    private static ReplogEntry FactEntry(Fact fact, string treeId = LatticeFactBackend.FactTreeId)
    {
        var payload = FactJsonCodec.Encode(fact);
        return new ReplogEntry
        {
            TreeId = treeId,
            Op = ReplogOp.Set,
            Key = $"{fact.Serial.Value}/{fact.Hlc.WallClockTicks:D20}/{fact.Hlc.Counter:D10}/{fact.FactId:N}",
            Value = payload,
            Timestamp = fact.Hlc,
            IsTombstone = false,
            ExpiresAtTicks = 0,
            OriginClusterId = "us",
            Mode = ReplicationMode.LwwRegister,
        };
    }

    [Test]
    public async Task Successful_apply_for_facts_tree_mirrors_to_baseline_and_raises_FactReplicated()
    {
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-99001");
        var fact = Nc(serial, tick: 1, ncNumber: "NC-A-1", NcSeverity.Major, ProcessSite.ToulouseNdtLab);
        var (decorator, inner, baseline, _, replicated, _, _) = Build(
            new ApplyResult { Applied = true, HighWaterMark = fact.Hlc });
        var entry = FactEntry(fact);

        var result = await decorator.ApplyAsync(entry, CancellationToken.None);

        var facts = await baseline.GetFactsAsync(serial);
        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(facts, Has.Count.EqualTo(1));
            Assert.That(facts[0].FactId, Is.EqualTo(fact.FactId));
            Assert.That(replicated, Has.Count.EqualTo(1));
            Assert.That(replicated[0].FactId, Is.EqualTo(fact.FactId));
            Assert.That(inner.ApplyCalls, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Apply_that_inner_did_not_apply_does_not_fan_out()
    {
        // HWM-deduped, shadow-forward-deduped, parked-causal, and
        // local-origin-defence applies all return Applied=false. The
        // decorator must not surface them to the dashboard or to the
        // baseline backend - that would produce duplicate rows / events.
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-99002");
        var fact = Nc(serial, tick: 2, ncNumber: "NC-A-2", NcSeverity.Minor, ProcessSite.ToulouseNdtLab);
        var (decorator, _, baseline, _, replicated, _, _) = Build(
            new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero });
        var entry = FactEntry(fact);

        await decorator.ApplyAsync(entry, CancellationToken.None);

        var facts = await baseline.GetFactsAsync(serial);
        Assert.Multiple(() =>
        {
            Assert.That(replicated, Is.Empty, "Dashboard must not see deduped applies.");
            Assert.That(facts, Is.Empty, "Baseline must not mirror deduped applies.");
        });
    }

    [Test]
    public async Task Entry_for_unrelated_tree_is_ignored_even_when_applied()
    {
        // mfg-site-activity-index also replicates as LwwRegister; the
        // decorator must not interpret its payload as a Fact.
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-99003");
        var fact = Nc(serial, tick: 3, ncNumber: "NC-A-3", NcSeverity.Minor, ProcessSite.ToulouseNdtLab);
        var (decorator, _, baseline, _, replicated, _, _) = Build(
            new ApplyResult { Applied = true, HighWaterMark = fact.Hlc });
        var entry = FactEntry(fact, treeId: SiteActivityIndex.TreeId);

        await decorator.ApplyAsync(entry, CancellationToken.None);

        var facts = await baseline.GetFactsAsync(serial);
        Assert.Multiple(() =>
        {
            Assert.That(replicated, Is.Empty);
            Assert.That(facts, Is.Empty);
        });
    }

    [Test]
    public async Task Tombstone_and_delete_op_are_skipped_silently()
    {
        // Baseline has no fact-retraction concept; matches the prior
        // replay loop's documented behaviour.
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-99004");
        var fact = Nc(serial, tick: 4, ncNumber: "NC-A-4", NcSeverity.Minor, ProcessSite.ToulouseNdtLab);
        var (decorator, _, baseline, _, replicated, _, _) = Build(
            new ApplyResult { Applied = true, HighWaterMark = fact.Hlc });

        var tombstoneEntry = FactEntry(fact) with { IsTombstone = true };
        var deleteEntry = FactEntry(fact) with { Op = ReplogOp.Delete, Value = null };

        await decorator.ApplyAsync(tombstoneEntry, CancellationToken.None);
        await decorator.ApplyAsync(deleteEntry, CancellationToken.None);

        var facts = await baseline.GetFactsAsync(serial);
        Assert.Multiple(() =>
        {
            Assert.That(replicated, Is.Empty);
            Assert.That(facts, Is.Empty);
        });
    }

    [Test]
    public void Malformed_payload_is_swallowed_without_throwing()
    {
        // A payload that fails to decode must never propagate back into
        // the package's apply pipeline (which would surface as a 500 on
        // the inbound gRPC Push and stall replication).
        var (decorator, _, _, _, replicated, _, _) = Build(
            new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Zero });
        var entry = new ReplogEntry
        {
            TreeId = LatticeFactBackend.FactTreeId,
            Op = ReplogOp.Set,
            Key = "bad-key",
            Value = new byte[] { 0x7b, 0x99, 0x00 }, // "{" + invalid UTF-8
            Timestamp = HybridLogicalClock.Zero,
            OriginClusterId = "us",
            Mode = ReplicationMode.LwwRegister,
        };

        Assert.DoesNotThrowAsync(async () => await decorator.ApplyAsync(entry, CancellationToken.None));
        Assert.That(replicated, Is.Empty);
    }

    [Test]
    public async Task Batch_apply_fans_out_every_facts_tree_entry_when_inner_reports_applied()
    {
        var s1 = new PartSerialNumber("HPT-BLD-S1-2028-99005");
        var s2 = new PartSerialNumber("HPT-BLD-S1-2028-99006");
        var f1 = Nc(s1, tick: 1, ncNumber: "NC-B-1", NcSeverity.Minor, ProcessSite.ToulouseNdtLab);
        var f2 = Nc(s2, tick: 2, ncNumber: "NC-B-2", NcSeverity.Major, ProcessSite.ToulouseNdtLab);
        var (decorator, _, baseline, _, replicated, _, _) = Build(
            new ApplyResult { Applied = true, HighWaterMark = f2.Hlc });

        var batch = new[] { FactEntry(f1), FactEntry(f2) };

        var result = await decorator.ApplyBatchAsync(batch, CancellationToken.None);

        var facts1 = await baseline.GetFactsAsync(s1);
        var facts2 = await baseline.GetFactsAsync(s2);
        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(replicated.Select(f => f.FactId), Is.EquivalentTo(new[] { f1.FactId, f2.FactId }));
            Assert.That(facts1.Single().FactId, Is.EqualTo(f1.FactId));
            Assert.That(facts2.Single().FactId, Is.EqualTo(f2.FactId));
        });
    }

    [Test]
    public async Task Batch_apply_with_inner_not_applied_skips_fan_out()
    {
        var s1 = new PartSerialNumber("HPT-BLD-S1-2028-99007");
        var f1 = Nc(s1, tick: 1, ncNumber: "NC-B-3", NcSeverity.Minor, ProcessSite.ToulouseNdtLab);
        var (decorator, _, baseline, _, replicated, _, _) = Build(
            new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero });

        await decorator.ApplyBatchAsync(new[] { FactEntry(f1) }, CancellationToken.None);

        var facts = await baseline.GetFactsAsync(s1);
        Assert.Multiple(() =>
        {
            Assert.That(replicated, Is.Empty);
            Assert.That(facts, Is.Empty);
        });
    }

    [Test]
    public void ApplyBatchAsync_throws_on_null_entries()
    {
        var (decorator, _, _, _, _, _, _) = Build(default);
        Assert.ThrowsAsync<ArgumentNullException>(
            async () => await decorator.ApplyBatchAsync(null!, CancellationToken.None));
    }

    [Test]
    public async Task Successful_apply_for_labels_tree_raises_PartChanged_for_the_serial()
    {
        // Cross-cluster OR-Set label arrival: the package's apply
        // pipeline merges the delta into the local labels tree
        // transparently, so the decorator's only job is to surface
        // the change to the dashboard via PartCrdtStore.PartChanged.
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-99100");
        var (decorator, _, _, _, replicated, _, crdtChanged) = Build(
            new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Zero });
        var entry = new ReplogEntry
        {
            TreeId = PartCrdtStore.LabelsTreeId,
            Op = ReplogOp.Set,
            Key = serial.Value,
            Value = new byte[] { 0x01 }, // payload shape is opaque to the decorator
            Timestamp = HybridLogicalClock.Zero,
            OriginClusterId = "eu",
            Mode = ReplicationMode.OrSet,
        };

        await decorator.ApplyAsync(entry, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(crdtChanged, Has.Count.EqualTo(1));
            Assert.That(crdtChanged[0].Value, Is.EqualTo(serial.Value));
            Assert.That(replicated, Is.Empty, "Labels-tree applies must not surface as Fact mirroring.");
        });
    }

    [Test]
    public async Task Shadow_key_on_labels_tree_is_skipped_silently()
    {
        // Foreign-silo shadow keys do replicate (the labels tree is
        // opted into replication), but the receiving cluster's UI
        // never renders foreign-silo shadow state, so the decorator
        // must skip them rather than firing a no-op refresh.
        var (decorator, _, _, _, _, _, crdtChanged) = Build(
            new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Zero });
        var entry = new ReplogEntry
        {
            TreeId = PartCrdtStore.LabelsTreeId,
            Op = ReplogOp.Set,
            Key = "shadow/b/HPT-BLD-S1-2028-99101",
            Value = new byte[] { 0x01 },
            Timestamp = HybridLogicalClock.Zero,
            OriginClusterId = "eu",
            Mode = ReplicationMode.OrSet,
        };

        await decorator.ApplyAsync(entry, CancellationToken.None);

        Assert.That(crdtChanged, Is.Empty);
    }

    [Test]
    public async Task Labels_tree_apply_with_inner_not_applied_does_not_raise_PartChanged()
    {
        // Mirror of the facts-tree dedupe defence: an inner that
        // returns Applied=false (HWM-deduped, parked-causal, etc.)
        // must not produce a dashboard refresh.
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-99102");
        var (decorator, _, _, _, _, _, crdtChanged) = Build(
            new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero });
        var entry = new ReplogEntry
        {
            TreeId = PartCrdtStore.LabelsTreeId,
            Op = ReplogOp.Set,
            Key = serial.Value,
            Value = new byte[] { 0x01 },
            Timestamp = HybridLogicalClock.Zero,
            OriginClusterId = "eu",
            Mode = ReplicationMode.OrSet,
        };

        await decorator.ApplyAsync(entry, CancellationToken.None);

        Assert.That(crdtChanged, Is.Empty);
    }

    [Test]
    public async Task Tombstone_on_labels_tree_does_not_raise_PartChanged()
    {
        // Tombstones / deletes on the labels tree are produced by
        // shadow-key cleanup after heal; they don't change what
        // GetLabelsAsync would return for any user-visible serial.
        var (decorator, _, _, _, _, _, crdtChanged) = Build(
            new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Zero });
        var entry = new ReplogEntry
        {
            TreeId = PartCrdtStore.LabelsTreeId,
            Op = ReplogOp.Delete,
            Key = "HPT-BLD-S1-2028-99103",
            Value = null,
            IsTombstone = true,
            Timestamp = HybridLogicalClock.Zero,
            OriginClusterId = "eu",
            Mode = ReplicationMode.OrSet,
        };

        await decorator.ApplyAsync(entry, CancellationToken.None);

        Assert.That(crdtChanged, Is.Empty);
    }
}
