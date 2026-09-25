using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Row-size admission bound for new atomic-write sagas (issue #3475): the
/// registry refuses new sagas with a retryable
/// <see cref="LatticeSaturatedException"/> before its persisted row can grow
/// past the storage provider's per-row limit.
/// </summary>
public partial class TxRegistryGrainTests
{
    private const int StorageRowLimitBytes = 1_000_000;

    [Test]
    public async Task EnsureSagaAdmissionAsync_admits_a_fresh_registry_without_writing()
    {
        var (grain, state) = CreateGrain(retention: TimeSpan.FromMinutes(1));

        await grain.EnsureSagaAdmissionAsync();

        Assert.That(state.WriteCount, Is.Zero, "The healthy path is a synchronous answer with no state write.");
    }

    [Test]
    public void EnsureSagaAdmissionAsync_refuses_with_TxRegistryCapacity_when_the_row_is_at_budget()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var (grain, state) = CreateGrain(
            options: new LatticeOptions
            {
                TxDecisionRetention = TimeSpan.FromMinutes(1),
                TxRegistryAdmissionBudgetBytes = 16 * 1024,
            },
            timeProvider: clock);
        SeedTombstones(state.State, 200, clock.GetUtcNow());
        Assume.That(grain.EstimatedRowBytes(), Is.GreaterThanOrEqualTo(16 * 1024));

        var ex = Assert.ThrowsAsync<LatticeSaturatedException>(() => grain.EnsureSagaAdmissionAsync());

        Assert.Multiple(() =>
        {
            Assert.That(ex!.SaturationSource, Is.EqualTo(LatticeSaturationSource.TxRegistryCapacity));
            Assert.That(ex.TreeId, Is.EqualTo("tree-x"));
            Assert.That(ex.Message, Does.Contain(nameof(LatticeOptions.TxRegistryAdmissionBudgetBytes)));
            Assert.That(state.WriteCount, Is.Zero, "Nothing had expired, so the refusal writes nothing.");
            Assert.That(state.State.ForgottenAt, Has.Count.EqualTo(200), "Unexpired tombstones are never reclaimed to make room.");
        });
    }

    [Test]
    public async Task Admitted_saga_operations_are_never_refused_over_budget()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var (grain, _) = CreateGrain(
            options: new LatticeOptions
            {
                TxDecisionRetention = TimeSpan.FromMinutes(1),
                TxRegistryAdmissionBudgetBytes = 1,
            },
            timeProvider: clock);
        var committed = Guid.NewGuid();
        var aborted = Guid.NewGuid();
        Assert.ThrowsAsync<LatticeSaturatedException>(() => grain.EnsureSagaAdmissionAsync());

        // Every call an already-admitted saga makes still succeeds: the bound
        // gates only the start of a new saga, never its completion.
        await grain.RegisterParticipantsAsync(committed, [0, 1]);
        await grain.RegisterParticipantAsync(committed, 2);
        await grain.MarkCommittedAsync(committed);
        await grain.RegisterParticipantsAsync(aborted, [0]);
        await grain.MarkAbortedAsync(aborted);
        var status = await grain.GetStatusAsync(committed);
        await grain.ForgetAsync(committed);
        await grain.ForgetAsync(aborted);

        Assert.That(status, Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task EnsureSagaAdmissionAsync_resumes_once_tombstones_age_out_of_retention()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var retention = TimeSpan.FromMinutes(1);
        var (grain, state) = CreateGrain(
            options: new LatticeOptions
            {
                TxDecisionRetention = retention,
                TxRegistryAdmissionBudgetBytes = 16 * 1024,
            },
            timeProvider: clock);
        SeedTombstones(state.State, 200, clock.GetUtcNow());
        Assert.ThrowsAsync<LatticeSaturatedException>(() => grain.EnsureSagaAdmissionAsync());
        var revisionBefore = await grain.GetDecisionsRevisionAsync();

        // An idle tree: no saga is left to call ForgetAsync and prune inline,
        // so the gate itself must reclaim the expired rows.
        clock.Advance(retention + TimeSpan.FromSeconds(1));
        await grain.EnsureSagaAdmissionAsync();
        var revisionAfter = await grain.GetDecisionsRevisionAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ForgottenAt, Is.Empty);
            Assert.That(state.State.Decisions, Is.Empty);
            Assert.That(state.WriteCount, Is.EqualTo(1), "The reclaim is one group-committed write.");
            Assert.That(state.State.TombstoneRetirementEpoch, Is.EqualTo(200));
            Assert.That(revisionAfter, Is.GreaterThanOrEqualTo(revisionBefore),
                "Retiring rows readers already saw as expired must never move the revision backwards.");
        });
    }

    [Test]
    public void EnsureSagaAdmissionAsync_failed_reclaim_write_restores_the_tombstones_and_propagates()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var retention = TimeSpan.FromMinutes(1);
        var (grain, state) = CreateGrain(
            options: new LatticeOptions
            {
                TxDecisionRetention = retention,
                TxRegistryAdmissionBudgetBytes = 16 * 1024,
            },
            timeProvider: clock);
        SeedTombstones(state.State, 200, clock.GetUtcNow());
        var revisionBefore = state.State.DecisionsRevision;
        clock.Advance(retention + TimeSpan.FromSeconds(1));
        state.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.ThrowsAsync<TxRegistryWriteFailedException>(() => grain.EnsureSagaAdmissionAsync());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ForgottenAt, Has.Count.EqualTo(200));
            Assert.That(state.State.Decisions, Has.Count.EqualTo(200));
            Assert.That(state.State.TombstoneRetirementEpoch, Is.Zero);
            Assert.That(state.State.DecisionsRevision, Is.EqualTo(revisionBefore));
        });
    }

    [Test]
    public async Task EnsureSagaAdmissionAsync_null_budget_disables_the_bound_and_the_row_outgrows_storage()
    {
        // The hazard the bound exists for: at the default 60 s retention and
        // about 150 sagas/s, the retained tombstones alone serialise past the
        // ~1 MB storage row limit. With the bound disabled nothing refuses.
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var (grain, state) = CreateGrain(
            options: new LatticeOptions
            {
                TxDecisionRetention = TimeSpan.FromMinutes(1),
                TxRegistryAdmissionBudgetBytes = null,
            },
            timeProvider: clock);
        SeedTombstones(state.State, 150 * 60, clock.GetUtcNow());

        await grain.EnsureSagaAdmissionAsync();

        Assert.That(JsonRowBytes(state.State), Is.GreaterThan(StorageRowLimitBytes));
    }

    [Test]
    public void EnsureSagaAdmissionAsync_default_budget_refuses_before_the_row_reaches_the_storage_limit()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var (grain, state) = CreateGrain(
            options: new LatticeOptions { TxDecisionRetention = TimeSpan.FromMinutes(1) },
            timeProvider: clock);
        var perTombstone = TxRegistryGrain.AdmissionEstimateDecisionBytes + TxRegistryGrain.AdmissionEstimateForgottenAtBytes;
        var atBudget = (int)((LatticeOptions.DefaultTxRegistryAdmissionBudgetBytes - TxRegistryGrain.AdmissionEstimateBaseBytes + perTombstone - 1) / perTombstone);
        SeedTombstones(state.State, atBudget, clock.GetUtcNow());

        Assert.ThrowsAsync<LatticeSaturatedException>(() => grain.EnsureSagaAdmissionAsync());
        var actual = JsonRowBytes(state.State);
        TestContext.Out.WriteLine(
            $"Default budget refuses at {atBudget:N0} tombstones (about {atBudget / 60:N0} sagas/s at 60 s retention); the row is then {actual:N0} JSON bytes.");
        Assert.That(actual, Is.LessThan(StorageRowLimitBytes * 0.85),
            "The default budget must leave headroom under the storage row limit for sagas admitted concurrently with the refusal.");
    }

    [Test]
    public void EstimatedRowBytes_never_under_counts_the_serialised_row()
    {
        var registry = new TxRegistryState();
        var now = DateTimeOffset.UtcNow;
        var treeId = "tree-with-a-realistically-long-physical-tree-identifier-0001";
        for (var i = 0; i < 400; i++)
        {
            var txid = Guid.NewGuid();
            registry.Decisions[txid] = TxStatus.Committed;
            registry.ForgottenAt[txid] = now;
        }
        for (var i = 0; i < 100; i++)
        {
            var txid = Guid.NewGuid();
            registry.Participants[txid] = [i, i + 1, i + 2, i + 3];
            registry.TerminalArrivals[txid] = [i, i + 1, i + 2, i + 3];
            registry.ExpectedTerminals[txid] = 4;
            registry.ExternalAuthorities[txid] = treeId + "/" + Guid.NewGuid();
            registry.ReceiverDecisionAuthorities[txid] = treeId + "/" + Guid.NewGuid();
        }
        for (var i = 0; i < 4; i++)
        {
            var pin = new SnapshotPin { ExpiresAt = now };
            foreach (var txid in registry.Decisions.Keys.Take(100)) pin.Txids.Add(txid);
            registry.SnapshotPins[Guid.NewGuid()] = pin;
        }

        var (grain, state) = CreateGrain();
        state.State = registry;

        var estimate = grain.EstimatedRowBytes();
        var actual = JsonRowBytes(registry);
        TestContext.Out.WriteLine($"Estimated {estimate:N0} bytes against {actual:N0} serialised JSON bytes.");
        Assert.That(estimate, Is.GreaterThanOrEqualTo(actual));
    }

    [TestCase(nameof(TxRegistryState.Decisions))]
    [TestCase(nameof(TxRegistryState.ForgottenAt))]
    [TestCase(nameof(TxRegistryState.Participants))]
    [TestCase(nameof(TxRegistryState.TerminalArrivals))]
    [TestCase(nameof(TxRegistryState.ExpectedTerminals))]
    [TestCase(nameof(TxRegistryState.ExternalAuthorities))]
    [TestCase(nameof(TxRegistryState.SnapshotPins))]
    public void Admission_estimate_weight_covers_the_measured_bytes_per_entry(string map)
    {
        const int entries = 200;
        var empty = JsonRowBytes(new TxRegistryState());
        var registry = new TxRegistryState();
        long weight;
        var now = DateTimeOffset.UtcNow;
        for (var i = 0; i < entries; i++)
        {
            var txid = Guid.NewGuid();
            switch (map)
            {
                case nameof(TxRegistryState.Decisions):
                    registry.Decisions[txid] = TxStatus.Committed;
                    break;
                case nameof(TxRegistryState.ForgottenAt):
                    registry.ForgottenAt[txid] = now;
                    break;
                case nameof(TxRegistryState.Participants):
                    registry.Participants[txid] = [100, 101, 102, 103];
                    break;
                case nameof(TxRegistryState.TerminalArrivals):
                    registry.TerminalArrivals[txid] = [100, 101, 102, 103];
                    break;
                case nameof(TxRegistryState.ExpectedTerminals):
                    registry.ExpectedTerminals[txid] = 1_000;
                    break;
                case nameof(TxRegistryState.ExternalAuthorities):
                    registry.ExternalAuthorities[txid] = "tree-with-a-realistically-long-physical-tree-id/" + Guid.NewGuid();
                    break;
                case nameof(TxRegistryState.SnapshotPins):
                    registry.SnapshotPins[txid] = new SnapshotPin { ExpiresAt = now, Txids = [Guid.NewGuid()] };
                    break;
            }
        }
        weight = map switch
        {
            nameof(TxRegistryState.Decisions) => TxRegistryGrain.AdmissionEstimateDecisionBytes,
            nameof(TxRegistryState.ForgottenAt) => TxRegistryGrain.AdmissionEstimateForgottenAtBytes,
            nameof(TxRegistryState.Participants) => TxRegistryGrain.AdmissionEstimateParticipantsBytes,
            nameof(TxRegistryState.TerminalArrivals) => TxRegistryGrain.AdmissionEstimateTerminalArrivalsBytes,
            nameof(TxRegistryState.ExpectedTerminals) => TxRegistryGrain.AdmissionEstimateExpectedTerminalsBytes,
            nameof(TxRegistryState.ExternalAuthorities) => TxRegistryGrain.AdmissionEstimateAuthorityBytes,
            _ => TxRegistryGrain.AdmissionEstimateSnapshotPinBytes + TxRegistryGrain.AdmissionEstimatePinnedTxidBytes,
        };

        var perEntry = (double)(JsonRowBytes(registry) - empty) / entries;
        TestContext.Out.WriteLine($"{map}: {perEntry:F1} JSON bytes per entry against a weight of {weight}.");
        Assert.Multiple(() =>
        {
            Assert.That(perEntry, Is.LessThanOrEqualTo(weight));
            Assert.That(empty, Is.LessThanOrEqualTo(TxRegistryGrain.AdmissionEstimateBaseBytes));
        });
    }

    private static void SeedTombstones(TxRegistryState registry, int count, DateTimeOffset forgottenAt)
    {
        for (var i = 0; i < count; i++)
        {
            var txid = Guid.NewGuid();
            registry.Decisions[txid] = TxStatus.Committed;
            registry.ForgottenAt[txid] = forgottenAt;
        }
    }

    private static int JsonRowBytes(TxRegistryState registry)
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        services.AddSingleton<OrleansJsonSerializer>();
        services.AddSingleton<JsonGrainStorageSerializer>();
        using var provider = services.BuildServiceProvider();
        return provider.GetRequiredService<JsonGrainStorageSerializer>().Serialize(registry).ToMemory().Length;
    }
}
