using Orleans.Lattice.Operations;
using Orleans.Lattice.Tests.Storage;

namespace Orleans.Lattice.Tests.Operations;

/// <summary>
/// Issue #5579: an operation record must survive the JSON grain-storage round
/// trip that Azure Table storage performs on every reactivation. In-memory
/// storage and the Orleans serializer never go through JSON, so the rest of
/// this fixture cannot see the defect.
/// </summary>
public sealed partial class LatticeOperationGrainTests
{
    [Test]
    public async Task A_record_written_by_the_grain_reloads_from_json_grain_storage()
    {
        var grain = CreateGrain();
        await grain.BeginAsync(new LatticeOperationBeginRequest
        {
            Kind = "test.kind",
            TreeIds = new List<string> { "tree-a", "tree-b" },
            Phases = new List<string> { "A", "B" },
            Attributes = new Dictionary<string, string> { ["scope"] = "whole" },
            RunnerSilo = RunnerSilo,
        });
        await grain.CompleteAsync(LatticeOperationCompletion.Succeeded("b1", new Dictionary<string, string> { ["backupId"] = "b1" }));
        using var storage = new SiloJsonGrainStorage();

        var reloaded = storage.Json.Deserialize<LatticeOperationGrainState>(storage.Json.Serialize(_state.State)).Record!;

        Assert.Multiple(() =>
        {
            Assert.That(reloaded.TreeIds, Is.EqualTo(new[] { "tree-a", "tree-b" }));
            Assert.That(reloaded.Phases, Is.EqualTo(new[] { "A", "B" }));
            Assert.That(reloaded.Attributes["scope"], Is.EqualTo("whole"));
            Assert.That(reloaded.Result["backupId"], Is.EqualTo("b1"));
            Assert.That(reloaded.State, Is.EqualTo(LatticeOperationState.Succeeded));
            Assert.That(reloaded.RunnerSilo, Is.EqualTo(RunnerSilo));
            Assert.That(LatticeOperationRecord.EmptyResult, Is.Empty,
                "A read must never populate the shared empty map in place.");
        });
    }

    [Test]
    public async Task A_record_with_one_tree_and_one_phase_reloads_from_json_grain_storage()
    {
        var grain = CreateGrain();
        await grain.BeginAsync(Begin("test.kind", "only"));
        using var storage = new SiloJsonGrainStorage();

        var reloaded = storage.Json.Deserialize<LatticeOperationGrainState>(storage.Json.Serialize(_state.State)).Record!;

        Assert.Multiple(() =>
        {
            Assert.That(reloaded.TreeIds, Is.EqualTo(new[] { "tree-a" }));
            Assert.That(reloaded.Phases, Is.EqualTo(new[] { "only" }));
        });
    }

    [Test]
    public async Task The_grains_record_collections_stay_read_only()
    {
        var grain = CreateGrain();
        await grain.BeginAsync(Begin("test.kind", "A", "B"));

        var record = _state.State.Record!;
        Assert.Multiple(() =>
        {
            Assert.That(record.TreeIds, Is.Not.InstanceOf<string[]>(), "A reader must not be able to write into the record.");
            Assert.That(((ICollection<string>)record.Phases).IsReadOnly, Is.True);
        });
    }

    [Test]
    public void A_row_already_written_with_synthesized_collections_reads_back()
    {
        IReadOnlyList<string> trees = [.. new[] { "tree-a", "tree-b" }];
        IReadOnlyList<string> phases = ["only"];
        var legacy = new LatticeOperationGrainState
        {
            Record = new LatticeOperationRecord
            {
                OperationId = OperationId,
                Kind = "test.kind",
                TenantId = Tenant,
                State = LatticeOperationState.Running,
                Phase = "only",
                TreeIds = trees,
                Phases = phases,
                RunnerSilo = RunnerSilo,
            },
            LastHeartbeatUtc = new DateTimeOffset(2026, 10, 1, 0, 0, 0, TimeSpan.Zero),
            IndexOutboxInitialized = true,
        };
        using var storage = new SiloJsonGrainStorage();
        var payload = storage.Json.Serialize(legacy);
        Assert.That(payload.ToString(), Does.Contain("<>z__ReadOnly"),
            "The row must carry the compiler-synthesized type names, or this test proves nothing.");

        var reloaded = storage.Lattice.Deserialize<LatticeOperationGrainState>(payload);

        Assert.Multiple(() =>
        {
            Assert.That(reloaded.Record!.TreeIds, Is.EqualTo(new[] { "tree-a", "tree-b" }));
            Assert.That(reloaded.Record.Phases, Is.EqualTo(new[] { "only" }));
            Assert.That(reloaded.Record.State, Is.EqualTo(LatticeOperationState.Running));
            Assert.That(reloaded.IndexOutboxInitialized, Is.True);
        });
    }
}
