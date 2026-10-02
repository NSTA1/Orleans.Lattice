using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Operations;
using ApiState = Orleans.Lattice.Api.Operations.LatticeOperationState;
using EngineState = Orleans.Lattice.Operations.LatticeOperationState;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeOperationMapping"/>, the one shared mapping
/// from the engine's internal coordinated-operation record to the public status
/// and handle every facade exposes: every field is carried, every engine state
/// maps to its public twin, and the facade-only attributes stay private.
/// </summary>
[TestFixture]
public sealed class LatticeOperationMappingTests
{
    private static LatticeOperationRecord Record(EngineState state) => new()
    {
        OperationId = "op-1",
        Kind = "backup.restore",
        TenantId = "acme",
        TreeIds = ["t1", "t2"],
        State = state,
        Phase = "Applying",
        PhaseIndex = 1,
        PhaseCount = 3,
        CompletedUnits = 4,
        TotalUnits = 9,
        UnitName = "entries",
        StartedAtUtc = DateTimeOffset.UnixEpoch,
        FinishedAtUtc = DateTimeOffset.UnixEpoch.AddMinutes(1),
        FailureReason = "why",
        ResultReference = "bk-1",
        Result = new Dictionary<string, string> { ["k"] = "v" },
        CancelRequested = true,
        Attributes = new Dictionary<string, string> { ["scope.0.kind"] = "Prefix" },
    };

    [Test]
    public void ToStatus_carries_every_public_field()
    {
        var status = LatticeOperationMapping.ToStatus(Record(EngineState.Running));

        Assert.Multiple(() =>
        {
            Assert.That(status.OperationId, Is.EqualTo("op-1"));
            Assert.That(status.Kind, Is.EqualTo("backup.restore"));
            Assert.That(status.Scope.TenantId, Is.EqualTo("acme"));
            Assert.That(status.Scope.TreeIds, Is.EqualTo(new[] { "t1", "t2" }));
            Assert.That(status.State, Is.EqualTo(ApiState.Running));
            Assert.That(status.Phase, Is.EqualTo("Applying"));
            Assert.That(status.PhaseIndex, Is.EqualTo(1));
            Assert.That(status.PhaseCount, Is.EqualTo(3));
            Assert.That(status.CompletedUnits, Is.EqualTo(4));
            Assert.That(status.TotalUnits, Is.EqualTo(9));
            Assert.That(status.UnitName, Is.EqualTo("entries"));
            Assert.That(status.StartedAtUtc, Is.EqualTo(DateTimeOffset.UnixEpoch));
            Assert.That(status.FinishedAtUtc, Is.EqualTo(DateTimeOffset.UnixEpoch.AddMinutes(1)));
            Assert.That(status.FailureReason, Is.EqualTo("why"));
            Assert.That(status.ResultReference, Is.EqualTo("bk-1"));
            Assert.That(status.Result["k"], Is.EqualTo("v"));
            Assert.That(status.CancelRequested, Is.True);
            Assert.That(status.Result.ContainsKey("scope.0.kind"), Is.False, "Facade attributes are never public.");
        });
    }

    [TestCase(0, ApiState.Queued)]
    [TestCase(1, ApiState.Running)]
    [TestCase(2, ApiState.Succeeded)]
    [TestCase(3, ApiState.Failed)]
    [TestCase(4, ApiState.Cancelled)]
    public void Every_engine_state_maps_to_its_public_twin(int engine, ApiState api)
    {
        Assert.That(LatticeOperationMapping.ToStatus(Record((EngineState)engine)).State, Is.EqualTo(api));
    }

    [Test]
    public void The_engine_and_public_state_sets_match_exactly()
    {
        Assert.That(
            Enum.GetValues<EngineState>().Select(s => ((int)s, s.ToString())),
            Is.EqualTo(Enum.GetValues<ApiState>().Select(s => ((int)s, s.ToString()))));
    }

    [Test]
    public void ToHandle_carries_id_kind_scope_and_created()
    {
        var handle = LatticeOperationMapping.ToHandle(Record(EngineState.Queued), created: false);

        Assert.Multiple(() =>
        {
            Assert.That(handle.OperationId, Is.EqualTo("op-1"));
            Assert.That(handle.Kind, Is.EqualTo("backup.restore"));
            Assert.That(handle.Scope.TreeIds, Is.EqualTo(new[] { "t1", "t2" }));
            Assert.That(handle.Created, Is.False);
        });
    }

    [Test]
    public void Null_records_are_rejected()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => LatticeOperationMapping.ToStatus(null!), Throws.ArgumentNullException);
            Assert.That(() => LatticeOperationMapping.ToHandle(null!, true), Throws.ArgumentNullException);
        });
    }
}
