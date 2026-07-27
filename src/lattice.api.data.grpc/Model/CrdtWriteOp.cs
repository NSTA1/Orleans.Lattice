namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// The typed CRDT mutation a <see cref="CrdtWriteRequest"/> carries. Each value
/// names both the primitive and the operation, so the server dispatches straight
/// onto the matching <c>ILatticeDataApi</c> verb without a separate kind field.
/// </summary>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.CrdtWriteOp)]
public enum CrdtWriteOp
{
    /// <summary>PN-counter increment by <c>Amount</c> for <c>ReplicaId</c>.</summary>
    CounterIncrement = 0,

    /// <summary>PN-counter decrement by <c>Amount</c> for <c>ReplicaId</c>.</summary>
    CounterDecrement = 1,

    /// <summary>OR-Set add of <c>Element</c> for <c>ReplicaId</c>.</summary>
    SetAdd = 2,

    /// <summary>OR-Set observed-remove of <c>Element</c>.</summary>
    SetRemove = 3,

    /// <summary>OR-Flag enable for <c>ReplicaId</c>.</summary>
    OrFlagEnable = 4,

    /// <summary>OR-Flag disable (observed).</summary>
    OrFlagDisable = 5,

    /// <summary>RW-Flag enable for <c>ReplicaId</c>.</summary>
    RwFlagEnable = 6,

    /// <summary>RW-Flag disable for <c>ReplicaId</c>.</summary>
    RwFlagDisable = 7,

    /// <summary>Version-vector tick for <c>ReplicaId</c>.</summary>
    VersionVectorTick = 8,

    /// <summary>MV-Register set to <c>Element</c> for <c>ReplicaId</c>.</summary>
    RegisterSet = 9,

    /// <summary>Sequence insert of <c>Element</c> at <c>Index</c> for <c>ReplicaId</c>.</summary>
    SequenceInsertAt = 10,

    /// <summary>Sequence remove at <c>Index</c>.</summary>
    SequenceRemoveAt = 11,

    /// <summary>OR-Map put of <c>Element</c> under <c>Field</c> for <c>ReplicaId</c>.</summary>
    MapSet = 12,

    /// <summary>OR-Map observed-remove of <c>Field</c>.</summary>
    MapRemove = 13,
}
