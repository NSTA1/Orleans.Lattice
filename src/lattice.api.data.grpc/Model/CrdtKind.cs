namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// The CRDT primitive a typed read targets, so the unified
/// <see cref="CrdtReadResponse"/> is decoded into the right logical shape. The
/// write side is discriminated by <see cref="CrdtWriteOp"/> instead, which
/// already names both the primitive and the mutation.
/// </summary>
[GenerateSerializer]
[Alias(GrpcDataTypeAliases.CrdtKind)]
public enum CrdtKind
{
    /// <summary>Positive-negative counter; read yields the converged total.</summary>
    PnCounter = 0,

    /// <summary>Observed-remove set; read yields the live members.</summary>
    OrSet = 1,

    /// <summary>Enable-wins flag; read yields the boolean state.</summary>
    OrFlag = 2,

    /// <summary>Remove-wins flag; read yields the boolean state.</summary>
    RwFlag = 3,

    /// <summary>Version vector; read yields per-replica clocks.</summary>
    VersionVector = 4,

    /// <summary>Multi-value register; read yields the concurrent values.</summary>
    MvRegister = 5,

    /// <summary>RGA sequence; read yields the ordered live elements.</summary>
    Sequence = 6,

    /// <summary>Observed-remove map; read yields per-field concurrent values.</summary>
    OrMap = 7,

    /// <summary>Grow-only counter; read yields the converged total.</summary>
    GCounter = 8,
    /// <summary>Grow-only set; read yields the members.</summary>
    GSet = 9,
    /// <summary>Remove-wins observed-remove set; read yields the live members.</summary>
    RwSet = 10,
    /// <summary>Monotone max register; read yields the current high-water value.</summary>
    MaxRegister = 11,

    /// <summary>Monotone min register; read yields the current low-water value.</summary>
    MinRegister = 12,
}
