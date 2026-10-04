namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The strength of a snapshot capture's hold on a saga decision registry
/// (issue #4485). See <see cref="ITxRegistryGrain.AcquireCaptureGateAsync"/>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TxRegistryCaptureGateMode)]
internal enum TxRegistryCaptureGateMode
{
    /// <summary>
    /// Refuses new cross-tree delegation registrations only. Decisions are
    /// still recorded. Held by a cross-tree-consistent backup set while it
    /// drains the in-flight cross-tree sagas, so the drain terminates.
    /// </summary>
    Fence = 0,

    /// <summary>
    /// Everything <see cref="Fence"/> refuses, and additionally refuses every
    /// call that would record a new decision, suppresses caching a delegated
    /// coordinator verdict, and answers terminal-intent status reads from local
    /// decisions only. The registry snapshots its local decisions (the capture's
    /// D0) when the gate is acquired.
    /// </summary>
    Gate = 1,
}
