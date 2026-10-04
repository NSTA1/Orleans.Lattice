namespace Orleans.Lattice;

/// <summary>
/// Thrown by <c>TxRegistryGrain</c> when a snapshot capture's decision gate or
/// fence refuses a call (issue #4485). See <see cref="Refusal"/> for which call
/// and how the caller reacts.
/// <para>
/// This exception derives directly from <see cref="Exception"/> so the
/// same-silo deep copier Orleans registers for <see cref="Exception"/> covers
/// its base slice. It is part of the internal coordination protocol between the
/// registry, the saga grains, the replication apply path, and the snapshot
/// capture.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TxDecisionGateRefused)]
internal sealed class TxDecisionGateRefusedException : Exception
{
    /// <summary>Creates a new <see cref="TxDecisionGateRefusedException"/>.</summary>
    /// <param name="registryKey">The grain key of the refusing registry.</param>
    /// <param name="refusal">Which call was refused.</param>
    /// <param name="retryAfter">The remaining lease of the gate that refused the call.</param>
    public TxDecisionGateRefusedException(string registryKey, TxDecisionGateRefusal refusal, TimeSpan retryAfter)
        : base(refusal switch
        {
            TxDecisionGateRefusal.DecisionGated =>
                $"Saga decision registry '{registryKey}' is held by a snapshot capture's decision gate; the decision is deferred until the capture releases the gate (at most {retryAfter.TotalMilliseconds:F0}ms of lease remain).",
            TxDecisionGateRefusal.RegistrationFenced =>
                $"Saga decision registry '{registryKey}' is fenced by a cross-tree-consistent backup set capture; a new cross-tree atomic write cannot register on this tree until the capture completes. Retry the write.",
            _ =>
                $"The snapshot capture's decision gate on registry '{registryKey}' is no longer held; the capture cannot be accepted and must be retried.",
        })
    {
        RegistryKey = registryKey;
        Refusal = refusal;
        RetryAfterMilliseconds = (long)Math.Max(0, retryAfter.TotalMilliseconds);
    }

    /// <summary>Parameterless constructor for Orleans serialization.</summary>
    public TxDecisionGateRefusedException() { }

    /// <summary>The grain key of the refusing registry.</summary>
    [Id(0)] public string RegistryKey { get; set; } = string.Empty;

    /// <summary>Which call was refused.</summary>
    [Id(1)] public TxDecisionGateRefusal Refusal { get; set; }

    /// <summary>The remaining lease of the refusing gate, in milliseconds.</summary>
    [Id(2)] public long RetryAfterMilliseconds { get; set; }
}
