namespace Orleans.Lattice.Operations;

/// <summary>
/// Names the coordinated operation a <b>tracked grain call</b> works for. The
/// runner side of an operation passes it to a grain method that does long work in
/// one call; the grain opens a <see cref="LatticeOperationRelay"/> over it and
/// reports progress, and observes cancellation, straight against the operation's
/// tracking grain.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeOperationTicket)]
[Immutable]
internal sealed record LatticeOperationTicket
{
    /// <summary>The tracking grain key (see <see cref="LatticeOperationKey.For"/>).</summary>
    [Id(0)] public required string OperationKey { get; init; }

    /// <summary>Builds the ticket of an operation.</summary>
    /// <param name="tenantId">The owning tenant.</param>
    /// <param name="operationId">The operation id.</param>
    /// <returns>The ticket.</returns>
    public static LatticeOperationTicket For(string tenantId, string operationId) =>
        new() { OperationKey = LatticeOperationKey.For(tenantId, operationId) };
}
