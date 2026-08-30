namespace Orleans.Lattice.Explorer.Plugins.Tenants;

/// <summary>
/// A destructive operation held for explicit confirmation: what it will do, what
/// it will affect, and the exact words on the button that commits it.
/// <para>
/// The surface never performs a delete, a suspend, a revocation, or a rejection
/// from the click that requests it. The request builds one of these, the surface
/// renders it, and only an explicit confirm runs the operation - so an
/// irreversible action always costs two deliberate clicks and always shows its
/// blast radius first.
/// </para>
/// </summary>
public sealed record TenantConfirmation
{
    /// <summary>Which destructive operation is being confirmed.</summary>
    public required TenantConfirmationKind Kind { get; init; }

    /// <summary>The tenant the operation targets.</summary>
    public required string TenantId { get; init; }

    /// <summary>The dialog's heading.</summary>
    public required string Title { get; init; }

    /// <summary>
    /// What the operation will do, in words, including its blast radius - the
    /// number of trees a delete cascades through, the subject an admin
    /// revocation removes, or the access a grant revocation withdraws.
    /// </summary>
    public required string Body { get; init; }

    /// <summary>The label on the button that commits the operation.</summary>
    public required string ConfirmLabel { get; init; }

    /// <summary>
    /// A further caution rendered separately, or <see langword="null"/> when
    /// there is none. Used where the cluster is expected to refuse the operation
    /// outright, so the operator is told before they commit rather than after.
    /// </summary>
    public string? Caution { get; init; }

    /// <summary>
    /// The subject, region, or grant scope the operation names, or
    /// <see langword="null"/> for an operation that names only a tenant. Carried
    /// so the confirm handler needs no second piece of state to know what it was
    /// confirming.
    /// </summary>
    public string? Target { get; init; }

    /// <summary>
    /// The counterparty tenant for a grant transition, or <see langword="null"/>
    /// for every other operation.
    /// </summary>
    public string? CounterpartyTenantId { get; init; }
}
