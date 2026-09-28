namespace Orleans.Lattice.Apps;

/// <summary>
/// The apps add-on's <see cref="ITreeOwnershipGuard"/>: bounds core tree aliasing by the tree ownership
/// ledger, so an alias from logical <c>L</c> to physical <c>P</c> is allowed iff the install that owns
/// <c>L</c> is the install that owns <c>DerivedFrom(P) ?? P</c> (either may be "no app"). Core's own
/// lifecycle aliases (resize, shadow restore, schema remediation) pass because their copies are derived
/// from the logical tree; aliases between two unowned trees are unchanged; every other alias that
/// would cross an ownership boundary is denied, for every caller including system origin.
/// </summary>
/// <remarks>
/// Consulted inside the registry's mutation turn, so it performs only point reads (see
/// <see cref="AppTreeOwnershipLedger.EvaluateAliasAsync"/>). A ledger failure propagates, which fails
/// the alias change closed.
/// </remarks>
internal sealed class AppTreeOwnershipGuard(AppTreeOwnershipLedger ledger) : ITreeOwnershipGuard
{
    private readonly AppTreeOwnershipLedger _ledger = ledger ?? throw new ArgumentNullException(nameof(ledger));

    /// <inheritdoc />
    public async ValueTask<TreeOwnershipDecision> AuthorizeAliasAsync(
        string logicalTreeId,
        string physicalTreeId,
        string? derivedFrom,
        CancellationToken cancellationToken = default)
    {
        var denial = await _ledger.EvaluateAliasAsync(logicalTreeId, physicalTreeId, derivedFrom, cancellationToken).ConfigureAwait(false);
        return denial is null ? TreeOwnershipDecision.Allow() : TreeOwnershipDecision.Deny(denial);
    }
}
