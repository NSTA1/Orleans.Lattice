namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// Where a combobox finds the existing values it offers: the trees, regions,
/// principals or tenants the cluster already knows.
/// </summary>
/// <remarks>
/// <para>
/// A combobox asks on input, never on a timer, and has at most one query
/// outstanding: typing while a query runs cancels it, and the keys typed
/// meanwhile collapse into one query for the latest text. A source must
/// therefore honour <c>cancellationToken</c> promptly.
/// </para>
/// <para>
/// Anything a source remembers between queries belongs to the circuit and must
/// be keyed on the tenant the circuit asserts, so an answer read under one tenant
/// is never offered under another. A source that cannot list its values answers
/// <see cref="LtSuggestionSet.Unavailable"/>; a source that throws is treated the
/// same way, so a fault never breaks the field.
/// </para>
/// </remarks>
public interface ILtSuggestionSource
{
    /// <summary>Finds the existing values that match <paramref name="text"/>.</summary>
    /// <param name="text">What the user has typed so far; empty lists the first values.</param>
    /// <param name="limit">The most values to return.</param>
    /// <param name="cancellationToken">Cancelled when the user types again or leaves the field.</param>
    /// <returns>
    /// At most <paramref name="limit"/> values, best first. A value equal to
    /// <paramref name="text"/>, when one exists, must be included and first.
    /// </returns>
    ValueTask<LtSuggestionSet> SuggestAsync(string text, int limit, CancellationToken cancellationToken);
}
