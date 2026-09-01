namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// What a document-wide sweep of <c>role=tab</c> elements found: how many were examined
/// and which of them are not bound to a real <c>role=tabpanel</c>.
/// </summary>
/// <remarks>
/// <para>
/// The count travels with the problems on purpose. "No problems" is only meaningful
/// beside "and this many were examined" - a sweep that found no tabs at all reports a
/// clean result for a document it never looked at.
/// </para>
/// <para>
/// <see cref="Problems"/> is a concrete array rather than an
/// <c>IReadOnlyList&lt;string&gt;</c>: Playwright's evaluate-result converter
/// materialises the shape it is given and cannot construct an interface, so the
/// interface-typed version failed to deserialize at run time rather than at compile
/// time.
/// </para>
/// </remarks>
internal sealed record TabBindingReport
{
    /// <summary>How many <c>role=tab</c> elements the sweep examined.</summary>
    public int Examined { get; init; }

    /// <summary>One line per unbound tab, naming the tab and what was wrong with it.</summary>
    public string[] Problems { get; init; } = [];
}
