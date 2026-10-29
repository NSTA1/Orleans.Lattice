namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// One deliberate abstraction gap of a refinement note: one top-level bullet of
/// its <c>## Deliberate abstraction gaps</c> section, parsed by
/// <see cref="RefinementGapRegister"/>.
/// </summary>
/// <param name="LineNumber">The 1-based line the bullet starts on, for failure messages.</param>
/// <param name="Title">The bold lead-in, without its closing full stop.</param>
/// <param name="Prose">The bullet's text before its symbol clause, lines joined by spaces.</param>
/// <param name="Symbols">
/// The production symbols the bullet's <c>Production symbols:</c> clause names,
/// as written (<c>Type</c> or <c>Type.Member</c>). Empty when the bullet states
/// <see cref="NoSymbolReason"/> instead, or has no well-formed clause.
/// </param>
/// <param name="NoSymbolReason">
/// The reason a <c>No production symbol:</c> clause gives, or <see langword="null"/>
/// when the bullet names symbols instead.
/// </param>
internal sealed record RefinementGapEntry(
    int LineNumber,
    string Title,
    string Prose,
    IReadOnlyList<string> Symbols,
    string? NoSymbolReason);
