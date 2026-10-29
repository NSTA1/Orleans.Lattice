namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// What <see cref="RefinementDetectorRunnabilityIndex"/> found about one
/// <c>Fixture.Method</c> declaration in one test package: what would stop NUnit
/// running it, and the categories each of its runnable cases carries.
/// </summary>
internal sealed record RefinementDetectorRunnabilityState
{
    /// <summary>The fixture's simple name, as a refinement note cites it.</summary>
    public required string Fixture { get; init; }

    /// <summary>The test method's name.</summary>
    public required string Method { get; init; }

    /// <summary>The package: the directory under <c>test/</c> that holds the fixture.</summary>
    public required string Package { get; init; }

    /// <summary>
    /// The name NUnit reports, <c>Namespace.Fixture.Method</c> (a nested
    /// fixture joins its containing types with <c>+</c>), without case arguments.
    /// </summary>
    public required string FullName { get; init; }

    /// <summary>
    /// Every reason NUnit would not run the test: an <c>[Ignore]</c> or
    /// <c>[Explicit]</c> on the method, on any partial declaration of the
    /// fixture, or inherited from a base fixture, or every <c>[TestCase]</c> row
    /// ignored or explicit. Empty when the test runs.
    /// </summary>
    public required IReadOnlyList<string> Disqualifiers { get; init; }

    /// <summary>
    /// The categories of each runnable case: the method's, the fixture's across
    /// its partial declarations, those inherited from base fixtures, and the
    /// case row's own. A method with no <c>[TestCase]</c> rows has one case.
    /// </summary>
    public required IReadOnlyList<IReadOnlySet<string>> CaseCategories { get; init; }

    /// <summary>The repository-relative files the evidence was read from.</summary>
    public required IReadOnlyList<string> Sources { get; init; }

    /// <summary>The union of <see cref="CaseCategories"/>.</summary>
    public IReadOnlySet<string> Categories =>
        CaseCategories.SelectMany(categories => categories).ToHashSet(StringComparer.Ordinal);

    /// <inheritdoc />
    public override string ToString() => $"{Fixture}.{Method} (test/{Package})";
}
