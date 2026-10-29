using Orleans.Lattice.Tests.Formal;

namespace Orleans.Lattice.Tests.Docs;

/// <summary>
/// The figures one TLA+ module contributes to the area census in
/// <c>docs/lattice/formal-coverage.md</c>, derived from the module itself rather
/// than from any prose about it.
/// <para>
/// Mutations and behaviour rows are the counts the module's manifest declares;
/// the Formal gates already hold those equal to the mutation directory and the
/// refinement note. Detectors are the distinct <c>Fixture.Method</c> names the
/// Detector cells of the note's behaviour-asserting rows cite, extracted with
/// the same parsers the Formal detector gates use, so the census and those gates
/// cannot disagree about what a note cites.
/// </para>
/// </summary>
/// <param name="Name">The module name.</param>
/// <param name="Mutations">The manifest's mutation count.</param>
/// <param name="BehaviourRows">The manifest's behaviour-row count.</param>
/// <param name="Detectors">The distinct detector names the behaviour-asserting rows cite.</param>
internal sealed record FormalCoverageModuleCensus(
    string Name,
    int Mutations,
    int BehaviourRows,
    IReadOnlySet<string> Detectors)
{
    /// <summary>Derives a module's census figures from its manifest and refinement note.</summary>
    /// <param name="module">The discovered module.</param>
    public static FormalCoverageModuleCensus Of(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var tables = module.ReadRefinementTables();
        var nonBehavioural = RefinementNote.NonBehaviouralRowLabels(module);
        var behaviourTables = new[] { tables[RefinementNote.ActionSection], tables[RefinementNote.PropertySection] };

        var detectors = RefinementCodeSymbols.ExtractDetectors(behaviourTables)
            .Where(d => !nonBehavioural.Contains(d.Row, StringComparer.Ordinal))
            .Select(d => d.Text)
            .ToHashSet(StringComparer.Ordinal);

        if (detectors.Count == 0)
        {
            throw new InvalidOperationException(
                $"{module.Describe(module.RefinementNotePath)} yielded no detector names from its behaviour-asserting "
                + "rows. The census would count zero detector tests for it, which is a parser or authoring fault, "
                + "not a figure.");
        }

        return new FormalCoverageModuleCensus(
            module.Name,
            module.Manifest.Counts.Mutations,
            module.Manifest.Counts.BehaviourRows,
            detectors);
    }
}
