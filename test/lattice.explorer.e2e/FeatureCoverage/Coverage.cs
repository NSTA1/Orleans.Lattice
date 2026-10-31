namespace Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;

/// <summary>
/// Runtime feature coverage (#5361): page objects record a hit for a catalogue feature with
/// <see cref="Hit"/> only after the interaction and its assertion succeed, so a hit is
/// evidence, not a claim.
/// </summary>
/// <remarks>
/// <para>
/// The contract that makes a hit evidence: <see cref="Hit"/> only <em>proposes</em> a hit to the
/// enclosing <see cref="CoverageScope"/>. The scope commits its proposed hits when
/// <see cref="CoverageScope.Complete"/> is reached - after every step it encloses has finished
/// without a failed assertion - and discards them otherwise. A page object that records a hit
/// before its assertion therefore gains nothing: when the assertion fails, the hit is rejected
/// with the scope. Each chapter runs in a scope named after it, and a step checkpoint may open a
/// nested scope per step so a later step's failure keeps the earlier steps' evidence.
/// </para>
/// <para>
/// <see cref="Hit"/> refuses an id the catalogue lacks, and a call outside any scope.
/// </para>
/// </remarks>
public static class Coverage
{
    /// <summary>Proposes a hit for <paramref name="featureId"/> to the enclosing scope.</summary>
    /// <param name="featureId">A catalogue feature id, such as <c>command:data.refresh</c>.</param>
    /// <exception cref="ArgumentException"><paramref name="featureId"/> is not in the catalogue.</exception>
    /// <exception cref="InvalidOperationException">No coverage scope is open.</exception>
    public static void Hit(string featureId) => CoverageRecorder.Current.Hit(featureId);

    /// <summary>Opens a scope whose proposed hits commit only on <see cref="CoverageScope.Complete"/>.</summary>
    /// <param name="name">The scope's name: a chapter id, or a step id such as <c>A3.4</c>.</param>
    public static CoverageScope BeginScope(string name) => CoverageRecorder.Current.BeginScope(name);
}
