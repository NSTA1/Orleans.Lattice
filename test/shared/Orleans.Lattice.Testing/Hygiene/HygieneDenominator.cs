using NUnit.Framework;

namespace Orleans.Lattice.Testing.Hygiene;

/// <summary>
/// The anti-vacuity control shared by every hygiene gate: an assertion on
/// the DENOMINATOR of a scan - how much the gate examined - rather than on
/// the size of the match set it produced.
/// <para>
/// WHY THE DENOMINATOR AND NOT THE MATCHES. Every gate in this namespace
/// ends in <c>Assert.That(violations, Is.Empty)</c>, and an empty violation
/// list is the normal, desired outcome that must stay legal. The failure
/// this control exists to catch is the one that renders IDENTICALLY: a scan
/// that examined nothing at all also produces an empty violation list and
/// also reports a pass. Absence and success collapse onto the same output,
/// and the collapse always resolves in the reassuring direction, so the gate
/// goes quiet at exactly the moment it stops working.
/// </para>
/// <para>
/// The collapse is reachable without anyone doing anything obviously wrong.
/// <see cref="HygieneRepository.EnumerateFiles"/> yields nothing when its
/// root does not exist, by design and without complaint, so a slice root
/// that is renamed, moved, or mistyped in a scope registry silently reduces
/// that gate to a no-op. Nothing else in the system observes the difference.
/// </para>
/// <para>
/// The rule this class enforces is therefore: ASSERT THE DENOMINATOR OF THE
/// SCAN, NEVER THE SIZE OF THE MATCH SET. It is never conditioned on the
/// violation list, so it fails a misconfigured gate on a clean repository -
/// which is the only situation in which it can tell you anything you did not
/// already know.
/// </para>
/// <para>
/// This control answers "did the gate look at anything?". It does NOT answer
/// "did the gate look at everything?" - a scan whose enumeration silently
/// covers a subset of what it claims still passes here. That second question
/// is a totality check on the scope registry and is tracked separately.
/// </para>
/// </summary>
public static class HygieneDenominator
{
    /// <summary>
    /// Fails the calling test when a scan examined nothing.
    /// </summary>
    /// <param name="examined">
    /// The count of units the gate actually examined (files opened, types
    /// reflected over, marker blocks parsed). This is the denominator, not
    /// the number of violations found.
    /// </param>
    /// <param name="gate">
    /// The gate's name, used to make the failure self-describing.
    /// </param>
    /// <param name="unit">
    /// Plural noun for what was counted, e.g. <c>"text files"</c>.
    /// </param>
    /// <param name="source">
    /// Where the gate looked, so a failure names the thing to fix. For a
    /// file scan pass <see cref="Describe(HygieneScanScope)"/>.
    /// </param>
    public static void RequireExamined(int examined, string gate, string unit, string source)
    {
        ArgumentException.ThrowIfNullOrEmpty(gate);
        ArgumentException.ThrowIfNullOrEmpty(unit);

        Assert.That(examined, Is.GreaterThan(0),
            $"HYGIENE GATE VACUOUS: '{gate}' examined 0 {unit}, so it would have reported a pass "
            + "no matter what the repository contained. "
            + "This is a failure of the GATE, not of the content it checks - nothing here says the "
            + "repository is dirty, only that this run proved nothing about it. "
            + Environment.NewLine
            + $"Looked in: {source}"
            + Environment.NewLine
            + "Usual cause: a slice root in the scope registry no longer exists (renamed, moved, or "
            + "mistyped). Directory enumeration returns empty for a missing directory rather than "
            + "throwing, so the gate goes quiet instead of going red. Fix the scope, do not delete "
            + "this assertion.");
    }

    /// <summary>
    /// Renders a scope's roots for a failure message, so a vacuous scan names
    /// the directories it expected to find.
    /// </summary>
    public static string Describe(HygieneScanScope scope)
    {
        ArgumentNullException.ThrowIfNull(scope);

        var roots = scope.SliceRelativeRoots.Count == 0
            ? "<no slice roots configured>"
            : string.Join(", ", scope.SliceRelativeRoots);

        return scope.OwnsRepoLevelFiles
            ? $"slice roots [{roots}] plus the repo-level remainder outside "
              + $"{scope.OtherSliceRoots.Count} registered package slice root(s)"
            : $"slice roots [{roots}]";
    }
}
