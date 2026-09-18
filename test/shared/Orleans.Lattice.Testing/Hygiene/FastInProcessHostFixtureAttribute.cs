namespace Orleans.Lattice.Testing.Hygiene;

/// <summary>
/// Declares that a fixture genuinely constructs an in-process host - so
/// <c>IntegrationCategoryHygieneTestsBase</c> detects it - but has been
/// <b>measured</b> cheap enough to stay in the Tier 2 fast dev loop, and is
/// therefore deliberately exempt from the slow-category requirement.
/// <para>
/// This exists because detection and categorization are different questions.
/// Before issue #3142 a fixture that built its host inside a method body was
/// invisible to the gate, so it stayed in the fast loop by accident. Making
/// it visible must not force the opposite error - sweeping a 35 ms in-memory
/// <c>TestServer</c> fixture into the Integration tier purely because the
/// detector can now see it. The exemption converts an accident into a
/// declaration: the fixture is still counted, still listed, and the reason it
/// is not tagged is written down next to it.
/// </para>
/// <para>
/// An exemption is only legitimate when a measurement says so, so the gate
/// rejects a justification that does not carry one (see
/// <see cref="Justification"/>). The gate also rejects an exemption on a
/// fixture it does not detect, and an exemption on a fixture that already
/// carries a slow category - both are contradictions that would otherwise rot
/// silently as the fixture changes underneath them.
/// </para>
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class FastInProcessHostFixtureAttribute : Attribute
{
    /// <summary>
    /// Creates the exemption.
    /// </summary>
    /// <param name="justification">
    /// Why this host-building fixture is cheap enough for the fast loop,
    /// including the measured cost. See <see cref="Justification"/>.
    /// </param>
    public FastInProcessHostFixtureAttribute(string justification)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(justification);
        Justification = justification;
    }

    /// <summary>
    /// Why the fixture is exempt. The gate requires this to be substantive
    /// (at least <see cref="MinimumJustificationLength"/> characters) and to
    /// contain a digit, because the only defensible reason to exempt a
    /// host-building fixture is a measurement, and a measurement has a number
    /// in it. The check is a floor on effort, not a proof of accuracy - it
    /// cannot tell a real figure from an invented one, only an argued
    /// exemption from an unargued one.
    /// </summary>
    public string Justification { get; }

    /// <summary>
    /// Minimum length of a <see cref="Justification"/> the gate will accept.
    /// </summary>
    public const int MinimumJustificationLength = 40;

    /// <summary>
    /// True when <paramref name="justification"/> clears the bar the gate
    /// applies: at least <see cref="MinimumJustificationLength"/> characters
    /// once trimmed, and containing at least one digit.
    /// <para>
    /// The rule lives here rather than in the gate because it is a statement
    /// about this attribute's contract, not about any one scan. The digit is
    /// the load-bearing half: an exemption is only legitimate when a
    /// measurement says so, and a measurement has a number in it. Neither
    /// half can tell a real figure from an invented one - this is a floor on
    /// effort, not a proof of accuracy.
    /// </para>
    /// </summary>
    /// <param name="justification">The justification to check.</param>
    public static bool IsSubstantiveJustification(string? justification)
    {
        if (justification is null) return false;
        if (justification.AsSpan().Trim().Length < MinimumJustificationLength) return false;

        foreach (var c in justification)
            if (char.IsDigit(c)) return true;

        return false;
    }
}
