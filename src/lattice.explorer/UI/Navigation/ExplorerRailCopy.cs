using Orleans.Lattice.Explorer.Core.Vocabulary;

namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>
/// The few strings that belong to the rail itself rather than to the shared
/// vocabulary: the names of its two groupings, and the one sentence that is a
/// <em>list</em> rather than prose about a surface.
/// </summary>
/// <remarks>
/// <para>
/// Everything a refusal says comes from
/// <see cref="ExplorerAccessCopy"/> and the gate's own
/// <c>ExplorerAccessRemedy</c>, not from here. One concept has one name across
/// the UI, and a second copy layer for denials is exactly the drift the epic's
/// vocabulary work removes - so this type deliberately owns no refusal, no
/// sign-in prompt and no remedy of its own.
/// </para>
/// <para>
/// What is left is genuinely rail-shaped. "Areas you cannot open" names a
/// grouping no other surface has; the missing-areas sentence enumerates a set
/// the rail alone knows; and both hide-preference strings describe a control
/// only the rail renders.
/// </para>
/// </remarks>
public static class ExplorerRailCopy
{
    /// <summary>The name of the group a demoted entry sits in, below the divider.</summary>
    public const string DemotedGroupLabel = "Areas you cannot open";

    /// <summary>The term the missing-areas affordance explains.</summary>
    public const string MissingAreasTerm = "missing areas";

    /// <summary>The visible text on the missing-areas affordance's trigger.</summary>
    public const string MissingAreasTriggerText = "Why can I not see everything?";

    /// <summary>The label of the control that hides the areas the caller cannot open.</summary>
    public const string HideInaccessibleLabel = "Hide areas I cannot open";

    /// <summary>The answer when nothing is missing, so the affordance still answers its own question.</summary>
    private const string NothingMissing =
        "The rail offers the areas this cluster has and your account can reach. "
        + "Every area this cluster has is listed.";

    private const string MissingPreamble =
        "The rail offers the areas this cluster has and your account can reach. "
        + "Not installed on this cluster: ";

    /// <summary>
    /// Answers "why do I not see everything?", naming the capabilities this
    /// cluster does not run.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The rail composes the <em>list</em>, because it is the only thing that
    /// knows which areas withdrew; the <em>remedy</em> comes from
    /// <see cref="ExplorerAccessCopy.Unavailable(string)"/>, so an absent
    /// capability is answered with the same sentence wherever it is met.
    /// </para>
    /// <para>
    /// Naming a capability is safe: the gate is advisory and the cluster remains
    /// the sole enforcement point. Naming an <em>instance</em> would not be, and
    /// nothing reachable from here can.
    /// </para>
    /// </remarks>
    /// <param name="unavailableLabels">The labels of the areas the cluster does not serve. Must not be null.</param>
    /// <returns>The message the affordance discloses.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="unavailableLabels"/> is <see langword="null"/>.</exception>
    public static ExplorerStateMessage MissingAreas(IReadOnlyList<string> unavailableLabels)
    {
        ArgumentNullException.ThrowIfNull(unavailableLabels);

        if (unavailableLabels.Count == 0)
        {
            return new ExplorerStateMessage
            {
                Kind = ExplorerStateKind.Unavailable,
                Headline = "Every area is listed",
                Explanation = NothingMissing,
            };
        }

        // Composed once, when the set of withdrawn areas actually changes.
        var builder = new System.Text.StringBuilder(MissingPreamble);
        for (var i = 0; i < unavailableLabels.Count; i++)
        {
            if (i > 0)
            {
                builder.Append(", ");
            }

            builder.Append(unavailableLabels[i]);
        }

        var shared = ExplorerAccessCopy.Unavailable(unavailableLabels[0]);

        return new ExplorerStateMessage
        {
            Kind = ExplorerStateKind.Unavailable,
            Headline = "Some areas are not enabled on this cluster",
            Explanation = builder.Append('.').ToString(),
            Remedy = shared.Remedy,
            RemedyLabel = shared.RemedyLabel,
            TermId = shared.TermId,
            DocsLink = shared.DocsLink,
        };
    }

    /// <summary>
    /// Why the shell is showing the home surface while the address names
    /// <paramref name="areaLabel"/>, which the caller cannot open.
    /// </summary>
    /// <remarks>
    /// Rail-shaped rather than shared: it is about the <em>address</em> not being
    /// honoured, which no other surface has to explain. The refusal itself is
    /// still stated by <see cref="ExplorerAccessCopy"/> on the demoted entry.
    /// </remarks>
    /// <param name="areaLabel">The area's label. Must not be null.</param>
    /// <exception cref="ArgumentNullException"><paramref name="areaLabel"/> is <see langword="null"/>.</exception>
    public static string UnreachableAddressNotice(string areaLabel)
    {
        ArgumentNullException.ThrowIfNull(areaLabel);
        return "This address asks for " + areaLabel
            + ", which your account cannot open, so the "
            + ExplorerVocabulary.ExploreArea + " surface is shown instead.";
    }
}
