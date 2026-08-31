namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>
/// The sentences the rail says when it refuses, or when something is missing.
/// </summary>
/// <remarks>
/// <para>
/// Gathered in one type so a refusal always has the same shape - what happened,
/// then what to do about it - and so the epic's vocabulary work (#1853) has a
/// single place to retune the wording rather than a call site per state. Every
/// method composes once, at the point an entry is built, never per render.
/// </para>
/// <para>
/// Nothing here names an instance. A capability name is safe to reveal because
/// the gate is advisory and the server enforces; a tenant id or a tree id is
/// not, and none is reachable from this type.
/// </para>
/// </remarks>
public static class ExplorerAreaDenialCopy
{
    /// <summary>The label introducing a remedy, used for every refusal the rail states.</summary>
    public const string RemedyLabel = "What to do:";

    /// <summary>The heading of the group a demoted entry sits in.</summary>
    public const string DemotedGroupLabel = "Areas you cannot open";

    /// <summary>The term the capabilities affordance explains.</summary>
    public const string CapabilitiesTerm = "missing areas";

    /// <summary>The visible text on the capabilities affordance's trigger.</summary>
    public const string CapabilitiesTriggerText = "Why can I not see everything?";

    /// <summary>The label of the control that hides inaccessible areas.</summary>
    public const string HideInaccessibleLabel = "Hide areas I cannot open";

    /// <summary>
    /// Why an authenticated caller was refused <paramref name="areaLabel"/>.
    /// </summary>
    /// <param name="areaLabel">The area's label. Must not be null.</param>
    /// <exception cref="ArgumentNullException"><paramref name="areaLabel"/> is <see langword="null"/>.</exception>
    public static string DeniedExplanation(string areaLabel)
    {
        ArgumentNullException.ThrowIfNull(areaLabel);
        return "Your account does not hold the permission " + areaLabel + " requires in this cluster.";
    }

    /// <summary>
    /// What the caller can do about being refused <paramref name="areaLabel"/>.
    /// </summary>
    /// <param name="areaLabel">The area's label. Must not be null.</param>
    /// <exception cref="ArgumentNullException"><paramref name="areaLabel"/> is <see langword="null"/>.</exception>
    public static string DeniedRemedy(string areaLabel)
    {
        ArgumentNullException.ThrowIfNull(areaLabel);
        return "Ask a platform administrator to grant you access to " + areaLabel + ".";
    }

    /// <summary>
    /// Why <paramref name="areaLabel"/> cannot be opened yet by a caller who has
    /// not signed in.
    /// </summary>
    /// <param name="areaLabel">The area's label. Must not be null.</param>
    /// <exception cref="ArgumentNullException"><paramref name="areaLabel"/> is <see langword="null"/>.</exception>
    public static string SignInExplanation(string areaLabel)
    {
        ArgumentNullException.ThrowIfNull(areaLabel);
        return areaLabel + " opens once you sign in.";
    }

    /// <summary>
    /// What the caller does to open <paramref name="areaLabel"/>. Phrased as an
    /// invitation, because the entry stays clickable and performs it.
    /// </summary>
    /// <param name="areaLabel">The area's label. Must not be null.</param>
    /// <exception cref="ArgumentNullException"><paramref name="areaLabel"/> is <see langword="null"/>.</exception>
    public static string SignInRemedy(string areaLabel)
    {
        ArgumentNullException.ThrowIfNull(areaLabel);
        return "Choose " + areaLabel + " to sign in, then it opens.";
    }

    /// <summary>
    /// Why the shell is offering fewer areas than the product has, naming the
    /// capabilities this cluster does not have.
    /// </summary>
    /// <remarks>
    /// Names capabilities, never instances. An empty list yields the general
    /// sentence, so the affordance still answers the question when every area
    /// the caller is missing was withheld by the hide preference instead.
    /// </remarks>
    /// <param name="unavailableLabels">The labels of the areas the cluster does not have. Must not be null.</param>
    /// <exception cref="ArgumentNullException"><paramref name="unavailableLabels"/> is <see langword="null"/>.</exception>
    public static string CapabilitiesExplanation(IReadOnlyList<string> unavailableLabels)
    {
        ArgumentNullException.ThrowIfNull(unavailableLabels);

        const string preamble =
            "The rail offers the areas this cluster has and your account can reach. ";

        if (unavailableLabels.Count == 0)
        {
            return preamble + "Every area this cluster has is listed.";
        }

        // Composed once, when the gate set settles, rather than per render.
        var builder = new System.Text.StringBuilder(preamble);
        builder.Append("Not installed on this cluster: ");
        for (var i = 0; i < unavailableLabels.Count; i++)
        {
            if (i > 0)
            {
                builder.Append(", ");
            }

            builder.Append(unavailableLabels[i]);
        }

        return builder.Append('.').ToString();
    }

    /// <summary>
    /// What a caller does about a capability this cluster does not have.
    /// </summary>
    public const string CapabilitiesRemedy =
        "Ask a platform administrator whether the capability can be installed on this cluster.";

    /// <summary>
    /// Why the shell is showing the home surface while the address names
    /// <paramref name="areaLabel"/>, which the caller cannot open.
    /// </summary>
    /// <param name="areaLabel">The area's label. Must not be null.</param>
    /// <exception cref="ArgumentNullException"><paramref name="areaLabel"/> is <see langword="null"/>.</exception>
    public static string UnreachableAddressNotice(string areaLabel)
    {
        ArgumentNullException.ThrowIfNull(areaLabel);
        return "This address asks for " + areaLabel
            + ", which your account cannot open, so the Explore surface is shown instead.";
    }
}
