namespace Orleans.Lattice.Explorer.Core.Vocabulary;

/// <summary>
/// What a gated surface says when it will not open: why, and what to do about
/// it.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="ExplorerStateCopy"/> answers "this list has nothing in it";
/// this answers "you cannot have this surface". They are separate because a
/// gated surface is named by its label rather than by what it lists, and a
/// refusal must always carry a remedy - a greyed-out entry that says nothing is
/// the failure mode this exists to prevent.
/// </para>
/// <para>
/// Gating in the Explorer is advisory: the cluster remains the sole enforcement
/// point, so this copy explains a decision rather than making one.
/// </para>
/// <para>
/// Every method composes, because a surface's label is a runtime value. A
/// caller therefore composes when its gate decision changes and not on the
/// render path - which is what the strip builders here already do.
/// </para>
/// </remarks>
public static class ExplorerAccessCopy
{
    /// <summary>The copy for a surface the caller's account may not use.</summary>
    /// <param name="surfaceLabel">The surface's label, as the user sees it.</param>
    /// <returns>The message, in the denial tone.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="surfaceLabel"/> is null.</exception>
    public static ExplorerStateMessage Denied(string surfaceLabel)
    {
        ArgumentNullException.ThrowIfNull(surfaceLabel);

        return new ExplorerStateMessage
        {
            Kind = ExplorerStateKind.NotPermitted,
            Headline = surfaceLabel + " is not available to your account",
            Explanation = "Your account does not hold the grant this cluster requires for "
                + surfaceLabel + ", so its data cannot be read.",
            Remedy = "Ask an operator to grant your account access to " + surfaceLabel + ".",
            TermId = ExplorerTermIds.Grant,
            DocsLink = ExplorerDocsLinks.ManagingAccess,
        };
    }

    /// <summary>The copy for a surface the cluster serves only to a signed-in identity.</summary>
    /// <param name="surfaceLabel">The surface's label, as the user sees it.</param>
    /// <returns>The message, in the denial tone.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="surfaceLabel"/> is null.</exception>
    public static ExplorerStateMessage SignInRequired(string surfaceLabel)
    {
        ArgumentNullException.ThrowIfNull(surfaceLabel);

        return new ExplorerStateMessage
        {
            Kind = ExplorerStateKind.SignInRequired,
            Headline = "Sign in to use " + surfaceLabel,
            Explanation = "This cluster serves " + surfaceLabel + " only to a signed-in identity.",
            Remedy = "Sign in to continue; the surface opens as soon as you are signed in.",
            ActionLabel = ExplorerVocabulary.SignInAction,
            TermId = ExplorerTermIds.SignInRequired,
            DocsLink = ExplorerDocsLinks.SigningIn,
        };
    }

    /// <summary>The copy for a surface whose feature this cluster does not run.</summary>
    /// <param name="surfaceLabel">The surface's label, as the user sees it.</param>
    /// <returns>The message. Not a denial: nothing is being withheld from the caller.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="surfaceLabel"/> is null.</exception>
    public static ExplorerStateMessage Unavailable(string surfaceLabel)
    {
        ArgumentNullException.ThrowIfNull(surfaceLabel);

        return new ExplorerStateMessage
        {
            Kind = ExplorerStateKind.Unavailable,
            Headline = surfaceLabel + " is not enabled on this cluster",
            Explanation = "The feature behind " + surfaceLabel
                + " is not running on the cluster you are connected to, so it has nothing to show.",
            Remedy = "Ask an operator to enable it, or connect to a cluster that already has.",
            TermId = ExplorerTermIds.NotAvailableHere,
            DocsLink = ExplorerDocsLinks.RunningTheExplorer,
        };
    }

    /// <summary>
    /// The copy for a gate decision, chosen by state, or <see langword="null"/>
    /// when the surface is allowed and therefore has nothing to explain.
    /// </summary>
    /// <param name="surfaceLabel">The surface's label, as the user sees it.</param>
    /// <param name="isAllowed">Whether the gate allows the surface.</param>
    /// <param name="requiresSignIn">Whether the refusal is recoverable by signing in.</param>
    /// <param name="isUnavailable">Whether the feature is absent from this cluster.</param>
    /// <returns>The message, or <see langword="null"/> when allowed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="surfaceLabel"/> is null.</exception>
    public static ExplorerStateMessage? For(
        string surfaceLabel,
        bool isAllowed,
        bool requiresSignIn = false,
        bool isUnavailable = false)
    {
        ArgumentNullException.ThrowIfNull(surfaceLabel);

        if (isAllowed)
        {
            return null;
        }

        if (requiresSignIn)
        {
            return SignInRequired(surfaceLabel);
        }

        return isUnavailable ? Unavailable(surfaceLabel) : Denied(surfaceLabel);
    }

    /// <summary>
    /// The refusal and its remedy as one sentence pair, for a control that has
    /// room for a single description string rather than a panel - a tab's
    /// accessible description, for instance.
    /// </summary>
    /// <param name="message">A message from this type or from <see cref="ExplorerStateCopy"/>.</param>
    /// <returns>The explanation, followed by the remedy when there is one.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="message"/> is null.</exception>
    public static string Describe(ExplorerStateMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);

        return string.IsNullOrEmpty(message.Remedy)
            ? message.Explanation
            : message.Explanation + " " + message.Remedy;
    }
}
