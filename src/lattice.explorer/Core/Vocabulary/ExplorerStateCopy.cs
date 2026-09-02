using System.Collections.Frozen;

namespace Orleans.Lattice.Explorer.Core.Vocabulary;

/// <summary>
/// Writes the Explorer's empty, error and loading copy: one voice, and always an
/// answer to "why is there nothing here, and what do I do?".
/// </summary>
/// <remarks>
/// <para>
/// The Explorer used to say "No trees found." for four different situations -
/// an empty cluster, a tenant scope filtering everything out, a missing grant,
/// and a failed read - which left the reader to guess which one they were in.
/// Every method here names the situation and offers the action that resolves it.
/// </para>
/// <para>
/// This is also the copy layer a gated surface renders through, so a refusal
/// states its remedy rather than only its refusal. Hand
/// <see cref="ExplorerStateMessage.Explanation"/> and
/// <see cref="ExplorerStateMessage.Remedy"/> to the help primitive, and switch
/// its tone on <see cref="ExplorerStateMessage.IsDenial"/>.
/// </para>
/// <para>
/// The unparameterised message for every subject in
/// <see cref="ExplorerSubjects"/> and every <see cref="ExplorerStateKind"/> is
/// pre-built into a frozen table, so <see cref="For"/> is a hash probe and an
/// array index. Only an overload given a runtime value - the tenant in force,
/// the grant that was missing, the error the cluster returned - composes, and
/// then once, on a path that runs when a list is empty rather than per item.
/// </para>
/// </remarks>
public static class ExplorerStateCopy
{
    private static readonly int KindCount = Enum.GetValues<ExplorerStateKind>().Length;

    private static readonly ExplorerSubject[] KnownSubjects =
    [
        ExplorerSubjects.Trees,
        ExplorerSubjects.Views,
        ExplorerSubjects.TagIndexes,
        ExplorerSubjects.Tenants,
        ExplorerSubjects.Backups,
        ExplorerSubjects.Grants,
        ExplorerSubjects.DeadLetters,
        ExplorerSubjects.Entries,
        ExplorerSubjects.Changes,
        ExplorerSubjects.SchemaVersions,
        ExplorerSubjects.TelemetrySignals,
        ExplorerSubjects.Shards,
        ExplorerSubjects.Metrics,
        ExplorerSubjects.DetailSurfaces,
    ];

    private static readonly FrozenDictionary<string, ExplorerStateMessage[]> Prebuilt = BuildCache();

    /// <summary>
    /// The pre-built message for a subject in a state, using the general wording
    /// for that state. Prefer an overload below when a runtime value can make
    /// the copy specific.
    /// </summary>
    /// <param name="subject">What the surface lists.</param>
    /// <param name="kind">Why it has nothing to show.</param>
    /// <returns>The message.</returns>
    /// <exception cref="ArgumentException"><paramref name="subject"/> is the uninitialised default.</exception>
    public static ExplorerStateMessage For(ExplorerSubject subject, ExplorerStateKind kind)
    {
        Require(subject);

        if (Prebuilt.TryGetValue(subject.Id, out var messages) && (int)kind < messages.Length && (int)kind >= 0)
        {
            return messages[(int)kind];
        }

        return Compose(subject, kind);
    }

    /// <summary>The copy for a read still in flight.</summary>
    /// <param name="subject">What the surface lists.</param>
    /// <returns>The message.</returns>
    /// <exception cref="ArgumentException"><paramref name="subject"/> is the uninitialised default.</exception>
    public static ExplorerStateMessage Loading(ExplorerSubject subject) =>
        For(subject, ExplorerStateKind.Loading);

    /// <summary>
    /// The copy for a list that is empty because there is genuinely nothing to
    /// list - explicitly not a permissions or a scoping problem.
    /// </summary>
    /// <param name="subject">What the surface lists.</param>
    /// <returns>The message.</returns>
    /// <exception cref="ArgumentException"><paramref name="subject"/> is the uninitialised default.</exception>
    public static ExplorerStateMessage Empty(ExplorerSubject subject) =>
        For(subject, ExplorerStateKind.Empty);

    /// <summary>
    /// The copy for a list emptied by the tenant scope in force, naming the
    /// tenant when it is known.
    /// </summary>
    /// <param name="subject">What the surface lists.</param>
    /// <param name="tenantId">The active tenant, or <see langword="null"/> when the caller cannot name it.</param>
    /// <returns>The message.</returns>
    /// <exception cref="ArgumentException"><paramref name="subject"/> is the uninitialised default.</exception>
    public static ExplorerStateMessage ScopedOut(ExplorerSubject subject, string? tenantId = null)
    {
        Require(subject);

        if (string.IsNullOrEmpty(tenantId))
        {
            return For(subject, ExplorerStateKind.ScopedOut);
        }

        return Compose(subject, ExplorerStateKind.ScopedOut) with
        {
            Explanation = "The Explorer is scoped to the tenant '" + tenantId + "', which has no "
                + subject.Plural + ". Another tenant may have some.",
        };
    }

    /// <summary>
    /// The copy for a list the caller may not read, naming the grant when the
    /// gate knows it.
    /// </summary>
    /// <param name="subject">What the surface lists.</param>
    /// <param name="grant">The grant the cluster requires, or <see langword="null"/> when the caller cannot name it.</param>
    /// <returns>The message.</returns>
    /// <exception cref="ArgumentException"><paramref name="subject"/> is the uninitialised default.</exception>
    public static ExplorerStateMessage NotPermitted(ExplorerSubject subject, string? grant = null)
    {
        Require(subject);

        if (string.IsNullOrEmpty(grant))
        {
            return For(subject, ExplorerStateKind.NotPermitted);
        }

        return Compose(subject, ExplorerStateKind.NotPermitted) with
        {
            Remedy = "Ask an operator to grant your account '" + grant + "'.",
        };
    }

    /// <summary>The copy for a surface the cluster serves only to a signed-in identity.</summary>
    /// <param name="subject">What the surface lists.</param>
    /// <returns>The message.</returns>
    /// <exception cref="ArgumentException"><paramref name="subject"/> is the uninitialised default.</exception>
    public static ExplorerStateMessage SignInRequired(ExplorerSubject subject) =>
        For(subject, ExplorerStateKind.SignInRequired);

    /// <summary>The copy for a feature this cluster does not run.</summary>
    /// <param name="subject">What the surface lists.</param>
    /// <returns>The message.</returns>
    /// <exception cref="ArgumentException"><paramref name="subject"/> is the uninitialised default.</exception>
    public static ExplorerStateMessage Unavailable(ExplorerSubject subject) =>
        For(subject, ExplorerStateKind.Unavailable);

    /// <summary>
    /// The copy for a read that failed, quoting what the cluster said when there
    /// is something worth quoting.
    /// </summary>
    /// <param name="subject">What the surface lists.</param>
    /// <param name="detail">What went wrong, or <see langword="null"/> for the general wording.</param>
    /// <returns>The message.</returns>
    /// <exception cref="ArgumentException"><paramref name="subject"/> is the uninitialised default.</exception>
    public static ExplorerStateMessage Failed(ExplorerSubject subject, string? detail = null)
    {
        Require(subject);

        if (string.IsNullOrEmpty(detail))
        {
            return For(subject, ExplorerStateKind.Failed);
        }

        return Compose(subject, ExplorerStateKind.Failed) with
        {
            Explanation = "The cluster could not return the " + subject.Plural + ": " + detail,
        };
    }

    private static void Require(ExplorerSubject subject)
    {
        if (subject.IsEmpty)
        {
            throw new ArgumentException(
                "The subject is the uninitialised default; use one from ExplorerSubjects or declare one with an Id, Singular, Plural and CollectionLabel.",
                nameof(subject));
        }
    }

    private static FrozenDictionary<string, ExplorerStateMessage[]> BuildCache()
    {
        var entries = new Dictionary<string, ExplorerStateMessage[]>(KnownSubjects.Length, StringComparer.Ordinal);

        foreach (var subject in KnownSubjects)
        {
            var messages = new ExplorerStateMessage[KindCount];
            for (var kind = 0; kind < messages.Length; kind++)
            {
                messages[kind] = Compose(subject, (ExplorerStateKind)kind);
            }

            entries[subject.Id] = messages;
        }

        return entries.ToFrozenDictionary(StringComparer.Ordinal);
    }

    private static ExplorerStateMessage Compose(ExplorerSubject subject, ExplorerStateKind kind) => kind switch
    {
        ExplorerStateKind.Loading => new ExplorerStateMessage
        {
            Kind = kind,
            Headline = "Loading " + subject.Plural,
            Explanation = "Reading the " + subject.Plural + " this cluster holds.",
            TermId = subject.TermId,
            DocsLink = subject.DocsLink,
        },

        ExplorerStateKind.Empty => new ExplorerStateMessage
        {
            Kind = kind,
            Headline = "No " + subject.Plural + " yet",
            Explanation = "There are no " + subject.Plural
                + " to list here. Nothing is being hidden from you and nothing is being filtered out.",
            Remedy = "Create a " + subject.Singular
                + ", or check that you are connected to the cluster you expected.",
            TermId = subject.TermId,
            DocsLink = subject.DocsLink,
        },

        ExplorerStateKind.ScopedOut => new ExplorerStateMessage
        {
            Kind = kind,
            Headline = "No " + subject.Plural + " in this tenant",
            Explanation = "The Explorer is scoped to one tenant, and that tenant has no "
                + subject.Plural + ". Another tenant may have some.",
            Remedy = "Switch the active tenant, or list across every tenant you can reach.",
            ActionLabel = ExplorerVocabulary.ClearScopeAction,
            TermId = ExplorerTermIds.ActiveTenant,
            DocsLink = ExplorerDocsLinks.Tenancy,
        },

        ExplorerStateKind.NotPermitted => new ExplorerStateMessage
        {
            Kind = kind,
            Headline = "You cannot see " + subject.Plural + " here",
            Explanation = "Your account does not hold the grant this cluster requires to read "
                + subject.Plural + ", so none can be listed. This is not an empty list.",
            Remedy = "Ask an operator to grant your account access to " + subject.Plural + ".",
            TermId = ExplorerTermIds.Grant,
            DocsLink = ExplorerDocsLinks.ManagingAccess,
        },

        ExplorerStateKind.SignInRequired => new ExplorerStateMessage
        {
            Kind = kind,
            Headline = "Sign in to see " + subject.Plural,
            Explanation = "This cluster serves " + subject.Plural
                + " only to a signed-in identity, so there is nothing to show while you are anonymous.",
            Remedy = "Sign in, then open this surface again.",
            ActionLabel = ExplorerVocabulary.SignInAction,
            TermId = ExplorerTermIds.SignInRequired,
            DocsLink = ExplorerDocsLinks.SigningIn,
        },

        ExplorerStateKind.Unavailable => new ExplorerStateMessage
        {
            Kind = kind,
            Headline = "Not available on this cluster",
            Explanation = "The cluster you are connected to does not have " + subject.Plural
                + " enabled, so there is nothing for this surface to read.",
            Remedy = "Ask an operator to enable it, or connect to a cluster that already has.",
            TermId = ExplorerTermIds.NotAvailableHere,
            DocsLink = ExplorerDocsLinks.RunningTheExplorer,
        },

        _ => new ExplorerStateMessage
        {
            Kind = ExplorerStateKind.Failed,
            Headline = "Could not load " + subject.Plural,
            Explanation = "The cluster did not answer the request for " + subject.Plural + ".",
            Remedy = "Try again. If it keeps failing, check the connection settings and the cluster's health.",
            ActionLabel = ExplorerVocabulary.RetryAction,
            TermId = subject.TermId,
            DocsLink = subject.DocsLink,
        },
    };
}
