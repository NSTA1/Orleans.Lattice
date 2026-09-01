using Orleans.Lattice.Explorer.Core.Vocabulary;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The access-control jargon this area puts in front of a reader, explained once
/// here and rendered at the point of use through the help disclosure.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a plugin-owned table rather than more entries in
/// <see cref="ExplorerGlossary"/>.</b> The shared glossary names the concepts the
/// <em>whole</em> Explorer shares. A rule's effect, a subject selector and a
/// rule's scope are meaningful only inside this area, and the Explorer's
/// assembly graph puts the shared glossary in a package this one consumes rather
/// than owns. So the terms live with the surface that says them, and reuse the
/// shared <see cref="ExplorerTerm"/> shape so a help disclosure renders them
/// identically.
/// </para>
/// <para>
/// Anything the shared glossary already names is <em>taken from it</em> rather
/// than reworded here - see <see cref="Grant"/> and <see cref="AdminSubject"/>.
/// One concept, one wording.
/// </para>
/// </remarks>
public static class AccessVocabulary
{
    /// <summary>The id of the <see cref="Rule"/> term.</summary>
    public const string RuleId = "access-rule";

    /// <summary>The id of the <see cref="SubjectSelector"/> term.</summary>
    public const string SubjectSelectorId = "subject-selector";

    /// <summary>The id of the <see cref="Effect"/> term.</summary>
    public const string EffectId = "rule-effect";

    /// <summary>The id of the <see cref="Scope"/> term.</summary>
    public const string ScopeId = "rule-scope";

    /// <summary>The id of the <see cref="Precedence"/> term.</summary>
    public const string PrecedenceId = "rule-precedence";

    /// <summary>One authored statement about who may do what.</summary>
    public static ExplorerTerm Rule { get; } = new()
    {
        Id = RuleId,
        Label = "Rule",
        Explanation =
            "A rule is one authored statement about who may do what: a subject, a scope, and whether the "
            + "permissions it names are allowed or denied. A decision is made by taking every rule that "
            + "matches and ranking them.",
        DocsLink = ExplorerDocsLinks.ManagingAccess,
    };

    /// <summary>Who a rule is about.</summary>
    public static ExplorerTerm SubjectSelector { get; } = new()
    {
        Id = SubjectSelectorId,
        Label = "Subject selector",
        Explanation =
            "The subject selector says who a rule is about: one identity, everyone in a group, or everyone. "
            + "A narrower selector outranks a wider one when two rules would otherwise disagree.",
        DocsLink = ExplorerDocsLinks.ManagingAccess,
    };

    /// <summary>Whether a rule grants or refuses.</summary>
    public static ExplorerTerm Effect { get; } = new()
    {
        Id = EffectId,
        Label = "Effect",
        Explanation =
            "A rule's effect is Allow or Deny. Where two rules of equal standing disagree, Deny wins, so a "
            + "refusal cannot be undone by adding another rule beside it.",
        DocsLink = ExplorerDocsLinks.ManagingAccess,
    };

    /// <summary>How much of the keyspace a rule covers.</summary>
    public static ExplorerTerm Scope { get; } = new()
    {
        Id = ScopeId,
        Label = "Scope",
        Explanation =
            "A rule's scope is how much it covers: a whole tree, or a prefix of keys within one. A narrower "
            + "scope outranks a wider one, so a rule about one prefix beats a rule about the whole tree.",
        DocsLink = ExplorerDocsLinks.ManagingAccess,
    };

    /// <summary>How competing rules are ordered.</summary>
    public static ExplorerTerm Precedence { get; } = new()
    {
        Id = PrecedenceId,
        Label = "Precedence",
        Explanation =
            "Precedence is the order matching rules are ranked in: the more specific subject and the "
            + "narrower scope come first, and Deny beats Allow at equal standing. It is why a decision can "
            + "be explained rather than only observed.",
        DocsLink = ExplorerDocsLinks.ManagingAccess,
    };

    /// <summary>
    /// The authority a rule confers, taken from the shared glossary rather than
    /// reworded, because it is the same concept every gated area names.
    /// </summary>
    public static ExplorerTerm Grant { get; } = ExplorerGlossary.Get(ExplorerTermIds.Grant);

    /// <summary>
    /// An identity that administers, taken from the shared glossary rather than
    /// reworded.
    /// </summary>
    public static ExplorerTerm AdminSubject { get; } = ExplorerGlossary.Get(ExplorerTermIds.AdminSubject);
}
