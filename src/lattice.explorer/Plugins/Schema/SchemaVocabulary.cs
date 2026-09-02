using Orleans.Lattice.Explorer.Core.Vocabulary;

namespace Orleans.Lattice.Explorer.Schema;

/// <summary>
/// The schema-governance jargon this area puts in front of a reader, explained
/// once here and rendered at the point of use through the help disclosure.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a plugin-owned table rather than more entries in
/// <see cref="ExplorerGlossary"/>.</b> The shared glossary names the concepts the
/// <em>whole</em> Explorer shares. A version config, a remediation and a
/// compliance scan are meaningful only inside this area, and the Explorer's
/// assembly graph puts the shared glossary in a package this one consumes rather
/// than owns. So the terms live with the surface that says them, and reuse the
/// shared <see cref="ExplorerTerm"/> shape so a help disclosure renders them
/// identically.
/// </para>
/// <para>
/// Anything the shared glossary already names is <em>taken from it</em> rather
/// than reworded here - see <see cref="StrictSchema"/> and
/// <see cref="DeadLetter"/>. One concept, one wording.
/// </para>
/// </remarks>
public static class SchemaVocabulary
{
    /// <summary>The id of the <see cref="SchemaPolicy"/> term.</summary>
    public const string SchemaPolicyId = "schema-policy";

    /// <summary>The id of the <see cref="VersionConfig"/> term.</summary>
    public const string VersionConfigId = "version-config";

    /// <summary>The id of the <see cref="Remediation"/> term.</summary>
    public const string RemediationId = "schema-remediation";

    /// <summary>The id of the <see cref="ComplianceScan"/> term.</summary>
    public const string ComplianceScanId = "compliance-scan";

    /// <summary>What a tree accepts, and what it does with a write that does not fit.</summary>
    public static ExplorerTerm SchemaPolicy { get; } = new()
    {
        Id = SchemaPolicyId,
        Label = "Schema policy",
        Explanation =
            "A tree's schema policy says what its values must look like and what happens to a write that "
            + "does not fit: accepted anyway, or rejected and set aside. Governance is opt-in per tree, so a "
            + "tree with no policy accepts whatever is written to it.",
        DocsLink = ExplorerDocsLinks.SchemaEnforcement,
    };

    /// <summary>How a tree's value shape is allowed to change over time.</summary>
    public static ExplorerTerm VersionConfig { get; } = new()
    {
        Id = VersionConfigId,
        Label = "Version config",
        Explanation =
            "The version config records which shapes of a value a tree currently accepts, and which one new "
            + "writes are stamped with. It is how a value's shape is allowed to change without every older "
            + "value becoming unreadable at once.",
        DocsLink = ExplorerDocsLinks.ManagingSchema,
    };

    /// <summary>Bringing values written under an older shape up to the current one.</summary>
    public static ExplorerTerm Remediation { get; } = new()
    {
        Id = RemediationId,
        Label = "Remediation",
        Explanation =
            "Remediation rewrites values that were stored under an older shape so they match the current "
            + "one. It is what lets an old version finally be retired, because nothing is left that still "
            + "needs it.",
        DocsLink = ExplorerDocsLinks.ManagingSchema,
    };

    /// <summary>Reading a tree to find what does not match its policy.</summary>
    public static ExplorerTerm ComplianceScan { get; } = new()
    {
        Id = ComplianceScanId,
        Label = "Compliance scan",
        Explanation =
            "A compliance scan reads what is already stored and reports how much of it would satisfy the "
            + "policy now in force. It changes nothing: it is how the cost of turning enforcement on is "
            + "found out before it is turned on.",
        DocsLink = ExplorerDocsLinks.SchemaEnforcement,
    };

    /// <summary>
    /// Rejecting a non-conforming write outright, taken from the shared glossary
    /// rather than reworded.
    /// </summary>
    public static ExplorerTerm StrictSchema { get; } = ExplorerGlossary.Get(ExplorerTermIds.StrictSchema);

    /// <summary>
    /// The queue a rejected write is set aside in, taken from the shared glossary
    /// rather than reworded.
    /// </summary>
    public static ExplorerTerm DeadLetter { get; } = ExplorerGlossary.Get(ExplorerTermIds.DeadLetter);
}
