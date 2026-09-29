namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>The tabs of a tree's schema workspace, as they appear in the <c>tab</c> query value.</summary>
internal static class SchemaTabs
{
    /// <summary>The enforcement policy: get, set and clear.</summary>
    public const string Policy = "policy";

    /// <summary>The envelope-version config: get, set, clear, advance and migrate.</summary>
    public const string Versions = "versions";

    /// <summary>The read-only compliance scan and its results.</summary>
    public const string Compliance = "compliance";

    /// <summary>Background remediation: start and status.</summary>
    public const string Remediation = "remediation";

    /// <summary>The strict-mode dead-letter queue: count and list.</summary>
    public const string DeadLetters = "dead-letters";

    /// <summary>Every tab, in the order the tab row shows them, with its title.</summary>
    public static IReadOnlyList<(string Id, string Title)> All { get; } =
    [
        (Policy, "Policy"),
        (Versions, "Versions"),
        (Compliance, "Compliance"),
        (Remediation, "Remediation"),
        (DeadLetters, "Dead letters"),
    ];

    /// <summary>The tab a query value names, falling back to <see cref="Policy"/> for none or an unknown one.</summary>
    /// <param name="value">The <c>tab</c> query value.</param>
    /// <returns>A known tab id.</returns>
    public static string Parse(string? value)
    {
        foreach (var (id, _) in All)
        {
            if (string.Equals(id, value, StringComparison.Ordinal))
            {
                return id;
            }
        }

        return Policy;
    }
}
