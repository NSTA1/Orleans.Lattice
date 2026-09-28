namespace Orleans.Lattice.Explorer.Shell.Areas.Access;

/// <summary>The rule editor's problems, one sentence per field; a <see langword="null"/> field has none.</summary>
internal sealed class AccessRuleDraftErrors
{
    /// <summary>The rule id's problem.</summary>
    public string? RuleId { get; set; }

    /// <summary>The subject's problem.</summary>
    public string? Subject { get; set; }

    /// <summary>The governed tree's problem.</summary>
    public string? Tree { get; set; }

    /// <summary>The key or prefix's problem.</summary>
    public string? KeyOrPrefix { get; set; }

    /// <summary>The operations' problem.</summary>
    public string? Operations { get; set; }

    /// <summary>A problem with the whole rule, such as the server's refusal.</summary>
    public string? Form { get; set; }

    /// <summary>Whether any field has a problem.</summary>
    public bool HasAny => RuleId is not null || Subject is not null || Tree is not null
        || KeyOrPrefix is not null || Operations is not null || Form is not null;
}
