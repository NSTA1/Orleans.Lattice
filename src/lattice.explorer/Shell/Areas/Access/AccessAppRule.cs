using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Explorer.Shell.Areas.Access;

/// <summary>
/// The owning app of an app-owned rule, read from its id. The app role compiler
/// names every rule it emits <c>app:{slug}:{role}:{hash}</c>, so the owner is the
/// segment after <see cref="LatticeAppRuleIds.Prefix"/>.
/// </summary>
/// <param name="Slug">The owning app's slug.</param>
/// <param name="Role">The manifest role the rule was compiled from, or <see langword="null"/> when the id does not carry one.</param>
internal readonly record struct AccessAppRule(string Slug, string? Role)
{
    /// <summary>
    /// Reads the owner of <paramref name="ruleId"/>. Returns <see langword="false"/>
    /// for an authored rule. An id under the prefix whose slug cannot be read still
    /// counts as app-owned, so it is never offered for editing; its slug is then empty.
    /// </summary>
    /// <param name="ruleId">The rule id.</param>
    /// <param name="owner">The owner, when app-owned.</param>
    public static bool TryParse(string? ruleId, out AccessAppRule owner)
    {
        owner = default;
        if (ruleId is null || !LatticeAppRuleIds.IsAppOwned(ruleId))
        {
            return false;
        }

        var rest = ruleId.AsSpan(LatticeAppRuleIds.Prefix.Length);
        var slugEnd = rest.IndexOf(':');
        var slug = slugEnd < 0 ? rest : rest[..slugEnd];
        string? role = null;
        if (slugEnd >= 0)
        {
            var afterSlug = rest[(slugEnd + 1)..];
            var roleEnd = afterSlug.IndexOf(':');
            var roleSpan = roleEnd < 0 ? afterSlug : afterSlug[..roleEnd];
            role = roleSpan.IsEmpty ? null : roleSpan.ToString();
        }

        owner = new AccessAppRule(slug.ToString(), role);
        return true;
    }

    /// <summary>Whether the slug could be read, so the owner can be linked.</summary>
    public bool HasSlug => !string.IsNullOrEmpty(Slug);
}
