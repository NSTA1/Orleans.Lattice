using System.Text;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;

/// <summary>
/// The tenant rule editor's working copy: the fields as the form holds them,
/// their validation, the local id it suggests, and the <see cref="TenantRuleDraft"/>
/// it sends. Scopes are Tree, Prefix, Key and every tree in the tenant;
/// operations are the data-plane mask only.
/// </summary>
internal sealed class TenantRuleForm
{
    /// <summary>The longest local id the editor suggests.</summary>
    public const int MaximumSuggestedIdLength = 63;

    /// <summary>The rule's tenant-local id.</summary>
    public string RuleId { get; set; } = string.Empty;

    /// <summary>The subject id: a user id, a cluster group id, or a tenant group's local name.</summary>
    public string SubjectId { get; set; } = string.Empty;

    /// <summary>Which source the subject is chosen from.</summary>
    public TenantSubjectKind SubjectKind { get; set; } = TenantSubjectKind.TenantGroup;

    /// <summary>The scope value (<see cref="TenantRuleFormat.TreeScope"/> and its siblings).</summary>
    public string ScopeValue { get; set; } = TenantRuleFormat.TreeScope;

    /// <summary>The tenant-local tree name, for a scope that names a tree.</summary>
    public string TreeName { get; set; } = string.Empty;

    /// <summary>The key or prefix, for a key or prefix scope.</summary>
    public string KeyOrPrefix { get; set; } = string.Empty;

    /// <summary>The operations the rule governs.</summary>
    public LatticeOperation Operations { get; set; } = LatticeOperation.Read;

    /// <summary>Whether the rule allows or denies.</summary>
    public LatticeEffect Effect { get; set; } = LatticeEffect.Allow;

    /// <summary>Whether the rule already exists, so its id and scope are fixed.</summary>
    public bool IsExisting { get; private init; }

    /// <summary>The scope kind the form's scope value names.</summary>
    public TenantRuleScopeKind ScopeKind => TenantRuleFormat.ScopeKind(ScopeValue);

    /// <summary>Whether the scope names a tree.</summary>
    public bool NeedsTree => ScopeKind != TenantRuleScopeKind.TenantWide;

    /// <summary>Whether the scope names a key or a prefix.</summary>
    public bool NeedsKeyOrPrefix => ScopeKind is TenantRuleScopeKind.Key or TenantRuleScopeKind.Prefix;

    /// <summary>A form for a new rule.</summary>
    /// <returns>The form.</returns>
    public static TenantRuleForm New() => new();

    /// <summary>A form editing an existing tenant rule.</summary>
    /// <param name="rule">The rule.</param>
    /// <returns>The form.</returns>
    public static TenantRuleForm From(TenantRuleView rule)
    {
        ArgumentNullException.ThrowIfNull(rule);
        return new TenantRuleForm
        {
            IsExisting = true,
            RuleId = rule.RuleId,
            SubjectId = rule.SubjectId ?? string.Empty,
            SubjectKind = rule.SubjectKind,
            ScopeValue = TenantRuleFormat.ScopeValue(rule.ScopeKind),
            TreeName = rule.TreeName ?? string.Empty,
            KeyOrPrefix = rule.KeyOrPrefix ?? string.Empty,
            Operations = rule.Operations,
            Effect = rule.Effect,
        };
    }

    /// <summary>Whether the form governs <paramref name="operation"/>.</summary>
    /// <param name="operation">A single operation.</param>
    public bool HasOperation(LatticeOperation operation) => (Operations & operation) == operation;

    /// <summary>Adds or removes <paramref name="operation"/>.</summary>
    /// <param name="operation">A single operation.</param>
    /// <param name="value">Whether to govern it.</param>
    public void SetOperation(LatticeOperation operation, bool value) =>
        Operations = value ? Operations | operation : Operations & ~operation;

    /// <summary>Checks every field, one sentence per field that has a problem.</summary>
    /// <returns>The problems; none when the rule can be sent.</returns>
    public AccessRuleDraftErrors Validate()
    {
        var errors = new AccessRuleDraftErrors();
        if (string.IsNullOrWhiteSpace(RuleId))
        {
            errors.RuleId = "Give the rule an id.";
        }

        if (string.IsNullOrWhiteSpace(SubjectId))
        {
            errors.Subject = "Choose who the rule is for.";
        }

        if (NeedsTree)
        {
            errors.Tree = string.IsNullOrWhiteSpace(TreeName)
                ? "Choose one of the tenant's trees."
                : TenantRuleFormat.TreeProblem(TreeName);
        }

        if (NeedsKeyOrPrefix && string.IsNullOrEmpty(KeyOrPrefix))
        {
            errors.KeyOrPrefix = ScopeKind == TenantRuleScopeKind.Key ? "Name the key." : "Name the key prefix.";
        }

        if (Operations == LatticeOperation.None)
        {
            errors.Operations = "Tick at least one operation.";
        }
        else if ((Operations & ~LatticeAuthOperations.All) != LatticeOperation.None)
        {
            errors.Operations = "A tenant rule governs data-plane operations only.";
        }

        return errors;
    }

    /// <summary>The draft the tenant policy is sent, with every text field trimmed.</summary>
    /// <returns>The draft.</returns>
    public TenantRuleDraft ToDraft() => new()
    {
        RuleId = RuleId.Trim(),
        SubjectId = SubjectId.Trim(),
        SubjectKind = SubjectKind,
        ScopeKind = ScopeKind,
        TreeName = NeedsTree ? TreeName.Trim() : null,
        KeyOrPrefix = NeedsKeyOrPrefix ? KeyOrPrefix : null,
        Operations = Operations,
        Effect = Effect,
    };

    /// <summary>
    /// A local id suggested from the rule's effect, subject and scope, such as
    /// <c>allow-eng-orders</c>, or <see langword="null"/> while there is nothing to
    /// suggest it from or the rule already exists.
    /// </summary>
    /// <returns>The suggested id: lower-case letters, digits and hyphens, at most <see cref="MaximumSuggestedIdLength"/> characters.</returns>
    public string? SuggestId()
    {
        if (IsExisting || string.IsNullOrWhiteSpace(SubjectId))
        {
            return null;
        }

        var builder = new StringBuilder(MaximumSuggestedIdLength);
        Append(builder, Effect == LatticeEffect.Deny ? "deny" : "allow");
        Append(builder, SubjectId);
        Append(builder, NeedsTree ? TreeName : "all-trees");
        if (NeedsKeyOrPrefix)
        {
            Append(builder, KeyOrPrefix);
        }

        var length = Math.Min(builder.Length, MaximumSuggestedIdLength);
        while (length > 0 && builder[length - 1] == '-')
        {
            length--;
        }

        return builder.ToString(0, length);
    }

    private static void Append(StringBuilder builder, string? part)
    {
        if (string.IsNullOrWhiteSpace(part))
        {
            return;
        }

        // Each part, and each run of other characters within one, becomes a single hyphen.
        var pendingSeparator = builder.Length > 0;
        foreach (var character in part)
        {
            if (builder.Length >= MaximumSuggestedIdLength)
            {
                return;
            }

            if (!char.IsAsciiLetterOrDigit(character))
            {
                pendingSeparator = builder.Length > 0;
                continue;
            }

            if (pendingSeparator)
            {
                builder.Append('-');
                pendingSeparator = false;
            }

            builder.Append(char.ToLowerInvariant(character));
        }
    }
}
