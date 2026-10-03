using System.Globalization;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Groups;

/// <summary>
/// The words the tenant Groups and Members pages use: a member's kind and source,
/// a cap's usage, the local group-name grammar, and the typed reasons the tenant
/// directory refuses a change with.
/// </summary>
internal static class TenantGroupFormat
{
    /// <summary>The sentence a name outside the local group-name grammar is refused with.</summary>
    public const string NameGrammarMessage =
        "A group name is 1 to 63 characters: lower-case letters, digits, '-', '_' and '.'.";

    /// <summary>The sentence a group nesting refusal (D3) is shown with.</summary>
    public const string NestingMessage =
        "A tenant group can never be placed inside a cluster group or another tenant's group, so this group cannot be added here.";

    /// <summary>The sentence a foreign tenant group refusal is shown with.</summary>
    public const string ForeignGroupMessage =
        "Another tenant's group can never be added. Choose one of this tenant's groups by its name.";

    /// <summary>The sentence the refusal to remove the tenant's last admin entry is shown with.</summary>
    public const string LastAdminMessage =
        "This group is the tenant's last administrator entry, so it cannot be deleted. Add another administrator first.";

    /// <summary>The label of a member's kind and source.</summary>
    /// <param name="kind">The kind.</param>
    /// <returns>The label.</returns>
    public static string KindLabel(TenantSubjectKind kind) => kind switch
    {
        TenantSubjectKind.TenantGroup => "This tenant's group",
        TenantSubjectKind.ClusterGroup => "Cluster group",
        _ => "User",
    };

    /// <summary>The value a member's kind is marked with on the page, for its <c>data-lt-kind</c> attribute.</summary>
    /// <param name="kind">The kind.</param>
    /// <returns>The value.</returns>
    public static string KindValue(TenantSubjectKind kind) => kind switch
    {
        TenantSubjectKind.TenantGroup => "tenant-group",
        TenantSubjectKind.ClusterGroup => "cluster-group",
        _ => "user",
    };

    /// <summary>
    /// Checks <paramref name="name"/> against the local group-name grammar (D1) under
    /// <paramref name="tenant"/>.
    /// </summary>
    /// <param name="tenant">The tenant the group would belong to.</param>
    /// <param name="name">The typed name.</param>
    /// <returns>The reason it is refused, or <see langword="null"/> when it is a valid name.</returns>
    public static string? NameError(string tenant, string? name)
    {
        ArgumentNullException.ThrowIfNull(tenant);
        if (string.IsNullOrEmpty(name))
        {
            return "Enter the group's name.";
        }

        return LatticeTenantGroupId.TryParse(string.Concat(ClusterSubjectSuggestionSource.TenantGroupPrefix, tenant, "/", name), out var id)
            && string.Equals(id.Name, name, StringComparison.Ordinal)
                ? null
                : NameGrammarMessage;
    }

    /// <summary>Whether a capped dimension is at (or over) its limit.</summary>
    /// <param name="usage">The dimension's usage.</param>
    /// <returns><see langword="true"/> when a further entry would be refused.</returns>
    public static bool AtCap(TenantQuotaDimensionUsage usage) =>
        usage.Limit is { } limit && usage.Usage is { } used && used >= limit;

    /// <summary>A capped dimension's usage, such as <c>12 of 500 groups</c>.</summary>
    /// <param name="usage">The dimension's usage.</param>
    /// <param name="singular">The noun for one entry.</param>
    /// <param name="plural">The noun for several entries.</param>
    /// <returns>The text, or <see langword="null"/> when the usage is unmeasured or unbounded.</returns>
    public static string? CapText(TenantQuotaDimensionUsage usage, string singular, string plural)
    {
        if (usage.Usage is not { } used || usage.Limit is not { } limit)
        {
            return null;
        }

        return string.Create(CultureInfo.InvariantCulture, $"{used} of {limit} {(limit == 1 ? singular : plural)}");
    }

    /// <summary>The reason shown when a capped dimension is full.</summary>
    /// <param name="usage">The dimension's usage.</param>
    /// <param name="plural">The noun for several entries.</param>
    /// <param name="tenant">The tenant.</param>
    /// <returns>The sentence.</returns>
    public static string CapReason(TenantQuotaDimensionUsage usage, string plural, string tenant) =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"Tenant {tenant} is at its cap of {usage.Limit} {plural}. Remove one, or ask a platform operator to raise the cap.");

    /// <summary>
    /// The sentence a refused membership change is shown with beside the field: the
    /// typed reason of a confinement refusal, otherwise the classified failure's.
    /// </summary>
    /// <param name="exception">The fault.</param>
    /// <param name="failure">The classified failure.</param>
    /// <returns>The sentence.</returns>
    public static string RefusalMessage(Exception exception, AccessFailure failure)
    {
        ArgumentNullException.ThrowIfNull(exception);
        ArgumentNullException.ThrowIfNull(failure);
        return exception is TenantAccessConfinementException confinement
            ? ConfinementMessage(confinement)
            : failure.Message;
    }

    /// <summary>The typed reason a confinement refusal is shown with.</summary>
    /// <param name="exception">The refusal.</param>
    /// <returns>The sentence.</returns>
    public static string ConfinementMessage(TenantAccessConfinementException exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return exception.Rule switch
        {
            TenantAccessConfinementRule.GroupNesting => NestingMessage,
            TenantAccessConfinementRule.ForeignTenantGroup => ForeignGroupMessage,
            _ => exception.Message,
        };
    }

    /// <summary>A count with its noun, such as <c>1 rule</c> or <c>3 rules</c>.</summary>
    /// <param name="count">The count.</param>
    /// <param name="singular">The noun for one.</param>
    /// <param name="plural">The noun for several.</param>
    /// <returns>The text.</returns>
    public static string Count(long count, string singular, string plural) =>
        string.Create(CultureInfo.InvariantCulture, $"{count} {(count == 1 ? singular : plural)}");

    /// <summary>What a completed removal cascaded to, from the tenant directory's report.</summary>
    /// <param name="result">The removal report.</param>
    /// <returns>The sentence.</returns>
    public static string RemovalText(TenantGroupRemovalResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        if (!result.Removed)
        {
            return $"Group {result.GroupName} was already gone.";
        }

        var parts = new List<string>(4)
        {
            Count(result.EdgesRemoved, "membership entry", "membership entries"),
            Count(result.RemovedRuleIds.Count, "rule", "rules"),
        };
        if (result.RemovedFromMemberSet)
        {
            parts.Add("its member-set entry");
        }

        if (result.RemovedFromAdminSet)
        {
            parts.Add("its administrator entry");
        }

        return $"Group {result.GroupName} deleted, with {string.Join(", ", parts)}.";
    }
}
