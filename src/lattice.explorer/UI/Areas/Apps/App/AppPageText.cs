using System.Globalization;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Framing;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.App;

/// <summary>
/// Plain-language text for the facts an app's page shows: operations, bridge operations,
/// scopes, shapes and retention. Every value it formats is data, rendered as text.
/// </summary>
internal static class AppPageText
{
    private static readonly (LatticeOperation Operation, string Text)[] OperationNames =
    [
        (LatticeOperation.Read, "read"),
        (LatticeOperation.Write, "write"),
        (LatticeOperation.Delete, "delete"),
        (LatticeOperation.RangeRead, "range read"),
        (LatticeOperation.RangeDelete, "range delete"),
        (LatticeOperation.CrdtApply, "CRDT apply"),
        (LatticeOperation.AtomicWrite, "atomic write"),
        (LatticeOperation.BulkLoad, "bulk load"),
        (LatticeOperation.Admin, "admin"),
        (LatticeOperation.Backup, "backup"),
        (LatticeOperation.Restore, "restore"),
        (LatticeOperation.SchemaAdmin, "schema admin"),
        (LatticeOperation.Telemetry, "telemetry"),
        (LatticeOperation.Replication, "replication"),
        (LatticeOperation.TreeLifecycle, "tree lifecycle"),
        (LatticeOperation.AppInstall, "app install"),
    ];

    /// <summary>The operations in <paramref name="operations"/>, comma-separated, or "none".</summary>
    /// <param name="operations">A set of operations.</param>
    /// <returns>Text such as "read, write, range read".</returns>
    public static string Operations(LatticeOperation operations)
    {
        var names = OperationNames.Where(pair => operations.HasFlag(pair.Operation)).Select(pair => pair.Text).ToList();
        var known = OperationNames.Aggregate(LatticeOperation.None, (all, pair) => all | pair.Operation);
        var unknown = operations & ~known;
        if (unknown != LatticeOperation.None)
        {
            names.Add("operation " + ((long)unknown).ToString(CultureInfo.InvariantCulture));
        }

        return names.Count == 0 ? "none" : string.Join(", ", names);
    }

    /// <summary>What a bridge operation lets the app's UI do, in plain language.</summary>
    /// <param name="operation">The bridge operation, such as <c>data.read</c>.</param>
    /// <returns>A phrase such as "read its own trees".</returns>
    public static string BridgeOperation(string operation) => operation switch
    {
        AppFrameProtocol.ContextRead => "read its launch context",
        AppFrameProtocol.ContextUser => "see your display name",
        AppFrameProtocol.DataRead => "read its own trees",
        AppFrameProtocol.DataWrite => "write to its own trees",
        AppFrameProtocol.DataDelete => "delete from its own trees",
        AppFrameProtocol.NavSync => "keep the address line in step with its page",
        AppFrameProtocol.UiNotify => "show notifications",
        _ => "an operation this Explorer does not recognise",
    };

    /// <summary>A bridge grant's reach: its tree, or every tree the app declares.</summary>
    /// <param name="grant">The grant.</param>
    /// <returns>Text such as "tree orders" or "every declared tree".</returns>
    public static string BridgeReach(AppUiBridgeGrantDescriptor grant)
    {
        ArgumentNullException.ThrowIfNull(grant);
        return !AppFrameProtocol.IsDataOperation(grant.Operation)
            ? "not tree-scoped"
            : grant.Tree is null ? "every declared tree" : "tree " + grant.Tree;
    }

    /// <summary>A role's scope template: its tree, its owning app when not this one, and its key extent.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="slug">The described app's slug.</param>
    /// <returns>Text such as "a/crm/orders, prefix eu/".</returns>
    public static string Scope(AppRoleScope scope, string slug)
    {
        ArgumentNullException.ThrowIfNull(scope);
        var tree = "a/" + (scope.App ?? slug) + "/" + scope.Tree;
        return scope.Kind switch
        {
            LatticeScopeKind.Key => tree + ", key " + scope.KeyOrPrefix,
            LatticeScopeKind.Prefix => tree + ", prefix " + scope.KeyOrPrefix,
            _ => tree + ", whole tree",
        };
    }

    /// <summary>An approved exception scope's target: an app's tree, or a legacy tree an operator declared.</summary>
    /// <param name="scope">The exception scope.</param>
    /// <returns>Text such as "a/billing/invoices" or "legacy tree orders-2019".</returns>
    public static string ExceptionTarget(AppExceptionScope scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        return scope.AdoptedTreeId is { } legacy
            ? "legacy tree " + legacy
            : "a/" + scope.App + "/" + scope.Tree;
    }

    /// <summary>An exception scope's key extent.</summary>
    /// <param name="scope">The exception scope.</param>
    /// <returns>Text such as "whole tree" or "prefix eu/".</returns>
    public static string ExceptionExtent(AppExceptionScope scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        return scope.Kind switch
        {
            LatticeScopeKind.Key => "key " + scope.KeyOrPrefix,
            LatticeScopeKind.Prefix => "prefix " + scope.KeyOrPrefix,
            _ => "whole tree",
        };
    }

    /// <summary>A subscription's source tree as a logical address.</summary>
    /// <param name="subscription">The subscription.</param>
    /// <param name="slug">The described app's slug.</param>
    /// <returns>Text such as "a/crm/orders".</returns>
    public static string SubscriptionSource(AppSubscriptionDescriptor subscription, string slug)
    {
        ArgumentNullException.ThrowIfNull(subscription);
        return "a/" + (subscription.App ?? slug) + "/" + subscription.Tree;
    }

    /// <summary>A declared number, or "host default".</summary>
    /// <param name="value">The declared value.</param>
    /// <returns>The number in the invariant culture, or "host default".</returns>
    public static string Declared(int? value) =>
        value is { } number ? number.ToString("N0", CultureInfo.InvariantCulture) : "host default";

    /// <summary>A retention period in plain words, or "host default".</summary>
    /// <param name="value">The declared retention.</param>
    /// <returns>Text such as "30 days", "12 hours" or "host default".</returns>
    /// <remarks>
    /// A fraction of a minute is rounded up first and the unit chosen on the
    /// rounded figure, so 59.5 minutes reads "1 hour" rather than "60 minutes".
    /// </remarks>
    public static string Retention(TimeSpan? value)
    {
        if (value is not { } span)
        {
            return "host default";
        }

        var minutes = (long)Math.Ceiling(span.TotalMinutes);
        if (minutes > 0 && minutes % 1_440 == 0)
        {
            return Count(minutes / 1_440, "day");
        }

        if (minutes > 0 && minutes % 60 == 0)
        {
            return Count(minutes / 60, "hour");
        }

        return Count(minutes, "minute");
    }

    /// <summary>A lifecycle state's label.</summary>
    /// <param name="state">The state.</param>
    /// <returns>Text such as "Enabled".</returns>
    public static string State(AppLifecycleState state) => state switch
    {
        AppLifecycleState.Installed => "Installed",
        AppLifecycleState.Enabled => "Enabled",
        AppLifecycleState.Disabled => "Disabled",
        AppLifecycleState.Uninstalled => "Uninstalled",
        AppLifecycleState.Failed => "Activation failed",
        _ => "Not installed",
    };

    /// <summary>"1 day", "2 days" and so on.</summary>
    /// <param name="count">The count.</param>
    /// <param name="unit">The singular unit.</param>
    /// <returns>The count and the unit, pluralised.</returns>
    public static string Count(long count, string unit) =>
        count.ToString("N0", CultureInfo.InvariantCulture) + " " + (count == 1 ? unit : unit + "s");
}
