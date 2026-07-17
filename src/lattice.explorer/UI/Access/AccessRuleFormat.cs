using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Explorer.UI.Access;

/// <summary>
/// Presentation helpers shared by the Access area's policy views: compact,
/// human-readable labels for a rule's subject, scope, and operation set, and the
/// canonical ordered list of assignable <see cref="LatticeOperation"/> flags for
/// the authoring and Explain pickers. Pure formatting over data the facade
/// already returned; it never derives a verdict.
/// </summary>
internal static class AccessRuleFormat
{
    /// <summary>A single selectable operation flag paired with its display label.</summary>
    /// <param name="Flag">The operation flag.</param>
    /// <param name="Label">The display label.</param>
    internal readonly record struct OperationOption(LatticeOperation Flag, string Label);

    /// <summary>
    /// The assignable operation flags, in a stable display order. Excludes the
    /// <see cref="LatticeOperation.None"/> sentinel. Allocated once and reused for
    /// every render so the pickers add no per-render allocation.
    /// </summary>
    internal static readonly IReadOnlyList<OperationOption> Operations = new OperationOption[]
    {
        new(LatticeOperation.Read, "Read"),
        new(LatticeOperation.Write, "Write"),
        new(LatticeOperation.Delete, "Delete"),
        new(LatticeOperation.RangeRead, "Range read"),
        new(LatticeOperation.RangeDelete, "Range delete"),
        new(LatticeOperation.CrdtApply, "CRDT apply"),
        new(LatticeOperation.AtomicWrite, "Atomic write"),
        new(LatticeOperation.BulkLoad, "Bulk load"),
        new(LatticeOperation.Admin, "Admin"),
        new(LatticeOperation.Backup, "Backup"),
        new(LatticeOperation.Restore, "Restore"),
        new(LatticeOperation.SchemaAdmin, "Schema admin"),
        new(LatticeOperation.Telemetry, "Telemetry"),
    };

    /// <summary>Formats a subject selector as, for example, <c>user:alice</c> or <c>group:admins</c>.</summary>
    /// <param name="subject">The subject selector. Must not be <see langword="null"/>.</param>
    /// <returns>The label.</returns>
    internal static string SubjectLabel(LatticeSubjectSelector subject)
    {
        ArgumentNullException.ThrowIfNull(subject);
        var kind = subject.Kind == LatticeSubjectSelectorKind.Group ? "group" : "user";
        return $"{kind}:{subject.Id}";
    }

    /// <summary>Formats a scope as, for example, <c>tree</c>, <c>prefix 'foo'</c>, or <c>key 'bar'</c>.</summary>
    /// <param name="scope">The scope. Must not be <see langword="null"/>.</param>
    /// <returns>The label.</returns>
    internal static string ScopeLabel(LatticeScope scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        return scope.Kind switch
        {
            LatticeScopeKind.Key => $"key '{scope.KeyOrPrefix}'",
            LatticeScopeKind.Prefix => $"prefix '{scope.KeyOrPrefix}'",
            _ => "tree",
        };
    }

    /// <summary>Formats an operation set as a compact comma-separated label, or <c>none</c> when empty.</summary>
    /// <param name="operations">The operation flags.</param>
    /// <returns>The label.</returns>
    internal static string OperationsLabel(LatticeOperation operations)
    {
        if (operations == LatticeOperation.None)
        {
            return "none";
        }

        var parts = new List<string>();
        foreach (var option in Operations)
        {
            if ((operations & option.Flag) == option.Flag)
            {
                parts.Add(option.Label);
            }
        }

        return parts.Count == 0 ? operations.ToString() : string.Join(", ", parts);
    }
}
