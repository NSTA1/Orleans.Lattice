using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Access;

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

    /// <summary>
    /// Formats a subject selector with its friendly directory display name as the
    /// primary text, for example <c>user:Alice Ng</c>, resolving the id through
    /// <paramref name="labels"/> when supplied. Falls back to the raw
    /// <see cref="SubjectLabel(LatticeSubjectSelector)"/> form when no resolver is
    /// supplied or the id is not yet resolved, so the display degrades to exactly
    /// the id-only label. The raw id form belongs on the hover tooltip.
    /// </summary>
    /// <param name="subject">The subject selector. Must not be <see langword="null"/>.</param>
    /// <param name="labels">The label resolver, or <see langword="null"/> to render the id only.</param>
    /// <returns>The display label.</returns>
    internal static string SubjectDisplayLabel(LatticeSubjectSelector subject, PrincipalLabelResolver? labels)
    {
        ArgumentNullException.ThrowIfNull(subject);
        if (labels is null)
        {
            return SubjectLabel(subject);
        }

        var kind = subject.Kind == LatticeSubjectSelectorKind.Group ? "group" : "user";
        return $"{kind}:{labels.Label(subject.Id)}";
    }

    /// <summary>
    /// Returns <paramref name="explanation"/>'s reason text with the raw subject id
    /// swapped for its friendly directory display name, matching the Access area's
    /// display-name-primary, id-on-hover convention. The swap is applied only when
    /// the subject id is non-empty and resolves through <paramref name="labels"/> to
    /// a label that differs from the id, so an unresolved id (or an absent directory)
    /// degrades to exactly the server-authored reason text, byte for byte. Returns
    /// <see langword="null"/> when <see cref="AuthExplanation.Reason"/> is
    /// <see langword="null"/>. Allocates a single replaced string per call, on the
    /// Explain data-load render path only.
    /// </summary>
    /// <param name="explanation">The authorization explanation. Must not be <see langword="null"/>.</param>
    /// <param name="labels">The label resolver. Must not be <see langword="null"/>.</param>
    /// <returns>The reason with the subject id replaced by its label, or <see langword="null"/>.</returns>
    internal static string? FriendlyReason(AuthExplanation explanation, PrincipalLabelResolver labels)
    {
        ArgumentNullException.ThrowIfNull(explanation);
        ArgumentNullException.ThrowIfNull(labels);
        var reason = explanation.Reason;
        if (reason is null)
        {
            return null;
        }

        var subjectId = explanation.SubjectId;
        if (string.IsNullOrEmpty(subjectId))
        {
            return reason;
        }

        var label = labels.Label(subjectId);
        if (string.Equals(label, subjectId, StringComparison.Ordinal))
        {
            return reason;
        }

        return reason.Replace(subjectId, label, StringComparison.Ordinal);
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
