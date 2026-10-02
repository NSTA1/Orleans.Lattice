using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// One long-running schema operation this circuit started: what it is, on which
/// tree, and how far it has got. Immutable; <see cref="SchemaOperations"/> replaces
/// it as it moves on.
/// </summary>
/// <param name="TreeId">The logical tree id.</param>
/// <param name="Kind">What the operation does.</param>
/// <param name="Summary">One plain line describing it, such as "Migrate every value to version 4".</param>
/// <param name="StartedAt">When it was confirmed.</param>
internal sealed record SchemaOperation(string TreeId, SchemaOperationKind Kind, string Summary, DateTimeOffset StartedAt)
{
    /// <summary>How far it has got.</summary>
    public SchemaOperationStage Stage { get; init; } = SchemaOperationStage.Starting;

    /// <summary>The terminal report, once the cluster returned one.</summary>
    public LatticeSchemaRemediationReport? Report { get; init; }

    /// <summary>The cluster operation id, once the start has been accepted.</summary>
    public string? OperationId { get; init; }

    /// <summary>The latest cluster status this circuit read.</summary>
    public LatticeOperationStatus? Status { get; init; }

    /// <summary>Why it failed, as a plain sentence, when it failed.</summary>
    public string? Failure { get; init; }

    /// <summary>When it ended, once it has.</summary>
    public DateTimeOffset? FinishedAt { get; init; }

    /// <summary>Whether it is still in the cluster's hands.</summary>
    public bool IsActive => Stage is SchemaOperationStage.Starting or SchemaOperationStage.Running;
}
