using System.Runtime.CompilerServices;
using NSubstitute;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// An in-memory admin audit tree behind a substituted <see cref="IGrainFactory"/>: rows are
/// written with the production key layout and read back through ordered range scans.
/// </summary>
internal sealed class AdminAuditTreeFake
{
    private readonly SortedDictionary<string, byte[]> _rows = new(StringComparer.Ordinal);

    public AdminAuditTreeFake(bool treeExists = true)
    {
        var tree = Substitute.For<ILattice>();
        tree.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(call => Range(call.ArgAt<string?>(0), call.ArgAt<string?>(1)));
        var registry = Substitute.For<ILatticeRegistry>();
        registry.ExistsAsync(AuthConstants.AdminAuditTree).Returns(treeExists);

        GrainFactory = Substitute.For<IGrainFactory>();
        GrainFactory.GetGrain<ILattice>(AuthConstants.AdminAuditTree).Returns(tree);
        GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
    }

    public IGrainFactory GrainFactory { get; }

    public static LatticeAdminAuditRecord Record(
        string operationId,
        int minute,
        string scope = LatticeAdminAuditRequest.ClusterScope,
        LatticeAdminAuditAction action = LatticeAdminAuditAction.RuleUpsert,
        LatticeAdminAuditOutcome outcome = LatticeAdminAuditOutcome.Committed,
        string actor = "alice") =>
        new()
        {
            OperationId = operationId,
            ActorSubjectId = actor,
            TargetScope = scope,
            Action = action,
            Outcome = outcome,
            TimestampUtc = new DateTimeOffset(2026, 11, 27, 12, minute, 0, TimeSpan.Zero),
        };

    /// <summary>Writes a final record under its final key.</summary>
    public void Final(LatticeAdminAuditRecord record) => _rows[AdminAuditKeys.Final(record)] = AdminAuditStore.Serialize(record);

    /// <summary>Writes an in-flight intent under its pending key.</summary>
    public void Pending(LatticeAdminAuditRecord record) => _rows[AdminAuditKeys.Pending(record.OperationId)] = AdminAuditStore.Serialize(record);

    private async IAsyncEnumerable<KeyValuePair<string, byte[]>> Range(
        string? start, string? end, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        foreach (var row in _rows.ToList())
        {
            if ((start is null || string.CompareOrdinal(row.Key, start) >= 0)
                && (end is null || string.CompareOrdinal(row.Key, end) < 0))
            {
                yield return row;
            }
        }
    }
}
