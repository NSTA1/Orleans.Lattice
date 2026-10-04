using System.Globalization;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;

/// <summary>
/// A tree's WAL reclamation (#4195): which durable pin holds its write-ahead-log
/// floor, the leaf behind it, that leaf's state and pin offset, and whether the
/// floor is wedged. Shown wherever the Cluster area reports a tree's WAL - the WAL
/// page and the tree's storage tab - so a tree that reclaims nothing because it is
/// wedged reads differently from one that has nothing to reclaim.
/// </summary>
/// <remarks>
/// The verdict is keyed on the floor holder's pin offset and state, read from the
/// cluster on demand, never on growth: a wedged tree need not be growing. Hidden
/// when the head serves no WAL reclamation read, and a quiet note when the cluster
/// does not answer it.
/// </remarks>
public partial class ClusterWalReclamation : IDisposable
{
    private readonly ComponentLifetime _lifetime = new();
    private string? _loadedFor;
    private TreeWalReclamationReport? _report;
    private string? _error;
    private bool _unserved;

    /// <summary>The logical tree id.</summary>
    [Parameter, EditorRequired]
    public string TreeId { get; set; } = string.Empty;

    [Inject]
    private ClusterFacades Facades { get; set; } = default!;

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Leave();
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Facades.WalReclamation is not { } reclamation || TreeId == _loadedFor)
        {
            return;
        }

        _loadedFor = TreeId;
        _report = null;
        _error = null;
        _unserved = false;
        var token = _lifetime.Renew();

        // Every outcome is checked against this read's own token, which the next
        // tree's read cancels: an answer or fault that arrives after the tree has
        // changed belongs to the previous tree and must not be shown for this one.
        try
        {
            var report = await reclamation.GetWalReclamationAsync(TreeId, token);
            if (!token.IsCancellationRequested)
            {
                _report = report;
            }
        }
        catch (Exception) when (token.IsCancellationRequested)
        {
            // Superseded by the next tree's read, or the section was left.
        }
        catch (NotSupportedException)
        {
            _unserved = true;
        }
        catch (Exception exception)
        {
            _error = ClusterFaults.Describe(exception);
        }
    }

    /// <summary>The pill's state role for a report.</summary>
    /// <param name="report">The report.</param>
    /// <returns>The role.</returns>
    internal static LtStateRole VerdictState(TreeWalReclamationReport report) => Verdict(report) switch
    {
        ReclamationVerdict.Wedged => LtStateRole.Failed,
        ReclamationVerdict.AwaitingCheckpoint => LtStateRole.Lagging,
        ReclamationVerdict.Unknown => LtStateRole.Unknown,
        _ => LtStateRole.Healthy,
    };

    /// <summary>The pill's text for a report.</summary>
    /// <param name="report">The report.</param>
    /// <returns>The text.</returns>
    internal static string VerdictLabel(TreeWalReclamationReport report) => Verdict(report) switch
    {
        ReclamationVerdict.Wedged => "Blocked",
        ReclamationVerdict.AwaitingCheckpoint => "Waiting for a checkpoint",
        ReclamationVerdict.Unknown => "Not established",
        _ => "Not blocked",
    };

    /// <summary>The sentence that explains a report's verdict.</summary>
    /// <param name="report">The report.</param>
    /// <returns>The sentence.</returns>
    internal static string Explanation(TreeWalReclamationReport report)
    {
        if (!report.PinStoreReadable)
        {
            return "The WAL pin store did not answer, so whether reclamation is blocked could not be established.";
        }

        if (report.FloorHolder is not { } holder)
        {
            return "No leaf holds a WAL pin on this tree, so no pin holds its WAL back.";
        }

        var leaf = LeafText(holder);
        var partition = holder.Partition.ToString(CultureInfo.InvariantCulture);
        var offset = holder.PinOffset.ToString(CultureInfo.InvariantCulture);
        return Verdict(report) switch
        {
            ReclamationVerdict.Wedged =>
                $"Reclamation is blocked: leaf {leaf} holds the floor with a durable pin at offset {offset} on partition {partition}, above a checkpoint it never persisted. This does not clear on its own.",
            ReclamationVerdict.AwaitingCheckpoint =>
                $"Leaf {leaf} has not checkpointed partition {partition} yet, so its pin reports no offset (-1) and holds no offset floor. This clears once the leaf checkpoints.",
            ReclamationVerdict.Unknown =>
                $"Leaf {leaf} holds the floor at offset {offset} on partition {partition}, but its state could not be read.",
            _ when holder.HoldsOffsetFloor =>
                $"Leaf {leaf} holds the floor at offset {offset} on partition {partition}. WAL below that offset can be reclaimed.",
            _ =>
                $"No pin reports an offset yet; leaf {leaf} on partition {partition} is the first of them.",
        };
    }

    private static ReclamationVerdict Verdict(TreeWalReclamationReport report)
    {
        if (!report.PinStoreReadable)
        {
            return ReclamationVerdict.Unknown;
        }

        if (report.IsWedged)
        {
            return ReclamationVerdict.Wedged;
        }

        return report.FloorHolder switch
        {
            null => ReclamationVerdict.Clear,
            { State: TreeWalFloorHolderState.NeverCheckpointed } => ReclamationVerdict.AwaitingCheckpoint,
            { State: TreeWalFloorHolderState.Unreadable } => ReclamationVerdict.Unknown,
            _ => ReclamationVerdict.Clear,
        };
    }

    private static string LeafText(TreeWalFloorHolder holder) => holder.LeafId ?? "(unknown leaf)";

    private static string CheckpointText(TreeWalFloorHolder holder) => holder.PersistedCheckpoint switch
    {
        null => "Not read",
        < 0 => "None (-1)",
        { } checkpoint => checkpoint.ToString(CultureInfo.InvariantCulture),
    };

    private static string PinsText(TreeWalReclamationReport report) =>
        report.PinsWithoutOffset == 0
            ? ClusterFormat.Plural(report.PinCount, "pin")
            : $"{ClusterFormat.Plural(report.PinCount, "pin")}, {ClusterFormat.Count(report.PinsWithoutOffset)} with no offset yet";

    private static string StateText(TreeWalFloorHolderState state) => state switch
    {
        TreeWalFloorHolderState.CheckpointedUncovered => "Checkpointed, snapshot coverage missing",
        TreeWalFloorHolderState.NeverCheckpointed => "Never checkpointed",
        TreeWalFloorHolderState.NoDurableState => "No durable state",
        TreeWalFloorHolderState.Orphaned => "Orphaned: the leaf is gone",
        TreeWalFloorHolderState.CheckpointedCoverageUnknown => "Checkpointed",
        _ => "Could not be read",
    };

    private enum ReclamationVerdict
    {
        Clear,
        Wedged,
        AwaitingCheckpoint,
        Unknown,
    }
}
