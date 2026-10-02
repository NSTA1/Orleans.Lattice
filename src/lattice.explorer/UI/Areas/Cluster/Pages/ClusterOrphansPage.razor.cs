using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Operations;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;

/// <summary>
/// <c>/cluster/orphans</c>: a tree's orphaned-leaf audit, repair and survey. An audit
/// or repair runs on the cluster as a tracked whole-tree operation (#4124) whose
/// progress the page follows, picks up again when it is reopened, and can stop; its
/// verdict says plainly whether the pass rules the defect out, found leaves, or could
/// not judge part of the tree, and the per-leaf findings are read on request batch by
/// batch. A survey (a read-only key census) is driven batch by batch from the page.
/// Repair needs the TreeLifecycle grant and a typed confirmation, and is always
/// followed by a fresh audit.
/// </summary>
public partial class ClusterOrphansPage : IDisposable
{
    private LtComboBox? _treeBox;
    /// <summary>The most batches one pass runs before it stops and says so.</summary>
    internal const int MaximumBatches = 1000;

    private readonly ComponentLifetime _lifetime = new();
    private ClusterLoad<LatticeTreeAdminCapabilities> _accessLoad = ClusterLoad<LatticeTreeAdminCapabilities>.Loading;
    private LatticeTreeAdminCapabilities _access = default!;
    private readonly ComponentLifetime _runs = new();
    private CancellationToken? _run;
    private ClusterOrphanPass? _pass;
    private string? _tree;
    private string? _treeError;
    private string? _error;
    private bool _survey;
    private bool _confirm;
    private bool _starting;
    private bool _cancelling;
    private ClusterOrphanOperationResult? _totals;
    private TreeAdminOperationWatch _watch = default!;

    /// <summary>The tree to audit, from the address's <c>tree</c> query.</summary>
    [Parameter]
    public string? TreeId { get; set; }

    /// <summary>The tenant a tenant-rooted address names, or <see langword="null"/> on a cluster-wide address; links keep it.</summary>
    [CascadingParameter(Name = ClusterScope.CascadeName)]
    internal string? Scope { get; set; }

    [Inject]
    private ClusterFacades Facades { get; set; } = default!;

    [Inject]
    internal ExplorerSuggestions Suggestions { get; set; } = default!;

    [Inject]
    private ExplorerNavigator Navigator { get; set; } = default!;

    [Inject]
    private LtToastService Toasts { get; set; } = default!;

    [Inject]
    internal TimeProvider Time { get; set; } = default!;

    private bool Running => _run is not null || _starting || _watch.IsRunning;

    private bool CanRepair => _access.CanManageTreeLifecycle && !Running
        && (_totals is { Repair: false, Repairable: > 0 } || _pass is { Complete: true, Repairable: > 0 });

    /// <inheritdoc />
    public void Dispose()
    {
        Stop();
        _runs.Leave();
        _lifetime.Leave();
        _watch?.Dispose();
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        _watch = new TreeAdminOperationWatch(Time);
        _watch.Changed += OnWatchChanged;
        _watch.Finished += OnWatchFinished;
        _tree = TreeId;
        _access = ClusterTreeAccess.None(TreeId ?? string.Empty);
        if (TreeId is { } tree)
        {
            _accessLoad = await ClusterLoad<LatticeTreeAdminCapabilities>.RunAsync(
                ct => ClusterTreeAccess.ProbeAsync(Facades.RequireTreeAdmin(), tree, ct),
                _lifetime.Token);
            _access = _accessLoad.Value ?? _access;
            if (_access.CanViewDiagnostics && TreeAdminOperationsAccess.Of(Facades.TreeAdmin) is { } operations)
            {
                try
                {
                    await _watch.ResumeAsync(
                        operations,
                        [(TreeAdminOperationKinds.OrphanedLeavesAudit, tree), (TreeAdminOperationKinds.OrphanedLeavesRepair, tree)],
                        _lifetime.Token);
                }
                catch (OperationCanceledException) when (_lifetime.IsLeft)
                {
                }
            }
        }
    }

    private async Task Choose()
    {
        var tree = _tree?.Trim();
        if (string.IsNullOrEmpty(tree))
        {
            _treeError = "Name the tree to audit.";
            return;
        }

        _treeError = null;
        if (_treeBox is not null && !await _treeBox.ConfirmAsync().ConfigureAwait(true) || _lifetime.IsLeft)
        {
            return;
        }

        Navigator.NavigateTo(ClusterAddresses.Orphans(tree).WithTenant(Scope));
    }

    private void Stop()
    {
        _run = null;
        _runs.Renew();
    }

    private Task StopAsync()
    {
        if (_run is not null)
        {
            Stop();
            return Task.CompletedTask;
        }

        return CancelOperationAsync();
    }

    private Task AuditAsync()
    {
        if (_survey)
        {
            var admin = Facades.RequireTreeAdmin();
            var tree = TreeId!;
            _totals = null;
            return DriveAsync("Survey", (resume, ct) => admin.SurveyOrphanedLeavesAsync(tree, resume, ct));
        }

        return StartOperationAsync(repair: false);
    }

    private Task FindingsAsync()
    {
        var admin = Facades.RequireTreeAdmin();
        var tree = TreeId!;
        _totals = null;
        return DriveAsync("Audit", (resume, ct) => admin.AuditOrphanedLeavesAsync(tree, resume, ct));
    }

    private Task RepairAsync() => StartOperationAsync(repair: true);

    private async Task StartOperationAsync(bool repair)
    {
        _error = null;
        if (TreeId is not { } tree || Running)
        {
            return;
        }

        if (TreeAdminOperationsAccess.Of(Facades.TreeAdmin) is not { } operations)
        {
            _error = "This Explorer cannot run an orphaned-leaf pass as a tracked operation.";
            return;
        }

        _starting = true;
        _pass = null;
        _totals = null;
        try
        {
            await _watch.StartAsync(
                operations,
                repair ? TreeAdminOperationKinds.OrphanedLeavesRepair : TreeAdminOperationKinds.OrphanedLeavesAudit,
                tree,
                (operationId, ct) => repair
                    ? operations.StartOrphanedLeavesRepairAsync(tree, operationId, ct)
                    : operations.StartOrphanedLeavesAuditAsync(tree, operationId, ct),
                _lifetime.Token);
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
        {
        }
        catch (Exception exception)
        {
            _error = repair
                ? ClusterFaults.Describe(exception) + " A repair's reply is not proof of what happened: audit again to learn the tree's true state."
                : ClusterFaults.Describe(exception);
        }
        finally
        {
            _starting = false;
        }
    }

    private async Task CancelOperationAsync()
    {
        _cancelling = true;
        try
        {
            await _watch.CancelAsync(_lifetime.Token);
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
        {
        }
        catch (Exception exception)
        {
            _error = ClusterFaults.Describe(exception);
        }
        finally
        {
            _cancelling = false;
        }
    }

    private void OnWatchChanged() => _ = InvokeAsync(StateHasChanged);

    private void OnWatchFinished(LatticeOperationStatus status) => _ = InvokeAsync(async () =>
    {
        if (_lifetime.IsLeft)
        {
            return;
        }

        var repair = string.Equals(status.Kind, TreeAdminOperationKinds.OrphanedLeavesRepair, StringComparison.Ordinal);
        switch (status.State)
        {
            case LatticeOperationState.Succeeded:
                _totals = ClusterOrphanOperationResult.From(status);
                _watch.Clear();
                if (repair)
                {
                    Toasts.Show($"Repair finished: {ClusterFormat.Plural(_totals.Repaired, "leaf", "leaves")} unspliced. Auditing again.", LtToastTone.Success);
                    StateHasChanged();
                    await StartOperationAsync(repair: false);
                }

                break;
            case LatticeOperationState.Cancelled:
                _error = "Stopped. The pass described only the part of the tree it reached.";
                break;
            default:
                _error = repair
                    ? "The repair failed. A repair's outcome is not proof of what happened: audit again to learn the tree's true state."
                    : "The audit failed.";
                break;
        }

        StateHasChanged();
    });
    private async Task<ClusterOrphanPass?> DriveAsync(string kind, Func<string?, CancellationToken, Task<TreeOrphanedLeafReport>> batch)
    {
        _error = null;
        var token = _runs.Renew();
        _run = token;
        var pass = new ClusterOrphanPass(kind);
        _pass = pass;

        try
        {
            do
            {
                pass.Add(await batch(pass.ResumeFrom, token));
                StateHasChanged();
            }
            while (!pass.Complete && pass.Batches < MaximumBatches);

            if (!pass.Complete)
            {
                _error = $"The pass stopped after {MaximumBatches} batches. Run it again to continue from the start.";
            }

            return pass;
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            if (_lifetime.IsLeft)
            {
                throw;
            }

            _error = "Stopped. The findings so far describe only the part of the tree the pass reached.";
            return null;
        }
        catch (Exception exception)
        {
            _error = kind == "Repair"
                ? ClusterFaults.Describe(exception) + " A repair's reply is not proof of what happened: audit again to learn the tree's true state."
                : ClusterFaults.Describe(exception);
            return null;
        }
        finally
        {
            if (_run == token)
            {
                _run = null;
            }
        }
    }

    private static LtStateRole TotalsState(ClusterOrphanOperationResult totals) => totals switch
    {
        { IsClean: true } => LtStateRole.Healthy,
        { Orphaned: > 0 } => LtStateRole.Drift,
        _ => LtStateRole.Unknown,
    };

    private static string TotalsLabel(ClusterOrphanOperationResult totals) => totals switch
    {
        { IsClean: true } => "Clean",
        { Orphaned: > 0 } => "Orphans found",
        _ => "Not judged",
    };

    private static string TotalsSentence(ClusterOrphanOperationResult totals) => totals switch
    {
        { IsClean: true } => "No orphaned leaves, and every region was judged: this rules them out as the cause of an unbounded WAL.",
        { Repair: true, Orphaned: > 0 } => $"{ClusterFormat.Plural(totals.Orphaned, "orphaned leaf", "orphaned leaves")}: {ClusterFormat.Count(totals.Repaired)} unspliced, {ClusterFormat.Count(totals.Refused)} refused.",
        { Orphaned: > 0 } => $"{ClusterFormat.Plural(totals.Orphaned, "orphaned leaf", "orphaned leaves")}, {ClusterFormat.Count(totals.Repairable)} repairable.",
        _ => $"No orphan in what was judged, but {ClusterFormat.Plural(totals.Gaps, "region")} could not be judged: that is not a clean bill of health.",
    };
    private static LtStateRole VerdictState(ClusterOrphanPass pass) => pass switch
    {
        { IsClean: true } => LtStateRole.Healthy,
        { Findings.Count: > 0 } => LtStateRole.Drift,
        _ => LtStateRole.Unknown,
    };

    private static string VerdictLabel(ClusterOrphanPass pass) => pass switch
    {
        { IsClean: true } => "Clean",
        { Findings.Count: > 0 } => "Orphans found",
        { Complete: false } => "Partial",
        _ => "Not judged",
    };

    private static string VerdictSentence(ClusterOrphanPass pass) => pass switch
    {
        { IsClean: true } => "No orphaned leaves, and every region was judged: this rules them out as the cause of an unbounded WAL.",
        { Findings.Count: > 0 } => $"{ClusterFormat.Plural(pass.Findings.Count, "orphaned leaf", "orphaned leaves")}, {pass.Repairable} repairable.",
        { Complete: false } => "The pass has not finished, so an empty list describes only the part of the tree it reached.",
        _ => $"No orphan in what was judged, but {ClusterFormat.Plural(pass.Gaps.Count, "region")} could not be judged: that is not a clean bill of health.",
    };

    private static LtStateRole DispositionState(TreeOrphanedLeafDisposition disposition) => disposition switch
    {
        TreeOrphanedLeafDisposition.Repaired => LtStateRole.Healthy,
        TreeOrphanedLeafDisposition.Repairable => LtStateRole.Drift,
        _ => LtStateRole.Failed,
    };

    private static string DispositionText(TreeOrphanedLeafDisposition disposition) => disposition switch
    {
        TreeOrphanedLeafDisposition.Repaired => "Repaired",
        TreeOrphanedLeafDisposition.Repairable => "Repairable",
        TreeOrphanedLeafDisposition.RefusedUnverifiedKeys => "Refused: a key is not readable elsewhere",
        TreeOrphanedLeafDisposition.RefusedKeyCountExceeded => "Refused: too many keys to verify",
        TreeOrphanedLeafDisposition.RefusedBlockingState => "Refused: the shard is busy",
        TreeOrphanedLeafDisposition.RefusedChainRace => "Refused: the chain changed",
        TreeOrphanedLeafDisposition.RefusedRoutingContradiction => "Refused: routing contradicts it",
        _ => disposition.ToString(),
    };

    private static string GapText(TreeOrphanedLeafGapReason reason) => reason switch
    {
        TreeOrphanedLeafGapReason.ShardSplitInProgress => "a shard split was in progress; run again once it settles",
        TreeOrphanedLeafGapReason.ShardPassAlreadyRunning => "another pass held the shard; run again once it finishes",
        TreeOrphanedLeafGapReason.ChainTruncated => "the sibling chain is severed; the rest was examined, but the break is itself a defect",
        TreeOrphanedLeafGapReason.ChainTruncatedUnrecoverable => "the sibling chain is severed and the rest could not be reached",
        TreeOrphanedLeafGapReason.WalkBudgetExhaustedWithoutResumePosition => "the pass ran out of budget with nowhere to resume",
        TreeOrphanedLeafGapReason.LeafBoundsUndecidable => "a leaf's bounds make reachability undecidable",
        TreeOrphanedLeafGapReason.EntryLeafUnreachable => "the walk could not reach the chain's entry leaf",
        _ => reason.ToString(),
    };

    private static string Range(TreeOrphanedLeafFinding finding) =>
        $"[{finding.LowKeyInclusive ?? "start"}, {finding.HighKeyExclusive ?? "end"})";
}
