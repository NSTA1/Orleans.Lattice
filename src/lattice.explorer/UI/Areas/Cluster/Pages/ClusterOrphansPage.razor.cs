using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;

/// <summary>
/// <c>/cluster/orphans</c>: a tree's orphaned-leaf survey, audit and repair. Each
/// is driven batch by batch to completion (and can be stopped), and the verdict
/// says plainly whether the pass rules the defect out, found leaves, or could not
/// judge part of the tree. Repair needs the TreeLifecycle grant and a typed
/// confirmation, and is always followed by a fresh audit.
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

    /// <summary>The tree to audit, from the address's <c>tree</c> query.</summary>
    [Parameter]
    public string? TreeId { get; set; }

    [Inject]
    private ClusterFacades Facades { get; set; } = default!;

    [Inject]
    internal ExplorerSuggestions Suggestions { get; set; } = default!;

    [Inject]
    private ExplorerNavigator Navigator { get; set; } = default!;

    [Inject]
    private LtToastService Toasts { get; set; } = default!;

    private bool Running => _run is not null;

    /// <inheritdoc />
    public void Dispose()
    {
        Stop();
        _runs.Leave();
        _lifetime.Leave();
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        _tree = TreeId;
        _access = ClusterTreeAccess.None(TreeId ?? string.Empty);
        if (TreeId is { } tree)
        {
            _accessLoad = await ClusterLoad<LatticeTreeAdminCapabilities>.RunAsync(
                ct => ClusterTreeAccess.ProbeAsync(Facades.RequireTreeAdmin(), tree, ct),
                _lifetime.Token);
            _access = _accessLoad.Value ?? _access;
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

        Navigator.NavigateTo(ClusterAddresses.Orphans(tree));
    }

    private void Stop()
    {
        _run = null;
        _runs.Renew();
    }

    private Task AuditAsync()
    {
        var admin = Facades.RequireTreeAdmin();
        var tree = TreeId!;
        return _survey
            ? DriveAsync("Survey", (resume, ct) => admin.SurveyOrphanedLeavesAsync(tree, resume, ct))
            : DriveAsync("Audit", (resume, ct) => admin.AuditOrphanedLeavesAsync(tree, resume, ct));
    }

    private async Task RepairAsync()
    {
        var admin = Facades.RequireTreeAdmin();
        var tree = TreeId!;
        var repaired = await DriveAsync("Repair", (resume, ct) => admin.RepairOrphanedLeavesAsync(tree, resume, ct));
        if (repaired is { Complete: true })
        {
            var count = repaired.Findings.Count(finding => finding.Disposition == TreeOrphanedLeafDisposition.Repaired);
            Toasts.Show($"Repair finished: {ClusterFormat.Plural(count, "leaf", "leaves")} unspliced. Auditing again.", LtToastTone.Success);
            if (!_lifetime.IsLeft)
            {
                await AuditAsync();
            }
        }
    }

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
