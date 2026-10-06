using System.Globalization;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Operations;

// Still calls the deprecated blocking tree-administration verbs (LATTICE0002); the Explorer moves to
// ILatticeTreeAdminOperations in the second #4124 change, which removes this suppression.
#pragma warning disable LATTICE0002

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;

/// <summary>
/// <c>/cluster/wal</c>: a tree's WAL placement audit and which durable pin holds its
/// WAL floor (#4195), then a staged move - plan
/// (a read-only preview at its own address, <c>?tree=&amp;partition=&amp;target=</c>,
/// so it resumes), execute and reclaim, each of the last two behind a typed
/// confirmation and the TreeLifecycle grant. It owns the visible control of the
/// "Plan WAL move..." palette command. A move runs on the cluster as a tracked
/// operation (#4124): the page follows its copy, verify and flip, picks it up again
/// when the plan's address is reopened, and can ask it to stop before the flip.
/// </summary>
public partial class ClusterWalPage : IDisposable
{
    private LtComboBox? _treeBox;
    private LtComboBox? _planTreeBox;
    private LtComboBox? _planTargetBox;
    private ClusterProviderKeySuggestionSource? _providerKeys;
    private readonly ComponentLifetime _load = new();
    private (string? Tree, int? Partition, string? Target) _loaded;
    private ClusterLoad<TreeWalPlacementAudit> _audit = ClusterLoad<TreeWalPlacementAudit>.Loading;
    private ClusterLoad<TreeWalMovePlan> _plan = ClusterLoad<TreeWalMovePlan>.Loading;
    private LatticeTreeAdminCapabilities _access = default!;
    private TreeWalMoveReceipt? _receipt;
    private string? _reclaimKey;
    private string? _tree;
    private string? _treeError;
    private bool _planOpen;
    private string? _planTree;
    private string? _planPartition;
    private string? _planTarget;
    private string? _planTreeError;
    private string? _planPartitionError;
    private string? _planTargetError;
    private Verb _confirm;
    private bool _busy;
    private bool _cancelling;
    private bool _initialised;
    private TreeAdminOperationWatch _watch = default!;

    private enum Verb
    {
        None,
        Execute,
        Reclaim,
    }

    /// <summary>The tree to audit, from the address's <c>tree</c> query.</summary>
    [Parameter]
    public string? TreeId { get; set; }

    /// <summary>The partition a move plan is for, from the address's <c>partition</c> query.</summary>
    [Parameter]
    public int? Partition { get; set; }

    /// <summary>The provider key a move plan targets, from the address's <c>target</c> query.</summary>
    [Parameter]
    public string? Target { get; set; }

    /// <summary>The tenant a tenant-rooted address names, or <see langword="null"/> on a cluster-wide address; links keep it.</summary>
    [CascadingParameter(Name = ClusterScope.CascadeName)]
    internal string? Scope { get; set; }

    [Inject]
    private ClusterFacades Facades { get; set; } = default!;

    [Inject]
    internal ExplorerSuggestions Suggestions { get; set; } = default!;

    [Inject]
    private ClusterCommandSignals Signals { get; set; } = default!;

    [Inject]
    private ExplorerNavigator Navigator { get; set; } = default!;

    [Inject]
    private LtToastService Toasts { get; set; } = default!;

    [Inject]
    internal TimeProvider Time { get; set; } = default!;

    /// <summary>Whether a move or reclaim is being asked for or a move is running.</summary>
    private bool Busy => _busy || _watch.IsRunning;

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    private LtDialogPlacement SheetPlacement => Breakpoint == LtBreakpoint.Compact ? LtDialogPlacement.End : LtDialogPlacement.Center;

    private string TargetHint => _audit.Value is { KnownProviderKeys.IsDefaultOrEmpty: false } audit
        ? "Known keys: " + string.Join(", ", audit.KnownProviderKeys) + "."
        : "A storage provider key every silo resolves.";

    private string? ReclaimSource => _receipt is { SourceRetained: true } receipt
        ? receipt.FromProviderKey
        : _receipt is null && _plan.Value is { AlreadyAtTarget: true } && !string.IsNullOrEmpty(_reclaimKey)
            ? _reclaimKey
            : null;

    /// <inheritdoc />
    public void Dispose()
    {
        Signals.Requested -= OnCommand;
        _load.Leave();
        _watch?.Dispose();
    }

    /// <inheritdoc />
    protected override void OnInitialized()
    {
        _access = ClusterTreeAccess.None(TreeId ?? string.Empty);
        _providerKeys = new ClusterProviderKeySuggestionSource(Facades, () => _planTree);
        _watch = new TreeAdminOperationWatch(Time);
        _watch.Changed += OnWatchChanged;
        _watch.Finished += OnWatchFinished;
        Signals.Requested += OnCommand;
        if (Signals.TryTake(ClusterArea.PlanWalMoveCommandId))
        {
            OpenPlan();
        }
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        var wanted = (TreeId, Partition, Target);
        if (_initialised && wanted == _loaded)
        {
            return;
        }

        _initialised = true;
        _loaded = wanted;
        _tree = TreeId;
        _receipt = null;
        _reclaimKey = null;
        _watch.Clear();
        var token = _load.Renew();

        if (TreeId is not { } tree)
        {
            return;
        }

        _audit = ClusterLoad<TreeWalPlacementAudit>.Loading;
        _plan = ClusterLoad<TreeWalMovePlan>.Loading;
        var access = ClusterLoad<LatticeTreeAdminCapabilities>.RunAsync(ct => ClusterTreeAccess.ProbeAsync(Facades.RequireTreeAdmin(), tree, ct), token);
        var audit = ClusterLoad<TreeWalPlacementAudit>.RunAsync(ct => Facades.RequireTreeAdmin().AuditWalPlacementAsync(tree, ct), token);
        var plan = Partition is { } partition && Target is { } target
            ? ClusterLoad<TreeWalMovePlan>.RunAsync(ct => Facades.RequireTreeAdmin().PlanWalMoveAsync(tree, partition, target, ct), token)
            : null;

        var accessLoad = await access;
        var auditLoad = await audit;
        var planLoad = plan is null ? null : await plan;

        // The next address's load renews the token: a read that answered, or faulted,
        // after that belongs to the previous address and must not be shown for this one.
        if (token.IsCancellationRequested)
        {
            return;
        }

        _access = accessLoad.Value ?? ClusterTreeAccess.None(tree);
        _audit = auditLoad;
        if (planLoad is not null)
        {
            _plan = planLoad;
        }

        if (Partition is { } movePartition && TreeAdminOperationsAccess.Of(Facades.TreeAdmin) is { } operations)
        {
            try
            {
                await _watch.ResumeAsync(operations, [(TreeAdminOperationKinds.WalMove, TreeAdminOperationIds.Target(tree, movePartition))], token);
            }
            catch (OperationCanceledException) when (_load.IsLeft)
            {
            }
        }
    }

    private async Task Audit()
    {
        var tree = _tree?.Trim();
        if (string.IsNullOrEmpty(tree))
        {
            _treeError = "Name the tree to audit.";
            return;
        }

        _treeError = null;
        if (_treeBox is not null && !await _treeBox.ConfirmAsync().ConfigureAwait(true) || _load.IsLeft)
        {
            return;
        }

        Navigator.NavigateTo(ClusterAddresses.Wal(tree).WithTenant(Scope));
    }

    private void OnCommand(string commandId)
    {
        if (string.Equals(commandId, ClusterArea.PlanWalMoveCommandId, StringComparison.Ordinal))
        {
            _ = InvokeAsync(() =>
            {
                OpenPlan();
                StateHasChanged();
            });
        }
    }

    private void OpenPlan()
    {
        _planTree = TreeId ?? _tree;
        _planPartition = Partition?.ToString(CultureInfo.InvariantCulture);
        _planTarget = Target;
        _planTreeError = _planPartitionError = _planTargetError = null;
        _planOpen = true;
    }

    private async Task ContinuePlan()
    {
        var tree = _planTree?.Trim();
        var target = _planTarget?.Trim();
        _planTreeError = string.IsNullOrEmpty(tree) ? "Name the tree." : null;
        _planPartitionError = int.TryParse(_planPartition?.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out var partition)
            ? null
            : "Enter a partition index: a whole number from 0.";
        _planTargetError = string.IsNullOrEmpty(target) ? "Name the provider key to move to." : null;

        if (_planTreeError is null && _planPartitionError is null && _planTargetError is null && await ConfirmPlanAsync().ConfigureAwait(true) && !_load.IsLeft)
        {
            _planOpen = false;
            Navigator.NavigateTo(ClusterAddresses.Wal(tree, partition, target).WithTenant(Scope));
        }
    }

    private async Task<bool> ConfirmPlanAsync()
    {
        var tree = _planTreeBox is null || await _planTreeBox.ConfirmAsync().ConfigureAwait(true);
        var target = _planTargetBox is null || await _planTargetBox.ConfirmAsync().ConfigureAwait(true);
        return tree && target;
    }

    private void Close(bool open)
    {
        if (!open)
        {
            _confirm = Verb.None;
        }
    }

    private async Task ExecuteAsync()
    {
        _confirm = Verb.None;
        if (TreeId is not { } tree || Partition is not { } partition || Target is not { } target || Busy)
        {
            return;
        }

        if (TreeAdminOperationsAccess.Of(Facades.TreeAdmin) is not { } operations)
        {
            Toasts.Show("This Explorer cannot run a WAL move as a tracked operation.", LtToastTone.Danger);
            return;
        }

        _busy = true;
        try
        {
            await _watch.StartAsync(
                operations,
                TreeAdminOperationKinds.WalMove,
                TreeAdminOperationIds.Target(tree, partition),
                (operationId, ct) => operations.StartWalMoveAsync(tree, partition, target, null, operationId, ct),
                _load.Token);
        }
        catch (OperationCanceledException) when (_load.IsLeft)
        {
        }
        catch (Exception exception)
        {
            Toasts.Show(ClusterFaults.Describe(exception), LtToastTone.Danger);
        }
        finally
        {
            _busy = false;
        }
    }

    private async Task StopMoveAsync()
    {
        _cancelling = true;
        try
        {
            await _watch.CancelAsync(_load.Token);
        }
        catch (OperationCanceledException) when (_load.IsLeft)
        {
        }
        catch (Exception exception)
        {
            Toasts.Show(ClusterFaults.Describe(exception), LtToastTone.Danger);
        }
        finally
        {
            _cancelling = false;
        }
    }

    private void OnWatchChanged() => _ = InvokeAsync(StateHasChanged);

    private void OnWatchFinished(LatticeOperationStatus status) => _ = InvokeAsync(async () =>
    {
        if (_load.IsLeft)
        {
            return;
        }

        switch (status.State)
        {
            case LatticeOperationState.Succeeded:
                _receipt = ReceiptFrom(status);
                _watch.Clear();
                Toasts.Show("WAL partition moved.", LtToastTone.Success);
                StateHasChanged();
                await RereadPlacementAsync();
                break;
            case LatticeOperationState.Cancelled:
                Toasts.Show("The move was stopped before its flip: the partition stays on its source.", LtToastTone.Warning);
                break;
            default:
                Toasts.Show("The WAL move failed. Audit the placement before trying again.", LtToastTone.Danger);
                break;
        }

        StateHasChanged();
    });

    // A finished move flips the partition to its target under a new placement
    // version, and a reclaim drops the retained source: the audit and the plan on
    // screen were read before either, so both are read again. A failed re-read
    // keeps what was shown.
    private async Task RereadPlacementAsync()
    {
        if (TreeId is not { } tree || _load.IsLeft)
        {
            return;
        }

        var token = _load.Token;
        var audit = ClusterLoad<TreeWalPlacementAudit>.RunAsync(ct => Facades.RequireTreeAdmin().AuditWalPlacementAsync(tree, ct), token);
        var plan = Partition is { } partition && Target is { } target
            ? ClusterLoad<TreeWalMovePlan>.RunAsync(ct => Facades.RequireTreeAdmin().PlanWalMoveAsync(tree, partition, target, ct), token)
            : null;
        var auditLoad = await audit;
        var planLoad = plan is null ? null : await plan;
        if (token.IsCancellationRequested)
        {
            return;
        }

        if (auditLoad.Value is not null)
        {
            _audit = auditLoad;
        }

        if (planLoad?.Value is not null)
        {
            _plan = planLoad;
        }

        StateHasChanged();
    }

    /// <summary>The receipt a finished move's result describes.</summary>
    /// <param name="status">The succeeded move's status.</param>
    /// <returns>The receipt.</returns>
    internal static TreeWalMoveReceipt ReceiptFrom(LatticeOperationStatus status)
    {
        var result = status.Result;
        string Text(string key) => result.TryGetValue(key, out var value) ? value : string.Empty;
        long Number(string key) => long.TryParse(Text(key), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var value) ? value : 0;
        return new TreeWalMoveReceipt
        {
            TreeId = Text(TreeAdminOperationResultKeys.TreeId),
            Partition = (int)Number(TreeAdminOperationResultKeys.Partition),
            FromProviderKey = Text(TreeAdminOperationResultKeys.FromProviderKey),
            ToProviderKey = Text(TreeAdminOperationResultKeys.ToProviderKey),
            PreviousPlacementVersion = Number(TreeAdminOperationResultKeys.PreviousPlacementVersion),
            NewPlacementVersion = Number(TreeAdminOperationResultKeys.NewPlacementVersion),
            CopiedFromOffset = Number(TreeAdminOperationResultKeys.CopiedFromOffset),
            CopiedThroughOffset = Number(TreeAdminOperationResultKeys.CopiedThroughOffset),
            SourceHighestOffset = Number(TreeAdminOperationResultKeys.SourceHighestOffset),
            TargetHighestOffset = Number(TreeAdminOperationResultKeys.TargetHighestOffset),
            SourceRetained = string.Equals(Text(TreeAdminOperationResultKeys.SourceRetained), "true", StringComparison.Ordinal),
            Outcome = Enum.TryParse<TreeWalMoveOutcome>(Text(TreeAdminOperationResultKeys.Outcome), out var outcome) ? outcome : TreeWalMoveOutcome.Moved,
        };
    }
    private async Task ReclaimAsync()
    {
        _confirm = Verb.None;
        if (TreeId is not { } tree || Partition is not { } partition || ReclaimSource is not { } source || Busy)
        {
            return;
        }

        _busy = true;
        var receipt = await ClusterLoad<TreeWalMoveReceipt>.RunAsync(
            ct => Facades.RequireTreeAdmin().ReclaimMovedWalSourceAsync(tree, partition, source, ct),
            _load.Token);
        _busy = false;

        if (receipt.Value is { } value)
        {
            _receipt = value;
            _reclaimKey = null;
            Toasts.Show("WAL source reclaimed.", LtToastTone.Success);
            await RereadPlacementAsync();
        }
        else
        {
            Toasts.Show(receipt.Error!, LtToastTone.Danger);
        }
    }

    private static IReadOnlyList<LtSelectOption> SourceOptions(TreeWalPlacementAudit audit, string target) =>
    [
        new(string.Empty, "Choose a provider key"),
        .. (audit.KnownProviderKeys.IsDefault ? [] : audit.KnownProviderKeys)
            .Where(key => !string.Equals(key, target, StringComparison.Ordinal))
            .Select(key => new LtSelectOption(key, key)),
    ];

    private static string KnownKeys(TreeWalPlacementAudit audit) =>
        audit.KnownProviderKeys.IsDefaultOrEmpty ? "none" : string.Join(", ", audit.KnownProviderKeys);

    private static string KeyText(string key) => string.IsNullOrEmpty(key) ? "default" : key;

    private static string ReceiptText(TreeWalMoveReceipt receipt) => receipt.Outcome switch
    {
        TreeWalMoveOutcome.Moved => $"Moved: offsets {ClusterFormat.Count(receipt.CopiedFromOffset)} to {ClusterFormat.Count(receipt.CopiedThroughOffset)} copied to {KeyText(receipt.ToProviderKey)}, placement version {ClusterFormat.Count(receipt.NewPlacementVersion)}."
            + (receipt.SourceRetained ? " The source is retained until you reclaim it." : string.Empty),
        TreeWalMoveOutcome.AlreadyAtTarget => "The partition was already at the target; its placement was repaired without a copy.",
        TreeWalMoveOutcome.SourceReclaimed => $"The retained log on {KeyText(receipt.FromProviderKey)} was reclaimed. The move can no longer be reverted.",
        _ => "Nothing changed.",
    };
}
