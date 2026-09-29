using System.Globalization;
using System.Text;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Shell.Design.Components;

namespace Orleans.Lattice.Explorer.Shell.Areas.Cluster.Pages;

/// <summary>
/// <c>/cluster/trees/{tree-path}/tools</c>: a tree's admin tools - an
/// out-of-cycle compaction pass (admin authority, typed confirmation), a shard's
/// projection digest (read authority), and a resumable bulk load into an empty
/// tree (the BulkLoad grant, typed confirmation). Each is hidden unless the
/// capability probe grants it.
/// </summary>
public partial class ClusterToolsPage : IDisposable
{
    /// <summary>Entries sent per bulk-load chunk.</summary>
    internal const int ChunkSize = 256;

    private readonly CancellationTokenSource _lifetime = new();
    private ClusterLoad<LatticeTreeAdminCapabilities> _accessLoad = ClusterLoad<LatticeTreeAdminCapabilities>.Loading;
    private LatticeTreeAdminCapabilities _access = default!;
    private int? _shardCount;
    private string? _compactionShard;
    private string? _compactionError;
    private int _compactionIndex;
    private bool _confirmCompaction;
    private TreeCompactionTriggerResult? _compaction;
    private string? _digestShard;
    private string? _digestError;
    private ShardProjectionDigestReport? _digest;
    private BulkStage _bulk;
    private string? _bulkText;
    private string? _bulkError;
    private List<DataEntry> _entries = [];
    private string _operationId = string.Empty;
    private long _nextChunk;
    private long _bulkKeys;
    private bool _confirmBulk;
    private bool _busy;

    private enum BulkStage
    {
        Compose,
        Review,
        Loading,
        Failed,
        Done,
    }

    /// <summary>The logical tree id.</summary>
    [Parameter, EditorRequired]
    public string TreeId { get; set; } = string.Empty;

    [Inject]
    private ClusterFacades Facades { get; set; } = default!;

    [Inject]
    private LtToastService Toasts { get; set; } = default!;

    private int ChunkCount => (_entries.Count + ChunkSize - 1) / ChunkSize;

    private string ShardHint => _shardCount is { } count
        ? $"A shard index from 0 to {count - 1}."
        : "A zero-based shard index.";

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Cancel();
        _lifetime.Dispose();
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        _access = ClusterTreeAccess.None(TreeId);
        _accessLoad = await ClusterLoad<LatticeTreeAdminCapabilities>.RunAsync(
            ct => ClusterTreeAccess.ProbeAsync(Facades.RequireTreeAdmin(), TreeId, ct),
            _lifetime.Token);
        _access = _accessLoad.Value ?? _access;

        if (_access.CanViewDiagnostics)
        {
            var stats = await ClusterLoad<TreeStatsReport>.RunAsync(ct => Facades.RequireTreeAdmin().GetTreeStatsAsync(TreeId, ct), _lifetime.Token);
            _shardCount = stats.Value?.ShardCount;
        }
    }

    private bool TryShard(string? text, out int shard, out string? error)
    {
        error = null;
        if (!int.TryParse(text?.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out shard))
        {
            error = "Enter a shard index: a whole number from 0.";
            return false;
        }

        if (_shardCount is { } count && shard >= count)
        {
            error = $"The tree has {ClusterFormat.Plural(count, "shard")}: enter 0 to {count - 1}.";
            return false;
        }

        return true;
    }

    private void ReviewCompaction()
    {
        if (TryShard(_compactionShard, out var shard, out _compactionError))
        {
            _compactionIndex = shard;
            _confirmCompaction = true;
        }
    }

    private async Task CompactAsync()
    {
        _busy = true;
        var shard = _compactionIndex;
        var result = await ClusterLoad<TreeCompactionTriggerResult>.RunAsync(
            ct => Facades.RequireTreeAdmin().TriggerShardCompactionAsync(TreeId, shard, ct),
            _lifetime.Token);
        _busy = false;
        _compaction = result.Value;
        if (result.Error is { } error)
        {
            Toasts.Show(error, LtToastTone.Danger);
        }
    }

    private async Task ReadDigestAsync()
    {
        if (!TryShard(_digestShard, out var shard, out _digestError))
        {
            return;
        }

        _busy = true;
        var result = await ClusterLoad<ShardProjectionDigestReport>.RunAsync(
            ct => Facades.RequireTreeAdmin().GetProjectionDigestAsync(TreeId, shard, ct),
            _lifetime.Token);
        _busy = false;
        _digest = result.Value;
        _digestError = result.Error;
    }

    private void ReviewBulk()
    {
        _bulkError = null;
        if (!TryParseEntries(_bulkText, out var entries, out var error))
        {
            _bulkError = error;
            return;
        }

        _entries = entries;
        _operationId = Guid.NewGuid().ToString("N");
        _nextChunk = 0;
        _bulk = BulkStage.Review;
    }

    private async Task RunBulkAsync()
    {
        _bulk = BulkStage.Loading;
        _bulkError = null;
        var admin = Facades.RequireTreeAdmin();
        var token = _lifetime.Token;

        try
        {
            if (_nextChunk == 0)
            {
                await admin.BeginBulkLoadAsync(TreeId, _operationId, token);
            }

            while (_nextChunk < ChunkCount)
            {
                var chunk = _entries.GetRange((int)(_nextChunk * ChunkSize), (int)Math.Min(ChunkSize, _entries.Count - (_nextChunk * ChunkSize)));
                var ack = await admin.AppendBulkLoadAsync(TreeId, _operationId, _nextChunk, chunk, token);
                _nextChunk = ack.NextChunkIndex;
                StateHasChanged();
            }

            var result = await admin.CommitBulkLoadAsync(TreeId, _operationId, token);
            _bulkKeys = result.TotalLiveKeys;
            _bulk = BulkStage.Done;
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _bulkError = ClusterFaults.Describe(exception);
            _bulk = BulkStage.Failed;
        }
    }

    private void ResetBulk()
    {
        _entries = [];
        _nextChunk = 0;
        _bulkError = null;
        _bulk = BulkStage.Compose;
    }

    /// <summary>Reads <c>key=value</c> lines into strictly ascending entries.</summary>
    /// <param name="text">The lines.</param>
    /// <param name="entries">The entries, when the text is valid.</param>
    /// <param name="error">What is wrong, when it is not.</param>
    /// <returns><see langword="true"/> when every line is an entry and the keys ascend strictly.</returns>
    internal static bool TryParseEntries(string? text, out List<DataEntry> entries, out string? error)
    {
        entries = [];
        error = null;
        var lines = (text ?? string.Empty).Split('\n');
        for (var index = 0; index < lines.Length; index++)
        {
            var line = lines[index].TrimEnd('\r');
            if (line.Length == 0)
            {
                continue;
            }

            var split = line.IndexOf('=', StringComparison.Ordinal);
            if (split <= 0)
            {
                error = $"Line {index + 1} is not key=value.";
                return false;
            }

            var key = line[..split];
            if (entries.Count > 0 && string.CompareOrdinal(entries[^1].Key, key) >= 0)
            {
                error = $"Line {index + 1}: keys must ascend strictly, and {key} does not follow {entries[^1].Key}.";
                return false;
            }

            entries.Add(new DataEntry { Key = key, Value = Encoding.UTF8.GetBytes(line[(split + 1)..]) });
        }

        if (entries.Count == 0)
        {
            error = "Enter at least one key=value line.";
            return false;
        }

        return true;
    }
}
