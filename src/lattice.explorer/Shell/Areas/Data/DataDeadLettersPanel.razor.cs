using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.DeadLetter;

namespace Orleans.Lattice.Explorer.Shell.Areas.Data;

/// <summary>The Dead letters tab: the tree's strict-mode dead-letter queue, read through the Core dead-letter reader.</summary>
public partial class DataDeadLettersPanel : IDisposable
{
    /// <summary>How many dead letters one page asks for.</summary>
    internal const int PageSize = 50;

    private readonly CancellationTokenSource _lifetime = new();
    private IReadOnlyList<DeadLetterEntry> _entries = [];
    private string? _loadedFor;
    private string? _continuation;
    private int? _count;
    private DeadLetterEntry? _selected;
    private bool _loading;
    private string? _error;

    [CascadingParameter]
    internal DataWorkspace? Workspace { get; set; }

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    private string CountText => _count is { } count ? DataArea.Plural(count, "dead letter") : string.Empty;

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Workspace is { } workspace && _loadedFor != workspace.Tree.StateId)
        {
            _loadedFor = workspace.Tree.StateId;
            await ReloadAsync();
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Cancel();
        _lifetime.Dispose();
        GC.SuppressFinalize(this);
    }

    internal static string SourceText(DeadLetterSource source) => source switch
    {
        DeadLetterSource.Replication => "Replication",
        DeadLetterSource.Restore => "Restore",
        DeadLetterSource.LocalRejected => "Local write",
        _ => "Unknown",
    };

    private static string Clip(string content) => content.Length <= DataValueRendering.DisplayLimit
        ? content
        : string.Concat(content.AsSpan(0, DataValueRendering.DisplayLimit), "\n...");

    private void Select(DeadLetterEntry entry) => _selected = ReferenceEquals(entry, _selected) ? null : entry;

    private async Task ReloadAsync()
    {
        _entries = [];
        _continuation = null;
        _selected = null;
        _count = null;
        if (Reader() is { } reader && Workspace is { } workspace)
        {
            try
            {
                _count = await reader.CountAsync(workspace.Tree.StateId, _lifetime.Token);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                // The count is a summary; the list below reports a failure.
            }
        }

        await LoadMoreAsync();
    }

    private async Task LoadMoreAsync()
    {
        if (Workspace is not { } workspace)
        {
            return;
        }

        if (Reader() is not { } reader)
        {
            _error = "This Explorer has no state API to read dead letters through.";
            return;
        }

        _loading = true;
        _error = null;
        try
        {
            var page = await reader.ListAsync(workspace.Tree.StateId, PageSize, _continuation, _lifetime.Token);
            _entries = [.. _entries, .. page.Entries];
            _continuation = page.HasMore ? page.ContinuationToken : null;
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            _error = DataErrors.Describe(exception, "read this tree's dead letters");
        }
        finally
        {
            _loading = false;
        }
    }

    private IDeadLetterReader? Reader() => DataServices.Find<IDeadLetterReader>(Services);
}
