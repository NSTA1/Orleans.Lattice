using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.DeadLetter;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>The Dead letters tab: the tree's strict-mode dead-letter queue, read through the Core dead-letter reader.</summary>
public partial class DataDeadLettersPanel : IDisposable
{
    /// <summary>How many dead letters one page asks for.</summary>
    internal const int PageSize = 50;

    private readonly ComponentLifetime _lifetime = new();
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
        _lifetime.Leave();
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
        : string.Concat(LtTextCut.Prefix(content, DataValueRendering.DisplayLimit), "\n...");

    private void Select(DeadLetterEntry entry) => _selected = ReferenceEquals(entry, _selected) ? null : entry;

    private async Task ReloadAsync()
    {
        var token = _lifetime.Renew();
        _entries = [];
        _continuation = null;
        _selected = null;
        _count = null;
        _error = null;
        _loading = true;
        if (Reader() is { } reader && Workspace is { } workspace)
        {
            try
            {
                var count = await reader.CountAsync(workspace.Tree.StateId, token);
                if (token.IsCancellationRequested)
                {
                    return;
                }

                _count = count;
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                // The count is a summary; the list below reports a failure.
            }

            if (!token.IsCancellationRequested)
            {
                await ReadPageAsync(reader, workspace.Tree.StateId, token);
            }

            return;
        }

        _loading = false;
        _error = "This Explorer has no state API to read dead letters through.";
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

        await ReadPageAsync(reader, workspace.Tree.StateId, _lifetime.Token);
    }

    private async Task ReadPageAsync(IDeadLetterReader reader, string treeId, CancellationToken token)
    {
        _loading = true;
        _error = null;
        try
        {
            var page = await reader.ListAsync(treeId, PageSize, _continuation, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            _entries = [.. _entries, .. page.Entries];
            _continuation = page.HasMore ? page.ContinuationToken : null;
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            if (!token.IsCancellationRequested)
            {
                _error = DataErrors.Describe(exception, "read this tree's dead letters");
            }
        }
        finally
        {
            if (!token.IsCancellationRequested)
            {
                _loading = false;
            }
        }
    }

    private IDeadLetterReader? Reader() => DataServices.Find<IDeadLetterReader>(Services);
}
