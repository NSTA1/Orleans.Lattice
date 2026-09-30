using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Schema;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// The Dead letters tab: counts the tree's strict-mode dead letters on arrival,
/// and lists them a page at a time on demand. The facade streams the queue, so a
/// page is the first entries of the stream; "Load more" reads further into it.
/// </summary>
public partial class SchemaDeadLettersPanel : IDisposable
{
    /// <summary>How many entries each "load" adds.</summary>
    public const int PageSize = 100;

    /// <summary>The most characters of a key the table shows before clipping it.</summary>
    public const int KeyCharacters = 96;

    private readonly ComponentLifetime _lifetime = new();
    private IReadOnlyList<LatticeSchemaDeadLetterEntry>? _entries;
    private string? _loadedTree;
    private string? _error;
    private int? _count;
    private int _limit = PageSize;
    private bool _more;
    private bool _loading;

    [CascadingParameter]
    internal SchemaWorkspace? Workspace { get; set; }

    [Inject]
    internal SchemaFacades Facades { get; set; } = default!;

    private string CountText => _count switch
    {
        null when _loading => "Counting the dead letters...",
        null => string.Empty,
        { } count when _entries is { } entries && entries.Count < count => $"{SchemaFormat.Count(count, "dead letter")}; showing the first {entries.Count.ToString("N0", System.Globalization.CultureInfo.InvariantCulture)}.",
        { } count => SchemaFormat.Count(count, "dead letter") + ".",
    };

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Leave();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Workspace is { } workspace && !string.Equals(workspace.TreeId, _loadedTree, StringComparison.Ordinal))
        {
            _loadedTree = workspace.TreeId;
            _entries = null;
            _limit = PageSize;
            await CountAsync();
        }
    }

    /// <summary>A key clipped for a table cell; the detail shows it whole.</summary>
    /// <param name="key">The key.</param>
    /// <returns>The clipped key.</returns>
    internal static string Clip(string key) => key.Length > KeyCharacters ? key[..KeyCharacters] + "..." : key;

    private async Task CountAsync()
    {
        if (Workspace is not { } workspace || !workspace.Grants.ViewDeadLetters)
        {
            return;
        }

        _loading = true;
        _error = null;
        try
        {
            _count = await Facades.RequireSchema().CountDeadLettersAsync(workspace.TreeId, _lifetime.Token);
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
        {
        }
        catch (Exception exception)
        {
            _error = SchemaFailure.Describe(exception, "count the dead letters");
        }
        finally
        {
            _loading = false;
        }
    }

    private Task LoadAsync() => ReadAsync(PageSize);

    private Task LoadMoreAsync() => ReadAsync(_limit + PageSize);

    private async Task ReloadAsync()
    {
        await CountAsync();
        if (_error is null && _entries is not null)
        {
            await ReadAsync(_limit);
        }
    }

    private async Task ReadAsync(int limit)
    {
        if (Workspace is not { } workspace)
        {
            return;
        }

        _loading = true;
        _error = null;
        try
        {
            var entries = new List<LatticeSchemaDeadLetterEntry>(Math.Min(limit, 1024));
            var more = false;
            await foreach (var entry in Facades.RequireSchema().ListDeadLettersAsync(workspace.TreeId, _lifetime.Token))
            {
                if (entries.Count == limit)
                {
                    more = true;
                    break;
                }

                entries.Add(entry);
            }

            _entries = entries;
            _limit = limit;
            _more = more;
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
        {
        }
        catch (Exception exception)
        {
            _error = SchemaFailure.Describe(exception, "list the dead letters");
        }
        finally
        {
            _loading = false;
        }
    }
}
