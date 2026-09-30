using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// The Schema area: the trees under a schema policy, version config or app
/// declaration, and each tree's policy, versions, compliance, remediation and
/// dead letters. Bound to <see cref="Orleans.Lattice.Api.Schema.ILatticeSchemaControl"/>
/// only; it replaces the optional Schema plugin and needs no registration of its
/// own - it appears whenever the facade's capability probe admits the caller.
/// </summary>
internal sealed class SchemaArea : IExplorerArea
{
    /// <summary>The id of the command that picks a tree and scans its compliance.</summary>
    public const string ScanCommandId = "schema.scan-compliance";

    /// <summary>The id of the command that lists every tree, governed or not.</summary>
    public const string AllTreesCommandId = "schema.all-trees";

    private readonly SchemaAccess _access;
    private readonly SchemaDirectory _directory;

    /// <summary>Creates the area over the circuit's access answer, directory, completions and command signals.</summary>
    /// <param name="access">The circuit's access answer.</param>
    /// <param name="directory">The circuit's schema directory.</param>
    /// <param name="completions">The area's address completions.</param>
    /// <param name="signals">Carries palette commands to the page.</param>
    public SchemaArea(SchemaAccess access, SchemaDirectory directory, SchemaCompletionSource completions, SchemaCommandSignals signals)
    {
        ArgumentNullException.ThrowIfNull(access);
        ArgumentNullException.ThrowIfNull(directory);
        ArgumentNullException.ThrowIfNull(completions);
        ArgumentNullException.ThrowIfNull(signals);
        _access = access;
        _directory = directory;
        Completions = completions;
        Commands =
        [
            new ExplorerCommand(ScanCommandId, "Scan compliance...")
            {
                Detail = "Choose a tree and check every value against its policy",
                Target = SchemaAddresses.Directory,
                InvokeAsync = _ => signals.RequestAsync(ScanCommandId),
            },
            new ExplorerCommand(AllTreesCommandId, "Show every tree's schema")
            {
                Detail = "Include trees with no policy or version config",
                Target = SchemaAddresses.AllTrees,
            },
        ];
    }

    /// <inheritdoc />
    public string Key => SchemaAddresses.AreaKey;

    /// <inheritdoc />
    public string DisplayName => "Schema";

    /// <inheritdoc />
    public int DirectoryOrder => 40;

    /// <inheritdoc />
    public IAddressCompletionSource? Completions { get; }

    /// <inheritdoc />
    public IReadOnlyList<ExplorerCommand> Commands { get; }

    /// <inheritdoc />
    /// <remarks>Every path segment below <c>/schema</c> belongs to the logical tree id, so the path is one node.</remarks>
    public IReadOnlyList<int>? GetChainSpans(Navigation.Address.ExplorerAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);
        return address.Path.Count == 0 ? null : [address.Path.Count];
    }

    /// <inheritdoc />
    public ValueTask<AreaAvailability> GetAvailabilityAsync(CancellationToken cancellationToken) =>
        _access.GetAvailabilityAsync(cancellationToken);

    /// <inheritdoc />
    public async ValueTask<string?> GetHomeStatusAsync(CancellationToken cancellationToken)
    {
        var read = await _directory.GetAsync(refresh: false, cancellationToken).ConfigureAwait(false);
        var governed = read.Governed.ToArray();
        if (governed.Length == 0)
        {
            return "No tree is under a schema policy yet.";
        }

        var versioned = governed.Count(row => row.Version is not null);
        return SchemaFormat.Count(governed.Length, "tree") + " under schema, " + versioned.ToString("N0", System.Globalization.CultureInfo.InvariantCulture) + " versioned.";
    }

    /// <inheritdoc />
    public ValueTask<string?> GetDirectoryBadgeAsync(CancellationToken cancellationToken)
    {
        // Asked on every navigation: answer only from a listing already read.
        if (_directory.Last is not { } read)
        {
            return ValueTask.FromResult<string?>(null);
        }

        var governed = read.Governed.Count();
        return ValueTask.FromResult<string?>(governed == 0 ? null : governed.ToString("N0", System.Globalization.CultureInfo.InvariantCulture));
    }
}
