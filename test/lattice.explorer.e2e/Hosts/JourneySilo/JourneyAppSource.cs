using Microsoft.Extensions.Options;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Sources;
using TaskBoardApp = Orleans.Lattice.Samples.Explorer.TaskBoard.TaskBoardApp;

namespace Orleans.Lattice.Explorer.EndToEnd.Hosts.Silo;

/// <summary>
/// A named, static app catalogue source over apps embedded in this host, the way an
/// operator publishes a curated catalogue: the in-image source's resolution, listing and
/// digest-verified asset serving, under a source key of its own. The journey estate
/// registers two, <see cref="Samples"/> and <see cref="SamplesNext"/>, so installing from a
/// named source, upgrading across sources and re-consenting are all reachable.
/// </summary>
internal sealed class JourneyAppSource : IAppCatalogSource
{
    /// <summary>The source of the task-board sample as it ships.</summary>
    public const string Samples = "samples";

    /// <summary>The source of the task board's next version.</summary>
    public const string SamplesNext = "samples-next";

    private readonly InImageAppSource _inner;

    private JourneyAppSource(string key, string displayName, InImageAppRegistration registration)
    {
        Descriptor = new AppSourceDescriptor(key, displayName, AppSourceKind.Static, AppSourceCapabilities.Enumerate);
        var options = new InImageAppSourceOptions();
        options.Registrations.Add(registration);
        _inner = new InImageAppSource(Options.Create(options));
    }

    /// <inheritdoc />
    public AppSourceDescriptor Descriptor { get; }

    /// <summary>The <see cref="Samples"/> source: the task-board sample at its shipped version.</summary>
    public static JourneyAppSource CreateSamples() =>
        new(Samples, "Samples", new InImageAppRegistration(
            AppSlug.Parse(TaskBoardApp.Slug),
            TaskBoardApp.Assembly,
            TaskBoardApp.ManifestResourceName));

    /// <summary>The <see cref="SamplesNext"/> source: the task board's next version, one added operation.</summary>
    public static JourneyAppSource CreateSamplesNext() =>
        new(SamplesNext, "Samples (next)", new InImageAppRegistration(
            AppSlug.Parse(TaskBoardApp.Slug),
            typeof(JourneyAppSource).Assembly,
            "JourneySilo.TaskBoardNext.manifest.json")
        {
            AssetResourcePrefix = "JourneySilo.TaskBoardNext.ui.",
        });

    /// <inheritdoc />
    public async ValueTask<AppSourceResult> ResolveAsync(AppSlug slug, AppVersion? version = null, CancellationToken cancellationToken = default)
    {
        var result = await _inner.ResolveAsync(slug, version, cancellationToken);
        return result is { IsResolved: true, Manifest: { } manifest, Provenance: { } provenance, Activation: { } activation }
            ? AppSourceResult.Resolved(manifest, Rekey(provenance), activation)
            : result;
    }

    /// <inheritdoc />
    public async ValueTask<AppSourcePage> ListAsync(AppSourceQuery query, CancellationToken cancellationToken = default)
    {
        var page = await _inner.ListAsync(query, cancellationToken);
        var entries = new AppSourceEntry[page.Entries.Count];
        for (var i = 0; i < entries.Length; i++)
        {
            var entry = page.Entries[i];
            entries[i] = entry is { Manifest: { } manifest, Provenance: { } provenance }
                ? AppSourceEntry.Available(entry.Versions, manifest, Rekey(provenance))
                : entry;
        }

        return AppSourcePage.Create(entries, page.Continuation);
    }

    /// <inheritdoc />
    public ValueTask<AppAssetResult> OpenAssetAsync(
        AppSlug slug,
        AppVersion version,
        string path,
        string expectedSha256,
        CancellationToken cancellationToken = default) =>
        _inner.OpenAssetAsync(slug, version, path, expectedSha256, cancellationToken);

    private AppProvenance Rekey(AppProvenance provenance) => new()
    {
        Source = Descriptor.Key,
        Publisher = provenance.Publisher,
        Reference = provenance.Reference,
    };
}
