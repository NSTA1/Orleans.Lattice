using System.Collections.Immutable;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// Maps the manifest's untrusted presentation and UI sections, and the consented bridge grants, to and from
/// their wire forms. Presentation text is carried verbatim as plain text; consumers never interpret it as
/// markup.
/// </summary>
internal static class AppsPresentationMapping
{
    /// <summary>Maps a manifest presentation to its wire form.</summary>
    /// <param name="presentation">The manifest presentation, or null.</param>
    /// <returns>The wire presentation, or null when none is declared.</returns>
    public static AppPresentationDescriptor? ToWirePresentation(AppPresentation? presentation)
    {
        if (presentation is null)
        {
            return null;
        }

        return new AppPresentationDescriptor
        {
            DisplayName = presentation.DisplayName,
            Summary = presentation.Summary,
            Description = presentation.Description,
            Icon = presentation.Icon is { } icon ? new AppIconDescriptor { Path = icon.Path, Sha256 = icon.Digest } : null,
            Categories = presentation.Categories is { Length: > 0 } categories ? [.. categories] : [],
            DocumentationUrl = presentation.DocumentationUrl,
            PublisherDisplayName = presentation.PublisherDisplayName,
        };
    }

    /// <summary>Maps a manifest's UI section, with its normalised bridge request, to the wire form.</summary>
    /// <param name="manifest">The manifest.</param>
    /// <returns>The wire UI descriptor, or null when the manifest ships no UI.</returns>
    public static AppUiDescriptor? ToWireUi(AppManifest manifest)
    {
        if (manifest.Ui is not { } ui)
        {
            return null;
        }

        return new AppUiDescriptor
        {
            Entry = ui.Entry,
            Styles = ui.Styles is { Length: > 0 } styles ? [.. styles] : [],
            Scripts = AppsControlMapping.Map(ui.Scripts, static s => new AppUiScriptDescriptor { Path = s.Path, Module = s.Module }),
            Assets = AppsControlMapping.Map(ui.Assets, static a => new AppUiAssetDescriptor
            {
                Path = a.Path,
                MediaType = a.MediaType,
                Sha256 = a.Digest,
            }),
            BundleDigest = ui.BundleDigest,
            Bridge = ToWireGrants(AppUiBridgeRequest.FromManifest(manifest)),
            MinProtocol = ui.MinProtocol,
        };
    }

    /// <summary>Maps a consented bridge set to its wire form.</summary>
    /// <param name="consented">The consented set, or null when none was recorded.</param>
    /// <returns>The wire grants, or null when none was recorded.</returns>
    public static ImmutableArray<AppUiBridgeGrantDescriptor>? ToWireConsent(AppUiBridgeRequest? consented) =>
        consented is null ? null : ToWireGrants(consented);

    /// <summary>
    /// Validates caller-supplied bridge grants and normalises them into an engine consent set. A null value
    /// means "leave the consent unchanged" and maps to null.
    /// </summary>
    /// <param name="grants">The wire grants, or null.</param>
    /// <returns>The normalised consent set, or null.</returns>
    /// <exception cref="ArgumentException">A grant is null, names an unknown operation, or names an invalid tree.</exception>
    public static AppUiBridgeRequest? ToEngineConsent(ImmutableArray<AppUiBridgeGrantDescriptor>? grants)
    {
        if (grants is not { } value)
        {
            return null;
        }

        if (value.IsDefaultOrEmpty)
        {
            return AppUiBridgeRequest.Empty;
        }

        var engine = new AppUiBridgeGrant[value.Length];
        for (var i = 0; i < engine.Length; i++)
        {
            if (value[i] is not { } grant || string.IsNullOrEmpty(grant.Operation))
            {
                throw new ArgumentException($"Bridge grant {i} must name an operation.", "request");
            }

            engine[i] = new AppUiBridgeGrant(grant.Operation, grant.Tree);
        }

        try
        {
            return AppUiBridgeRequest.Create(engine);
        }
        catch (ArgumentException ex)
        {
            throw new ArgumentException("A bridge grant names an unknown operation or an invalid tree: " + ex.Message, "request");
        }
    }

    private static ImmutableArray<AppUiBridgeGrantDescriptor> ToWireGrants(AppUiBridgeRequest request)
    {
        var grants = request.Grants;
        if (grants.IsDefaultOrEmpty)
        {
            return [];
        }

        var builder = ImmutableArray.CreateBuilder<AppUiBridgeGrantDescriptor>(grants.Length);
        foreach (var grant in grants)
        {
            builder.Add(new AppUiBridgeGrantDescriptor { Operation = grant.Operation, Tree = grant.Tree });
        }

        return builder.MoveToImmutable();
    }
}
