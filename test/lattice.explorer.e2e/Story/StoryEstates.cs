using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>
/// The estate each storyline runs on (decision D2): Contoso, non-tenanted, from
/// <see cref="EstateProfile.Contoso"/> with its persona catalogue (#5360); Fabrikam Cloud,
/// tenanted, from <see cref="EstateProfile.Fabrikam"/> with its own cast. Journeys, story runs
/// and the contract conformance run start their estate here, never with options of their own.
/// </summary>
public static class StoryEstates
{
    /// <summary>
    /// How many backups a scheduled scope keeps before retention prunes it. Each scheduled
    /// cycle prunes every manifest of its scope beyond this count, the operator's own captures
    /// included, so at the reference default (7) A6's compressed telemetry schedule would prune
    /// the 60 historical telemetry backups A10 pages through. Contoso keeps them all.
    /// </summary>
    public const int ContosoRetentionKeepLast = 1000;

    /// <summary>The options of <paramref name="storyline"/>'s estate.</summary>
    /// <param name="storyline">A storyline from <see cref="StoryNames.Storylines"/>.</param>
    public static JourneyEstateOptions OptionsFor(string storyline) => storyline switch
    {
        StoryNames.Storylines.Contoso => Contoso(),
        StoryNames.Storylines.Fabrikam => Fabrikam(),
        _ => throw new ArgumentException($"'{storyline}' is not a storyline of the epic.", nameof(storyline)),
    };

    private static JourneyEstateOptions Contoso()
    {
        var profile = EstateProfile.Contoso;
        profile.SiloEnvironment["Backup__RetentionKeepLast"] = ContosoRetentionKeepLast.ToString(System.Globalization.CultureInfo.InvariantCulture);
        return profile;
    }

    private static JourneyEstateOptions Fabrikam()
    {
        var profile = EstateProfile.Fabrikam;
        return new JourneyEstateOptions
        {
            Name = profile.Name,
            East = profile.East,
            Prometheus = profile.Prometheus,
            Tenancy = profile.Tenancy,
            DelegatedAccessAdministration = profile.DelegatedAccessAdministration,
            AllowInteractiveEndpointConfiguration = profile.AllowInteractiveEndpointConfiguration,
            LineFeed = profile.LineFeed,
            Users = Cast.Personas.FabrikamUsers(),
            BootstrapAdministrators = Cast.Personas.BootstrapAdministratorsOf(Cast.Personas.FabrikamEstate),
        };
    }

    /// <summary>Starts a fresh estate for <paramref name="storyline"/>.</summary>
    /// <param name="storyline">A storyline from <see cref="StoryNames.Storylines"/>.</param>
    /// <param name="cancellationToken">Cancels the start.</param>
    public static Task<JourneyEstate> StartAsync(string storyline, CancellationToken cancellationToken = default) =>
        JourneyEstate.StartAsync(OptionsFor(storyline), cancellationToken);
}
