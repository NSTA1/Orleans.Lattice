using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>
/// The estate each storyline runs on (decision D2): Contoso, non-tenanted; Fabrikam Cloud,
/// tenanted. Personas (#5360) and the east region and Fabrikam's full shape (#5362) extend these
/// options; journeys and story runs start their estate here, never with options of their own.
/// </summary>
public static class StoryEstates
{
    /// <summary>The options of <paramref name="storyline"/>'s estate.</summary>
    /// <param name="storyline">A storyline from <see cref="StoryNames.Storylines"/>.</param>
    public static JourneyEstateOptions OptionsFor(string storyline) => storyline switch
    {
        StoryNames.Storylines.Contoso => new JourneyEstateOptions { Name = StoryNames.Storylines.Contoso },
        StoryNames.Storylines.Fabrikam => new JourneyEstateOptions { Name = StoryNames.Storylines.Fabrikam, Tenancy = true },
        _ => throw new ArgumentException($"'{storyline}' is not a storyline of the epic.", nameof(storyline)),
    };

    /// <summary>Starts a fresh estate for <paramref name="storyline"/>.</summary>
    /// <param name="storyline">A storyline from <see cref="StoryNames.Storylines"/>.</param>
    /// <param name="cancellationToken">Cancels the start.</param>
    public static Task<JourneyEstate> StartAsync(string storyline, CancellationToken cancellationToken = default) =>
        JourneyEstate.StartAsync(OptionsFor(storyline), cancellationToken);
}
