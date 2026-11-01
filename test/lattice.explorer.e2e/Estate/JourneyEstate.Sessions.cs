using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

/// <summary>Persona sessions on the estate's Explorer heads (issue #5360).</summary>
public sealed partial class JourneyEstate
{
    private HostFaultLog? _hostFaults;

    /// <summary>
    /// The estate's host-fault log, shared by every session: an Explorer head's or a gateway's
    /// error fails the checkpoint that follows it; a silo's is reported with it, because fault
    /// journeys provoke silo errors on purpose.
    /// </summary>
    public HostFaultLog HostFaults => _hostFaults ??= new HostFaultLog(HostLogSources);

    /// <summary>
    /// Opens a session for <paramref name="persona"/> on an Explorer head of
    /// <paramref name="region"/> and signs them in through the Explorer's own sign-in form.
    /// Several sessions may be open at once, each in its own browser context.
    /// </summary>
    /// <param name="persona">The persona, from <see cref="Personas"/>.</param>
    /// <param name="region">The region, <see cref="West"/> by default.</param>
    /// <param name="options">How the session is opened, or <see langword="null"/> for the defaults.</param>
    public async Task<PersonaSession> SignInAsAsync(Persona persona, string region = West, PersonaSessionOptions? options = null)
    {
        var session = await OpenSessionAsync(persona, region, options);
        try
        {
            await session.SignInAsync();
            return session;
        }
        catch
        {
            await session.DisposeAsync();
            throw;
        }
    }

    /// <summary>
    /// Opens a session for <paramref name="persona"/> on an Explorer head of
    /// <paramref name="region"/>, signed out: for a journey that signs in itself (or fails to).
    /// </summary>
    /// <param name="persona">The persona, from <see cref="Personas"/>.</param>
    /// <param name="region">The region, <see cref="West"/> by default.</param>
    /// <param name="options">How the session is opened, or <see langword="null"/> for the defaults.</param>
    public Task<PersonaSession> OpenSessionAsync(Persona persona, string region = West, PersonaSessionOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(persona);
        options ??= new PersonaSessionOptions();
        var heads = RegionOf(region).ExplorerUris;
        if (options.Explorer < 0 || options.Explorer >= heads.Count)
        {
            throw new ArgumentOutOfRangeException(nameof(options), $"Region '{region}' has {heads.Count} Explorer head(s); there is no head {options.Explorer}.");
        }

        return PersonaSession.OpenAsync(heads[options.Explorer], persona, region, options, HostFaults, ArtifactsDirectory);
    }

    private IEnumerable<HostLogSource> HostLogSources()
    {
        foreach (var region in _regions.Values)
        {
            foreach (var explorer in region.ExplorerProcesses)
            {
                yield return new HostLogSource(explorer.Name, explorer.LogFile, FailsCheckpoint: true);
            }

            yield return new HostLogSource(region.GatewayProcess.Name, region.GatewayProcess.LogFile, FailsCheckpoint: true);
            foreach (var silo in region.Silos)
            {
                yield return new HostLogSource(silo.Process.Name, silo.LogFile, FailsCheckpoint: false);
            }
        }
    }
}
