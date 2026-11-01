using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Faults;

/// <summary>
/// One estate shared by every fixture in this namespace: booting one takes a minute or two,
/// and the fault-seam fixtures arrange disjoint state on it.
/// </summary>
[SetUpFixture]
public sealed class FaultEstate
{
    private static JourneyEstate? _current;

    /// <summary>The shared estate.</summary>
    public static JourneyEstate Current => _current ?? throw new InvalidOperationException("The fault estate has not started.");

    /// <summary>Boots the estate.</summary>
    [OneTimeSetUp]
    public async Task StartAsync() => _current = await JourneyEstate.StartAsync(new JourneyEstateOptions { Name = "faults" });

    /// <summary>Stops it and removes its storage.</summary>
    [OneTimeTearDown]
    public async Task StopAsync()
    {
        if (_current is not null)
        {
            await _current.DisposeAsync();
            _current = null;
        }
    }
}
