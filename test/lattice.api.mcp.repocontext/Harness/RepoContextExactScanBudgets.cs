using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// Builds <see cref="RepoContextExactScanBudget"/> instances for fixtures, over
/// the scan-page options a deployment would configure on the vector-metadata
/// tree.
/// </summary>
/// <remarks>
/// Most fixtures are not about the budget at all and want
/// <see cref="Unbounded"/>, which restores the behaviour that existed before the
/// exact gather was bounded: the fallback runs whatever the corpus size. Fixtures
/// that are about the budget build one from explicit options so the threshold
/// under test is visibly a projection of them.
/// </remarks>
internal static class RepoContextExactScanBudgets
{
    /// <summary>
    /// A budget that never bounds the exact gather, because the deployment
    /// disabled the page-fill stall ceiling and so has no ceiling for a gather to
    /// trip.
    /// </summary>
    /// <returns>An unbounded budget.</returns>
    internal static RepoContextExactScanBudget Unbounded()
        => From(new LatticeOptions { MaxScanPageStallDuration = Timeout.InfiniteTimeSpan });

    /// <summary>
    /// The shipped defaults: the Orleans default 30 second response timeout
    /// derives a 25 second stall ceiling against a 5 second cooperative page
    /// budget, so a gather may visit five pages.
    /// </summary>
    /// <returns>A budget over the default options.</returns>
    internal static RepoContextExactScanBudget Default() => From(new LatticeOptions());

    /// <summary>Builds a budget over explicit tree options.</summary>
    /// <param name="options">The vector-metadata tree's effective options. Must not be <see langword="null"/>.</param>
    /// <param name="responseTimeout">
    /// The silo response timeout the unset-ceiling derivation reads, or
    /// <see langword="null"/> for the Orleans default.
    /// </param>
    /// <returns>A budget over <paramref name="options"/>.</returns>
    internal static RepoContextExactScanBudget From(
        LatticeOptions options, TimeSpan? responseTimeout = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);

        var messaging = new SiloMessagingOptions();
        if (responseTimeout is { } timeout)
        {
            messaging.ResponseTimeout = timeout;
        }

        return new RepoContextExactScanBudget(monitor, Options.Create(messaging));
    }
}
