using NSubstitute;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Tests.Connection;
using Orleans.Lattice.Explorer.UI.Session;

namespace Orleans.Lattice.Explorer.Tests.UI.Session;

/// <summary>
/// The connection tester's probe itself, dialled against real endpoints: what an
/// operator is told after typing an address and pressing Test.
/// </summary>
/// <remarks>
/// <para>
/// The sibling fixture covers the tester's guards - the refusal, and which
/// headers a probe is allowed to carry - without dialling anything. This one
/// covers the part those cannot reach: <c>TestAsync</c> builds a throwaway
/// <see cref="LatticeStateConnection"/> itself, so the only way to see what it
/// reports is to give it something real to dial. Each outcome is produced by a
/// genuinely different endpoint rather than by a scripted status, which is what
/// makes the mapping meaningful: an endpoint that answers, one that demands a
/// sign-in, one that is not there, and one that never replies.
/// </para>
/// <para>
/// The endpoints are plaintext h2c on loopback, so every probe sets
/// <see cref="ExplorerConfiguration.AllowUnencryptedHttp2"/>; that is the
/// transport opt-in a local-development endpoint needs, and it is per-channel, so
/// it says nothing about the probe's own posture.
/// </para>
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class LatticeConnectionTesterProbeTests
{
    private static readonly SessionEndpointConfigurationOptions Interactive =
        new() { AllowInteractiveEndpointConfiguration = true };

    [Test]
    public async Task An_endpoint_that_answers_is_reachable()
    {
        await using var server = await StateApiH2cServer.StartAsync(Answering());

        var result = await Tester().TestAsync(Candidate(server.Address));

        Assert.That(result.Outcome, Is.EqualTo(ConnectionTestOutcome.Reachable));
    }

    [Test]
    public async Task An_endpoint_that_refuses_an_anonymous_caller_asks_for_a_sign_in()
    {
        // The binding's own default-deny authorizer is left in force, so the
        // endpoint is up and answering - it just will not serve this caller.
        await using var server = await StateApiH2cServer.StartAsync(Answering(), requireAuthorization: true);

        var result = await Tester().TestAsync(Candidate(server.Address));

        Assert.That(
            result.Outcome,
            Is.EqualTo(ConnectionTestOutcome.SignInRequired),
            "an authentication refusal still proves the endpoint is up");
    }

    [Test]
    public async Task An_endpoint_nothing_is_listening_on_is_unreachable()
    {
        var result = await Tester().TestAsync(Candidate(StateApiH2cServer.UnreachableAddress));

        Assert.That(result.Outcome, Is.EqualTo(ConnectionTestOutcome.Unreachable));
    }

    [Test]
    public async Task A_caller_that_gives_up_first_is_not_told_the_endpoint_is_unreachable()
    {
        await using var server = await StateApiH2cServer.StartAsync(Hanging());
        using var caller = new CancellationTokenSource();
        await caller.CancelAsync();

        // The tester distinguishes its own budget from the caller's cancellation:
        // only the former becomes an outcome. A caller who navigated away gets the
        // cancellation back, rather than a verdict about an endpoint nobody waited
        // for.
        Assert.That(
            async () => await Tester().TestAsync(Candidate(server.Address), caller.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task An_endpoint_that_never_replies_is_unreachable_once_the_probe_budget_is_spent()
    {
        await using var server = await StateApiH2cServer.StartAsync(Hanging());

        var result = await Tester().TestAsync(Candidate(server.Address));

        Assert.That(
            result.Outcome,
            Is.EqualTo(ConnectionTestOutcome.Unreachable),
            "the probe is bounded, and a budget it spent itself is reported rather than thrown");
    }

    private static LatticeConnectionTester Tester() =>
        new(new FakeExplorerSession(new FakeStateConnection()), Interactive);

    private static ExplorerConfiguration Candidate(string endpoint) => new()
    {
        Endpoint = endpoint,
        AllowUnencryptedHttp2 = true,
    };

    /// <summary>A facade whose catalogue read answers at once, so the probe succeeds.</summary>
    private static ILatticeStateQuery Answering()
    {
        var query = Substitute.For<ILatticeStateQuery>();
        query.ListTreesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new TreeCatalogPage()));
        return query;
    }

    /// <summary>
    /// A facade whose catalogue read never answers until the call is cancelled, so
    /// the endpoint is up and connectable but the probe gets nothing back.
    /// </summary>
    private static ILatticeStateQuery Hanging()
    {
        var query = Substitute.For<ILatticeStateQuery>();
        query.ListTreesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>())
            .Returns(call => HangAsync(call.Arg<CancellationToken>()));
        return query;
    }

    // An async helper, not Task.Delay(...).ContinueWith(...): a continuation
    // without OnlyOnRanToCompletion runs after the cancellation and completes
    // successfully, which would swallow the cancellation the probe depends on.
    private static async Task<TreeCatalogPage> HangAsync(CancellationToken cancellationToken)
    {
        await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
        return new TreeCatalogPage();
    }
}
