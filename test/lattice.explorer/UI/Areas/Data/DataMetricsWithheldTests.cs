using Bunit;
using Grpc.Core;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Data;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// Issue #5419: a grant narrowed to part of a tree is refused that tree's
/// whole-tree measures. The Metrics tab says why rather than reporting a failure
/// it would invite the reader to retry, and the summary line omits the key count.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataMetricsWithheldTests : DataTestContext
{
    private static LatticeStateApiException GrantRefusal() =>
        new("Access to the state API was denied.", new RpcException(new Status(StatusCode.PermissionDenied, "denied")))
        {
            RequiresAuthentication = true,
            IsPermissionDenied = true,
        };

    private static LatticeStateApiException SignedOutRefusal() =>
        new("Authentication is required to access the state API.", new RpcException(new Status(StatusCode.Unauthenticated, "no credential")))
        {
            RequiresAuthentication = true,
        };

    [Test]
    public void A_refused_metrics_read_says_the_measures_need_the_whole_tree_and_offers_no_retry()
    {
        Client.WithTree("export-ledger", shards: 2);
        Client.Fault = call => call == nameof(ILatticeStateClient.GetMetricsSnapshotAsync) ? GrantRefusal() : null;

        var cut = RenderAt("data/export-ledger?tab=metrics");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Metrics are not available to you"));
            Assert.That(cut.Find("[data-lt-metrics-withheld]").TextContent, Does.Contain("read access to the whole tree"));
            Assert.That(cut.FindAll(".lt-empty button"), Is.Empty, "retrying cannot change a refusal");
            Assert.That(cut.Find("p.lt-data-meta").TextContent, Does.Not.Contain("live key"));
        });
    }

    [Test]
    public void A_signed_out_refusal_keeps_the_ordinary_failure_state()
    {
        Client.WithTree("export-ledger");
        Client.Fault = call => call == nameof(ILatticeStateClient.GetMetricsSnapshotAsync) ? SignedOutRefusal() : null;

        var cut = RenderAt("data/export-ledger?tab=metrics");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("The metrics did not load"));
            Assert.That(cut.FindAll("[data-lt-metrics-withheld]"), Is.Empty);
        });
    }

    [TestCase(StatusCode.PermissionDenied, true)]
    [TestCase(StatusCode.Unauthenticated, false)]
    [TestCase(StatusCode.Internal, false)]
    public void IsGrantRefusal_reads_only_a_refusal_of_a_signed_in_caller(StatusCode status, bool expected)
    {
        Assert.That(DataMetricsPanel.IsGrantRefusal(new RpcException(new Status(status, "x"))), Is.EqualTo(expected));
    }

    [Test]
    public void IsGrantRefusal_reads_an_in_process_gate_denial_and_a_wrapped_state_denial()
    {
        Assert.Multiple(() =>
        {
            Assert.That(DataMetricsPanel.IsGrantRefusal(new LatticeAuthorizationDeniedException("denied")), Is.True);
            Assert.That(DataMetricsPanel.IsGrantRefusal(GrantRefusal()), Is.True);
            Assert.That(DataMetricsPanel.IsGrantRefusal(SignedOutRefusal()), Is.False);
            Assert.That(DataMetricsPanel.IsGrantRefusal(new InvalidOperationException("x")), Is.False);
        });
    }
}
