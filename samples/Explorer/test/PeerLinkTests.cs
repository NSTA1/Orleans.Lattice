using Microsoft.AspNetCore.Http;

namespace Orleans.Lattice.Samples.Explorer.Tests;

[TestFixture]
public sealed class PeerLinkTests
{
    [TestCase("/orleans.lattice.replication.LatticeReplication/Push", true)]
    [TestCase("/orleans.lattice.replication.LatticeRemoteSnapshot/Stream", true)]
    [TestCase("/orleans.lattice.replication.LatticeSaga/Prepare", true)]
    [TestCase("/orleans.lattice.api.replication/GetReplicationConfig", false)]
    [TestCase("/orleans.lattice.api.replication.status/GetPeerStatus", false)]
    [TestCase("/", false)]
    [TestCase("", false)]
    public void Only_cross_region_replication_services_are_replication_paths(string path, bool expected) =>
        Assert.That(PeerLink.IsReplicationPath(new PathString(path)), Is.EqualTo(expected));

    [Test]
    public void A_new_link_is_open() =>
        Assert.That(new PeerLink().IsPaused, Is.False);

    [Test]
    public async Task An_open_link_passes_replication_calls_on()
    {
        var (context, reached) = await InvokeAsync(new PeerLink(), "/orleans.lattice.replication.LatticeReplication/Push");

        Assert.That(reached, Is.True);
        Assert.That(context.Response.StatusCode, Is.EqualTo(StatusCodes.Status200OK));
    }

    [Test]
    public async Task A_paused_link_refuses_replication_calls_as_unavailable()
    {
        var link = new PeerLink();
        link.Pause();

        var (context, reached) = await InvokeAsync(link, "/orleans.lattice.replication.LatticeReplication/Push");

        Assert.That(reached, Is.False);
        Assert.That(context.Response.StatusCode, Is.EqualTo(StatusCodes.Status503ServiceUnavailable));
    }

    [Test]
    public async Task A_paused_link_passes_every_other_call_on()
    {
        var link = new PeerLink();
        link.Pause();

        var (_, reached) = await InvokeAsync(link, "/orleans.lattice.api.replication.status/GetPeerStatus");

        Assert.That(reached, Is.True);
    }

    [Test]
    public async Task A_resumed_link_passes_replication_calls_on_again()
    {
        var link = new PeerLink();
        link.Pause();
        link.Resume();

        var (_, reached) = await InvokeAsync(link, "/orleans.lattice.replication.LatticeSaga/Prepare");

        Assert.That(reached, Is.True);
    }

    [Test]
    public void Toggle_flips_the_state_and_reports_it()
    {
        var link = new PeerLink();
        var raised = new List<bool>();
        link.Changed += raised.Add;

        Assert.That(link.Toggle(), Is.True);
        Assert.That(link.IsPaused, Is.True);
        Assert.That(link.Toggle(), Is.False);
        Assert.That(link.IsPaused, Is.False);
        Assert.That(raised, Is.EqualTo(new[] { true, false }));
    }

    [Test]
    public void Pause_and_resume_report_only_real_changes()
    {
        var link = new PeerLink();
        var raised = new List<bool>();
        link.Changed += raised.Add;

        Assert.That(link.Resume(), Is.False, "resuming an open link changes nothing");
        Assert.That(link.Pause(), Is.True);
        Assert.That(link.Pause(), Is.False, "pausing a paused link changes nothing");
        Assert.That(link.Resume(), Is.True);
        Assert.That(raised, Is.EqualTo(new[] { true, false }));
    }

    [Test]
    public void Invoke_rejects_null_arguments()
    {
        var link = new PeerLink();

        Assert.That(() => link.InvokeAsync(null!, _ => Task.CompletedTask), Throws.ArgumentNullException);
        Assert.That(() => link.InvokeAsync(new DefaultHttpContext(), null!), Throws.ArgumentNullException);
    }

    private static async Task<(HttpContext Context, bool Reached)> InvokeAsync(PeerLink link, string path)
    {
        var context = new DefaultHttpContext();
        context.Request.Path = path;
        var reached = false;
        await link.InvokeAsync(context, _ =>
        {
            reached = true;
            return Task.CompletedTask;
        });
        return (context, reached);
    }
}
