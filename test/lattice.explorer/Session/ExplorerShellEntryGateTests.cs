using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Tests.Session;

/// <summary>
/// The remembered view is restored once per session, not once per page instance.
/// </summary>
/// <remarks>
/// The restore used to be guarded by a field on the page that performs it. The
/// router destroys and recreates that page on every navigation away and back, so
/// the guard reset each time and the rule became "every time you return to
/// <c>/</c>". Pressing Back out of an area therefore returned to the home address
/// and was immediately bounced back into the area, which made browser history
/// impossible to walk - the defect the journey suite reported as "Back and
/// forward do not walk the areas that were visited".
/// </remarks>
[TestFixture]
public sealed class ExplorerShellEntryGateTests
{
    [Test]
    public void The_first_claim_succeeds()
    {
        var gate = new ExplorerShellEntryGate();

        Assert.That(gate.TryClaimEntry(), Is.True);
    }

    [Test]
    public void A_second_claim_is_refused_so_a_later_home_address_is_taken_at_face_value()
    {
        var gate = new ExplorerShellEntryGate();
        gate.TryClaimEntry();

        Assert.That(
            gate.TryClaimEntry(),
            Is.False,
            "a page recreated by a later navigation must not get a second restore");
    }

    [Test]
    public void Repeated_claims_stay_refused()
    {
        var gate = new ExplorerShellEntryGate();
        gate.TryClaimEntry();

        Assert.That(new[] { gate.TryClaimEntry(), gate.TryClaimEntry() }, Is.All.False);
    }

    [Test]
    public void The_gate_is_registered_scoped_so_the_claim_belongs_to_the_session()
    {
        // A singleton would leak one session's claim into every other session, and a
        // transient would hand every page its own gate - which is the bug this type
        // exists to prevent, reintroduced through the container.
        var services = new ServiceCollection();
        services.AddExplorerSession();

        var descriptor = services.Single(d => d.ServiceType == typeof(IExplorerShellEntryGate));

        Assert.That(descriptor.Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
    }

    [Test]
    public void One_sessions_claim_does_not_disturb_another()
    {
        var services = new ServiceCollection();
        services.AddExplorerSession();
        using var provider = services.BuildServiceProvider();

        using var first = provider.CreateScope();
        using var second = provider.CreateScope();

        var firstGate = first.ServiceProvider.GetRequiredService<IExplorerShellEntryGate>();
        var secondGate = second.ServiceProvider.GetRequiredService<IExplorerShellEntryGate>();

        firstGate.TryClaimEntry();

        Assert.Multiple(() =>
        {
            Assert.That(firstGate.TryClaimEntry(), Is.False, "the first session has spent its claim");
            Assert.That(secondGate.TryClaimEntry(), Is.True, "a different session still has its own");
        });
    }
}
