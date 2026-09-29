using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>The Shell's <see cref="ILatticeAppsControl"/> transport adapter.</summary>
[TestFixture]
public sealed class ShellAppsControlTransportTests : ShellTransportAdapterContractTests<ILatticeAppsControl>
{
    private const string Service = "/orleans.lattice.api.apps/";

    private static readonly AppCapabilityCeilingDescriptor Ceiling = new();

    internal override IEnumerable<ShellTransportCall<ILatticeAppsControl>> Calls() =>
    [
        new("InstallAsync", Service + "Install", (f, ct) => f.InstallAsync(new AppInstallRequest { Slug = "crm", Version = "1.0.0", Ceiling = Ceiling }, ct)),
        new("EnableAsync", Service + "Enable", (f, ct) => f.EnableAsync("crm", ct)),
        new("DisableAsync", Service + "Disable", (f, ct) => f.DisableAsync("crm", ct)),
        new("UninstallAsync", Service + "Uninstall", (f, ct) => f.UninstallAsync("crm", ct)),
        new("ListAsync", Service + "List", (f, ct) => f.ListAsync(ct)),
        new("DescribeAsync", Service + "Describe", (f, ct) => f.DescribeAsync("crm", "1.0.0", ct)),
        new("GetConsentAsync", Service + "GetConsent", (f, ct) => f.GetConsentAsync("crm", ct)),
        new("UpdateConsentAsync", Service + "UpdateConsent", (f, ct) => f.UpdateConsentAsync(new AppConsentUpdate { Slug = "crm", Version = "1.0.0", Ceiling = Ceiling }, ct)),
        new("GetCapabilitiesAsync", Service + "GetCapabilities", (f, ct) => f.GetCapabilitiesAsync(ct)),
    ];

    [Test]
    public void An_unknown_app_maps_to_key_not_found()
    {
        using var circuit = new ShellTransportCircuit();
        var apps = circuit.Resolve<ILatticeAppsControl>();
        circuit.Peer.AnswerWith(Grpc.Core.StatusCode.NotFound, "The requested app or version was not found.");

        Assert.That(() => apps.EnableAsync("crm"), Throws.InstanceOf<KeyNotFoundException>());
    }

    [Test]
    public void Argument_guards_run_before_any_call()
    {
        using var circuit = new ShellTransportCircuit();
        var apps = circuit.Resolve<ILatticeAppsControl>();

        Assert.Multiple(() =>
        {
            Assert.That(() => apps.InstallAsync(null!), Throws.ArgumentNullException);
            Assert.That(() => apps.EnableAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => apps.DisableAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => apps.UninstallAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => apps.DescribeAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => apps.GetConsentAsync(string.Empty), Throws.ArgumentException);
            Assert.That(() => apps.UpdateConsentAsync(null!), Throws.ArgumentNullException);
            Assert.That(circuit.Peer.Requests, Is.Empty);
        });
    }
}
