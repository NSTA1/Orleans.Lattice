using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

public sealed partial class AppActivationEngineTests
{
    private static AppManifest BridgeManifest(AppVersion? version = null, params AppUiBridgeDeclaration[] bridge) =>
        UiTestManifests.WithUi(ActivationHarness.Manifest(version), bridge);

    private static async Task InstallWithBridgeConsentAsync(ActivationHarness harness, AppManifest manifest, AppUiBridgeRequest? consent)
    {
        harness.Source.Publish(manifest);
        var result = await harness.Registry.InstallAsync(AppRegistryTestData.Request(manifest.Identity.Version) with { BridgeConsent = consent });
        Assert.That(result.Succeeded, Is.True, result.Message);
    }

    [Test]
    public async Task Enable_refuses_a_manifest_whose_bridge_grants_were_never_consented()
    {
        var harness = new ActivationHarness();
        await InstallWithBridgeConsentAsync(harness, BridgeManifest(null, UiTestManifests.Bridge(AppUiBridgeOperations.DataRead)), consent: null);

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Succeeded, Is.False);
        Assert.That(outcome.Failure, Is.EqualTo(AppActivationFailure.BridgeConsentRequired));
        Assert.That(outcome.Diagnostics.Select(d => d.Code), Is.All.EqualTo("bridge-consent"));
        Assert.That(outcome.Diagnostics[0].Message, Does.Contain(AppUiBridgeOperations.DataRead));
        Assert.That(harness.OwnedRuleIds(), Is.Empty);
        Assert.That(harness.Trees.Created, Is.Empty);
    }

    [Test]
    public async Task Enable_activates_when_the_consented_bridge_covers_the_request()
    {
        var harness = new ActivationHarness();
        var manifest = BridgeManifest(null, UiTestManifests.Bridge(AppUiBridgeOperations.DataRead, "records"), UiTestManifests.Bridge(AppUiBridgeOperations.UiNotify));
        await InstallWithBridgeConsentAsync(harness, manifest, AppUiBridgeRequest.Create(
            [new AppUiBridgeGrant(AppUiBridgeOperations.DataRead), new AppUiBridgeGrant(AppUiBridgeOperations.UiNotify)]));

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Succeeded, Is.True, () => string.Join("; ", outcome.Diagnostics.Select(d => d.Message)));
        Assert.That(harness.OwnedRuleIds(), Has.Length.EqualTo(1));
    }

    [Test]
    public async Task Enable_of_a_manifest_without_a_bridge_needs_no_bridge_consent()
    {
        var harness = new ActivationHarness();
        await InstallWithBridgeConsentAsync(harness, BridgeManifest(), consent: null);

        var outcome = await harness.RunAsync(AppActivationOperation.Enable);

        Assert.That(outcome.Succeeded, Is.True, () => string.Join("; ", outcome.Diagnostics.Select(d => d.Message)));
    }

    [Test]
    public async Task Upgrade_that_adds_a_bridge_operation_fails_closed_until_it_is_re_consented()
    {
        var harness = new ActivationHarness();
        var readOnly = AppUiBridgeRequest.Create([new AppUiBridgeGrant(AppUiBridgeOperations.DataRead)]);
        await InstallWithBridgeConsentAsync(harness, BridgeManifest(null, UiTestManifests.Bridge(AppUiBridgeOperations.DataRead)), readOnly);
        Assert.That((await harness.RunAsync(AppActivationOperation.Enable)).Succeeded, Is.True);
        Assert.That(harness.OwnedRuleIds(), Is.Not.Empty);

        // The upgrade keeps the consented grants and asks for a write it was never granted.
        var upgraded = await harness.UpgradeAsync(BridgeManifest(
            ActivationHarness.V2,
            UiTestManifests.Bridge(AppUiBridgeOperations.DataRead),
            UiTestManifests.Bridge(AppUiBridgeOperations.DataWrite, "records")));
        Assert.That(upgraded.ConsentedBridge, Is.EqualTo(readOnly));

        var blocked = await harness.RunAsync(AppActivationOperation.Reconcile);
        Assert.That(blocked.Failure, Is.EqualTo(AppActivationFailure.BridgeConsentRequired));
        Assert.That(blocked.Diagnostics.Single().Message, Does.Contain(AppUiBridgeOperations.DataWrite).And.Contain("records"));
        Assert.That(harness.OwnedRuleIds(), Is.Empty, "a blocked activation withdraws the grants it left behind");

        var widened = AppUiBridgeRequest.Create(
            [new AppUiBridgeGrant(AppUiBridgeOperations.DataRead), new AppUiBridgeGrant(AppUiBridgeOperations.DataWrite, "records")]);
        var reconsented = await harness.Registry.UpgradeAsync(AppRegistryTestData.Request(ActivationHarness.V2) with
        {
            BridgeConsent = widened,
            ExpectedVersion = ActivationHarness.V2,
        });
        Assert.That(reconsented.Succeeded, Is.True, reconsented.Message);

        var restored = await harness.RunAsync(AppActivationOperation.Reconcile);
        Assert.That(restored.Succeeded, Is.True, () => string.Join("; ", restored.Diagnostics.Select(d => d.Message)));
        Assert.That(harness.OwnedRuleIds(), Is.Not.Empty);
    }

    [Test]
    public async Task Upgrade_that_removes_a_bridge_operation_needs_no_re_consent()
    {
        var harness = new ActivationHarness();
        await InstallWithBridgeConsentAsync(
            harness,
            BridgeManifest(null, UiTestManifests.Bridge(AppUiBridgeOperations.DataRead), UiTestManifests.Bridge(AppUiBridgeOperations.NavSync)),
            AppUiBridgeRequest.Create([new AppUiBridgeGrant(AppUiBridgeOperations.DataRead), new AppUiBridgeGrant(AppUiBridgeOperations.NavSync)]));
        Assert.That((await harness.RunAsync(AppActivationOperation.Enable)).Succeeded, Is.True);

        await harness.UpgradeAsync(BridgeManifest(ActivationHarness.V2, UiTestManifests.Bridge(AppUiBridgeOperations.DataRead)));

        Assert.That((await harness.RunAsync(AppActivationOperation.Reconcile)).Succeeded, Is.True);
    }
}
