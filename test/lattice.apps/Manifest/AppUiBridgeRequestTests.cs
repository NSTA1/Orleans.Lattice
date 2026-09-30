using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public sealed class AppUiBridgeRequestTests
{
    private static AppUiBridgeGrant G(string operation, string? tree = null) => new(operation, tree);

    private static AppUiBridgeRequest R(params AppUiBridgeGrant[] grants) => AppUiBridgeRequest.Create(grants);

    [Test]
    public void Empty_grants_nothing()
    {
        Assert.That(AppUiBridgeRequest.Empty.IsEmpty, Is.True);
        Assert.That(AppUiBridgeRequest.Empty.Grants, Is.Empty);
        Assert.That(R(), Is.SameAs(AppUiBridgeRequest.Empty));
        Assert.That(AppUiBridgeRequest.Empty.Covers(G("context.read")), Is.False);
    }

    [Test]
    public void Grant_exposes_its_operation_and_tree()
    {
        var grant = new AppUiBridgeGrant("data.read", "records");
        Assert.That(grant.Operation, Is.EqualTo("data.read"));
        Assert.That(grant.Tree, Is.EqualTo("records"));
        Assert.That(new AppUiBridgeGrant("nav.sync").Tree, Is.Null);
        Assert.That(grant, Is.EqualTo(G("data.read", "records")));
    }

    [Test]
    public void Create_sorts_deduplicates_and_folds_per_tree_grants_into_every_tree_grants()
    {
        var request = R(G("ui.notify"), G("data.read", "b"), G("data.read", "a"), G("data.read"), G("data.write", "b"),
            G("data.write", "a"), G("ui.notify"), G("context.read"));
        Assert.That(request.Grants, Is.EqualTo(new[]
        {
            G("context.read"), G("data.read"), G("data.write", "a"), G("data.write", "b"), G("ui.notify"),
        }));
        Assert.That(request.IsEmpty, Is.False);
    }

    [Test]
    public void Create_rejects_unknown_operations_and_misplaced_or_invalid_trees()
    {
        Assert.Throws<ArgumentNullException>(() => AppUiBridgeRequest.Create(null!));
        Assert.Throws<ArgumentException>(() => R(G("data.admin")));
        Assert.Throws<ArgumentException>(() => R(G(null!)));
        Assert.Throws<ArgumentException>(() => R(G("context.read", "records")));
        Assert.Throws<ArgumentException>(() => R(G("data.read", "")));
        Assert.Throws<ArgumentException>(() => R(G("data.read", "a/b")));
        Assert.Throws<ArgumentException>(() => R(G("data.read", "*")));
    }

    [Test]
    public void Equality_is_by_normalised_grants()
    {
        var a = R(G("data.read", "a"), G("context.read"), G("data.read"));
        var b = R(G("context.read"), G("data.read"));
        Assert.That(a, Is.EqualTo(b));
        Assert.That(a.Equals((object)b), Is.True);
        Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        Assert.That(a.Equals(a), Is.True);
        Assert.That(a.Equals((object)"context.read"), Is.False);
        Assert.That(a, Is.Not.EqualTo(R(G("context.read"), G("data.read", "a"))));
        Assert.That(AppUiBridgeRequest.Empty.GetHashCode(), Is.EqualTo(R().GetHashCode()));
        Assert.That(a.Equals((AppUiBridgeRequest?)null), Is.False);
    }

    [Test]
    public void Covers_honours_exact_grants_and_every_tree_grants()
    {
        var request = R(G("context.read"), G("data.read"), G("data.write", "a"));
        Assert.That(request.Covers(G("context.read")), Is.True);
        Assert.That(request.Covers(G("data.read")), Is.True);
        Assert.That(request.Covers(G("data.read", "anything")), Is.True);
        Assert.That(request.Covers(G("data.write", "a")), Is.True);
        Assert.That(request.Covers(G("data.write", "b")), Is.False);
        Assert.That(request.Covers(G("data.write")), Is.False, "a per-tree grant does not cover every tree");
        Assert.That(request.Covers(G("data.delete", "a")), Is.False);
        Assert.That(request.Covers(G("unknown")), Is.False);
    }

    [Test]
    public void AddedRelativeTo_reports_new_operations_and_widened_scopes_only()
    {
        var consented = R(G("context.read"), G("data.read", "a"), G("data.write"));

        Assert.That(R(G("context.read"), G("data.read", "a")).AddedRelativeTo(consented).IsEmpty, Is.True, "an identical or narrower request adds nothing");
        Assert.That(R(G("data.write", "x")).AddedRelativeTo(consented).IsEmpty, Is.True, "an every-tree consent covers any one tree");
        Assert.That(AppUiBridgeRequest.Empty.AddedRelativeTo(consented), Is.SameAs(AppUiBridgeRequest.Empty), "removing everything never needs consent");

        var added = R(G("context.read"), G("context.user"), G("data.read"), G("data.delete", "a"), G("data.write")).AddedRelativeTo(consented);
        Assert.That(added.Grants, Is.EqualTo(new[] { G("context.user"), G("data.delete", "a"), G("data.read") }));
        Assert.That(R(G("data.read", "b")).AddedRelativeTo(consented).Grants, Is.EqualTo(new[] { G("data.read", "b") }));
        Assert.That(R(G("nav.sync")).AddedRelativeTo(AppUiBridgeRequest.Empty), Is.EqualTo(R(G("nav.sync"))));
        Assert.That(consented.AddedRelativeTo(consented), Is.SameAs(AppUiBridgeRequest.Empty));
        Assert.Throws<ArgumentNullException>(() => consented.AddedRelativeTo(null!));
    }

    [Test]
    public void FromManifest_expands_the_bridge_section()
    {
        var manifest = AppManifestTests.UiManifest;
        Assert.That(AppUiBridgeRequest.FromManifest(manifest).Grants, Is.EqualTo(new[]
        {
            G("context.read"), G("data.read"), G("data.write", "records"),
        }));
        Assert.That(AppUiBridgeRequest.FromManifest(manifest with { Ui = null }), Is.SameAs(AppUiBridgeRequest.Empty));
        Assert.That(AppUiBridgeRequest.FromManifest(manifest with { Ui = manifest.Ui! with { Bridge = null } }), Is.SameAs(AppUiBridgeRequest.Empty));
        Assert.That(AppUiBridgeRequest.FromManifest(manifest with { Ui = manifest.Ui! with { Bridge = [] } }), Is.SameAs(AppUiBridgeRequest.Empty));
        Assert.Throws<ArgumentNullException>(() => AppUiBridgeRequest.FromManifest(null!));
        Assert.Throws<ArgumentException>(() => AppUiBridgeRequest.FromManifest(manifest with { Ui = manifest.Ui! with { Bridge = [null!] } }));
        Assert.Throws<ArgumentException>(() => AppUiBridgeRequest.FromManifest(
            manifest with { Ui = manifest.Ui! with { Bridge = [new() { Operation = "data.admin" }] } }));
    }

    [Test]
    public void Upgrade_that_adds_an_operation_is_detected_from_manifests()
    {
        var v1 = AppManifestTests.UiManifest;
        var v2 = v1 with { Ui = v1.Ui! with { Bridge = [.. v1.Ui.Bridge!, new() { Operation = "ui.notify" }] } };
        Assert.That(AppUiBridgeRequest.FromManifest(v2).AddedRelativeTo(AppUiBridgeRequest.FromManifest(v1)).Grants,
            Is.EqualTo(new[] { G("ui.notify") }));
        Assert.That(AppUiBridgeRequest.FromManifest(v1).AddedRelativeTo(AppUiBridgeRequest.FromManifest(v2)).IsEmpty, Is.True);
    }

    [Test]
    public void Orleans_roundtrip_preserves_the_request()
    {
        using var services = new ServiceCollection()
            .AddSerializer(builder => builder.AddAssembly(typeof(AppManifest).Assembly))
            .BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer>();
        var request = R(G("context.user"), G("data.read", "a"), G("data.delete"));
        var copy = serializer.Deserialize<AppUiBridgeRequest>(serializer.SerializeToArray(request));
        Assert.That(copy, Is.EqualTo(request));
        Assert.That(copy.Covers(G("data.delete", "x")), Is.True);
        Assert.That(serializer.Deserialize<AppUiBridgeRequest>(serializer.SerializeToArray(AppUiBridgeRequest.Empty)).IsEmpty, Is.True);
        var copier = services.GetRequiredService<DeepCopier>();
        Assert.That(copier.Copy(request), Is.EqualTo(request));
    }
}
