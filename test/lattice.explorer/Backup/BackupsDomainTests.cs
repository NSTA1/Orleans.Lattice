using NSubstitute;
using Orleans.Lattice.Explorer.Backup;
using Orleans.Lattice.Explorer.Core.Catalog;

namespace Orleans.Lattice.Explorer.Tests.Backup;

/// <summary>
/// The Backups plugin's controlled domain model: the one contract the host
/// resolves for the plugin, and the tree projection it hands the capture
/// picker.
/// </summary>
[TestFixture]
public sealed class BackupsDomainTests
{
    [Test]
    public void The_domain_rejects_null_dependencies()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => new BackupsDomain(null!, Substitute.For<ICatalogReader>()),
                Throws.ArgumentNullException);
            Assert.That(
                () => new BackupsDomain(Substitute.For<IBackupCatalogReader>(), null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void The_domain_exposes_the_catalogue_reader_it_was_given()
    {
        var catalog = Substitute.For<IBackupCatalogReader>();

        var domain = new BackupsDomain(catalog, Substitute.For<ICatalogReader>());

        Assert.That(domain.Catalog, Is.SameAs(catalog));
    }

    [Test]
    public async Task LoadTreesAsync_projects_every_tree_onto_the_plugins_own_option()
    {
        var trees = Substitute.For<ICatalogReader>();
        trees.LoadAsync(CatalogKind.Trees, null, Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new CatalogPage
            {
                Items = new[]
                {
                    new CatalogItem { Id = "orders", Kind = CatalogKind.Trees },
                    new CatalogItem
                    {
                        Id = "orders-shadow",
                        Kind = CatalogKind.Trees,
                        RestoreShadowOfTreeId = "orders",
                    },
                },
            }));

        var domain = new BackupsDomain(Substitute.For<IBackupCatalogReader>(), trees);

        var options = await domain.LoadTreesAsync();

        Assert.Multiple(() =>
        {
            Assert.That(options.Select(o => o.Id), Is.EqualTo(new[] { "orders", "orders-shadow" }));
            Assert.That(options[0].IsRestoreShadow, Is.False);
            Assert.That(options[1].IsRestoreShadow, Is.True);
            Assert.That(options[1].RestoreShadowOfTreeId, Is.EqualTo("orders"));
        });
    }

    [Test]
    public async Task LoadTreesAsync_follows_the_continuation_cursor_to_the_end()
    {
        var trees = Substitute.For<ICatalogReader>();
        trees.LoadAsync(CatalogKind.Trees, null, Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new CatalogPage
            {
                Items = new[] { new CatalogItem { Id = "a", Kind = CatalogKind.Trees } },
                NextPageToken = "cursor",
            }));
        trees.LoadAsync(CatalogKind.Trees, "cursor", Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new CatalogPage
            {
                Items = new[] { new CatalogItem { Id = "b", Kind = CatalogKind.Trees } },
            }));

        var domain = new BackupsDomain(Substitute.For<IBackupCatalogReader>(), trees);

        Assert.That((await domain.LoadTreesAsync()).Select(o => o.Id), Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task LoadTreesAsync_keeps_what_it_gathered_when_discovery_fails_partway()
    {
        var trees = Substitute.For<ICatalogReader>();
        trees.LoadAsync(CatalogKind.Trees, null, Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new CatalogPage
            {
                Items = new[] { new CatalogItem { Id = "a", Kind = CatalogKind.Trees } },
                NextPageToken = "cursor",
            }));
        trees.LoadAsync(CatalogKind.Trees, "cursor", Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns<Task<CatalogPage>>(_ => throw new InvalidOperationException("no connection"));

        var domain = new BackupsDomain(Substitute.For<IBackupCatalogReader>(), trees);

        // Discovery is best-effort: the area still lists visible backups and the
        // operator can retry, rather than the whole panel failing to mount.
        Assert.That((await domain.LoadTreesAsync()).Select(o => o.Id), Is.EqualTo(new[] { "a" }));
    }

    [Test]
    public async Task LoadTreesAsync_returns_an_empty_list_when_discovery_fails_outright()
    {
        var trees = Substitute.For<ICatalogReader>();
        trees.LoadAsync(CatalogKind.Trees, null, Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns<Task<CatalogPage>>(_ => throw new InvalidOperationException("no connection"));

        var domain = new BackupsDomain(Substitute.For<IBackupCatalogReader>(), trees);

        Assert.That(await domain.LoadTreesAsync(), Is.Empty);
    }

    [Test]
    public void The_tree_option_reports_a_restore_shadow_from_its_logical_tree_marker()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new BackupTreeOption("orders", null).IsRestoreShadow, Is.False);
            Assert.That(new BackupTreeOption("orders-shadow", "orders").IsRestoreShadow, Is.True);
            Assert.That(
                new BackupTreeOption("orders", null),
                Is.EqualTo(new BackupTreeOption("orders", null)),
                "a readonly record struct compares by value, so the picker can diff options cheaply");
        });
    }
}
