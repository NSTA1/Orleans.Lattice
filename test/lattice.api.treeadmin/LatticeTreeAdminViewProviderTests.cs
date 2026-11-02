using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Api.TreeAdmin.Tests;

/// <summary>
/// <see cref="LatticeTreeAdmin.ListViewProvidersAsync"/> (#5369): the listing an
/// operator console chooses a view provider from. It shares the create verb's
/// boundary - whole-tree admin over the source, refused fail-closed before anything
/// is disclosed - and lists the host's providers with their descriptions while
/// withholding the library's own predicate provider. Driven with substitutes and a
/// per-tree access gate; no cluster.
/// </summary>
[TestFixture]
public sealed class LatticeTreeAdminViewProviderTests
{
    private const string Owned = "orders";

    private sealed class PerTreeGate(params string[] allowedTreeIds) : ILatticeAccessGate
    {
        private readonly HashSet<string> _allowed = new(allowedTreeIds, StringComparer.Ordinal);

        public List<(string TreeId, LatticeOperation Operation)> Requested { get; } = [];

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
        {
            Requested.Add((request.TreeId, request.Operation));
            return new(_allowed.Contains(request.TreeId)
                ? LatticeAccessDecision.Allow()
                : LatticeAccessDecision.Deny($"no grant on '{request.TreeId}'"));
        }
    }

    private static LatticeViewDefinition Define(LatticeRuntimeViewProjectionContext context) =>
        new(context.ViewName, new PredicateLatticeViewProjection());

    private static RuntimeViewProjectionProviderCatalog Providers() => new(
    [
        new RuntimeViewProjectionProviderRegistration(PredicateRuntimeViewProjectionCodec.ProviderKey, (_, context) => Define(context)),
        new RuntimeViewProjectionProviderRegistration("raw.v1", (_, context) => Define(context)),
        new RuntimeViewProjectionProviderRegistration(
            "by-status.v1",
            (_, context) => Define(context),
            new LatticeRuntimeViewProviderDescription(
                "Orders grouped by status",
                [
                    new LatticeRuntimeViewProviderField("status", "Status") { Required = true, Description = "The status to keep" },
                    new LatticeRuntimeViewProviderField("limit", "Limit") { Kind = LatticeRuntimeViewProviderFieldKind.Number },
                    new LatticeRuntimeViewProviderField("open", "Open only") { Kind = LatticeRuntimeViewProviderFieldKind.Boolean },
                ])),
    ]);

    private static LatticeTreeAdmin Create(
        ILatticeAccessGate gate,
        bool views = true,
        RuntimeViewProjectionProviderCatalog? providers = null)
        => new(
            Substitute.For<ILatticeSchemaControl>(),
            Substitute.For<IGrainFactory>(),
            new TreeAdminAccessAuthorizer(gate),
            Options.Create(new LatticeApiTreeAdminOptions()),
            new NullTenantContextResolver(),
            viewFactory: views ? Substitute.For<ILatticeViewFactory>() : null,
            viewProviders: providers);

    [Test]
    public async Task ListViewProvidersAsync_lists_host_providers_by_key_with_their_descriptions()
    {
        var gate = new PerTreeGate(Owned);
        var facade = Create(gate, providers: Providers());

        var catalog = await facade.ListViewProvidersAsync(Owned);

        Assert.Multiple(() =>
        {
            Assert.That(catalog.Providers.Select(provider => provider.ProviderKey), Is.EqualTo(new[] { "by-status.v1", "raw.v1" }));
            var described = catalog.Providers[0];
            Assert.That(described.Description, Is.EqualTo("Orders grouped by status"));
            Assert.That(described.PayloadFields.Select(field => (field.Name, field.Label, field.Required, field.Kind)), Is.EqualTo(new[]
            {
                ("status", "Status", true, TreeViewProviderFieldKind.Text),
                ("limit", "Limit", false, TreeViewProviderFieldKind.Number),
                ("open", "Open only", false, TreeViewProviderFieldKind.Boolean),
            }));
            Assert.That(described.PayloadFields[0].Description, Is.EqualTo("The status to keep"));
            Assert.That(catalog.Providers[1].Description, Is.Null);
            Assert.That(catalog.Providers[1].PayloadFields, Is.Empty);
            Assert.That(gate.Requested, Is.EqualTo(new[] { (Owned, LatticeOperation.Admin) }));
        });
    }

    [Test]
    public void ListViewProvidersAsync_refuses_a_caller_without_admin_over_the_source()
    {
        var gate = new PerTreeGate();
        var facade = Create(gate, providers: Providers());

        Assert.That(
            async () => await facade.ListViewProvidersAsync(Owned),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
        Assert.That(gate.Requested, Is.EqualTo(new[] { (Owned, LatticeOperation.Admin) }));
    }

    [Test]
    public async Task ListViewProvidersAsync_is_empty_when_the_host_registered_no_provider()
    {
        var facade = Create(new PerTreeGate(Owned));

        var catalog = await facade.ListViewProvidersAsync(Owned);

        Assert.That(catalog.Providers, Is.Empty);
    }

    [Test]
    public void ListViewProvidersAsync_refuses_when_views_are_not_enabled()
    {
        var gate = new PerTreeGate(Owned);
        var facade = Create(gate, views: false);

        Assert.That(async () => await facade.ListViewProvidersAsync(Owned), Throws.InvalidOperationException);
        Assert.That(gate.Requested, Is.Empty, "nothing is authorized or disclosed on a cluster without views");
    }

    [TestCase("view-by-status")]
    [TestCase("_lattice_registry")]
    public void ListViewProvidersAsync_refuses_a_view_or_reserved_source_before_authorizing(string source)
    {
        var gate = new PerTreeGate(source);
        var facade = Create(gate, providers: Providers());

        Assert.That(async () => await facade.ListViewProvidersAsync(source), Throws.ArgumentException);
        Assert.That(gate.Requested, Is.Empty);
    }

    [Test]
    public void ListViewProvidersAsync_rejects_an_empty_source()
    {
        var facade = Create(new PerTreeGate(Owned), providers: Providers());

        Assert.That(async () => await facade.ListViewProvidersAsync(string.Empty), Throws.ArgumentException);
    }
}
