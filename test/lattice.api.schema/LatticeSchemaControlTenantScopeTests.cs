using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.Schema;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Schema.Tests;

/// <summary>
/// Detectors for the <c>SchemaTenantScope</c> model (spec/schema): the facade
/// resolves the caller's tree once under its tenant, refuses an unattributed
/// caller and a confined tenant's escape into a reserved or malformed namespace
/// before the gate, authorizes the effective tree for
/// <see cref="LatticeOperation.SchemaAdmin"/> or <see cref="LatticeOperation.Read"/>,
/// and acts on exactly that tree. Driven with substitutes and a recording gate -
/// no cluster.
/// </summary>
[TestFixture]
public sealed class LatticeSchemaControlTenantScopeTests
{
    private const string Tree = "orders";
    private const string ComposedTree = "t/acme/orders";

    private static readonly TenantId Acme = TenantId.Parse("acme");

    private sealed class Harness
    {
        public required ILatticeSchemaAdmin Admin { get; init; }

        public required IGrainFactory Grains { get; init; }

        public required LatticeSchemaControl Control { get; init; }
    }

    private static Harness Create(ILatticeAccessGate gate, ITenantContextResolver resolver)
    {
        var admin = Substitute.For<ILatticeSchemaAdmin>();

        var transforms = new LatticeValueTransformRegistry([]);
        var registryGrain = Substitute.For<ILatticeSchemaRuntimeRegistryGrain>();
        registryGrain.GetAsync().Returns(new LatticeSchemaRuntimeRegistration());
        var grains = Substitute.For<IGrainFactory>();
        grains.GetGrain<ILatticeSchemaRuntimeRegistryGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(registryGrain);
        var runtime = new SchemaRuntimeRegistry(
            grains, new LatticeSchemaRegistryBuilder().Build(transforms), transforms, []);

        var control = new LatticeSchemaControl(
            admin,
            Substitute.For<ILatticeSchemaRemediationAdmin>(),
            Substitute.For<ILatticeSchemaComplianceAdmin>(),
            new SchemaAccessAuthorizer(gate),
            Options.Create(new LatticeApiSchemaOptions()),
            new ServiceCollection().AddSingleton(runtime).BuildServiceProvider(),
            resolver);

        return new Harness { Admin = admin, Grains = grains, Control = control };
    }

    private static LatticeSchemaPolicy JsonPolicy() => new(new[] { LatticeSchemaRule.Json() });

    private static string[] TreesPassedTo(object substitute, string method) =>
        substitute.ReceivedCalls()
            .Where(call => call.GetMethodInfo().Name == method)
            .Select(call => (string)call.GetArguments()[0]!)
            .ToArray();

    [Test]
    public async Task SetPolicyAsync_under_a_confined_tenant_authorizes_and_writes_the_composed_tree()
    {
        var gate = new LedgerGate();
        var h = Create(gate, new FixedTenantResolver(Acme));

        await h.Control.SetPolicyAsync(Tree, JsonPolicy());

        Assert.Multiple(() =>
        {
            Assert.That(gate.Requests, Is.EqualTo(new[] { (ComposedTree, LatticeOperation.SchemaAdmin) }),
                "The gate must be asked exactly once, for SchemaAdmin over the tenant-composed tree.");
            Assert.That(TreesPassedTo(h.Admin, nameof(ILatticeSchemaAdmin.SetPolicyAsync)), Is.EqualTo(new[] { ComposedTree }),
                "The admin must write the same composed tree the gate authorized, never the bare caller name.");
        });
    }

    [Test]
    public async Task GetPolicyAsync_under_a_confined_tenant_reads_the_composed_tree_under_read_authority()
    {
        var gate = new LedgerGate();
        var h = Create(gate, new FixedTenantResolver(Acme));

        await h.Control.GetPolicyAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(gate.Requests, Is.EqualTo(new[] { (ComposedTree, LatticeOperation.Read) }),
                "The gate must be asked exactly once, for Read over the tenant-composed tree.");
            Assert.That(TreesPassedTo(h.Admin, nameof(ILatticeSchemaAdmin.GetPolicyAsync)), Is.EqualTo(new[] { ComposedTree }),
                "The admin must read the same composed tree the gate authorized, never the bare caller name.");
        });
    }

    [Test]
    public async Task GetRegistryAsync_under_a_confined_tenant_reads_the_composed_trees_catalog()
    {
        var gate = new LedgerGate();
        var h = Create(gate, new FixedTenantResolver(Acme));

        await h.Control.GetRegistryAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(gate.Requests, Is.EqualTo(new[] { (ComposedTree, LatticeOperation.Read) }),
                "The gate must be asked exactly once, for Read over the tenant-composed tree.");
            Assert.That(TreesPassedTo(h.Grains, nameof(IGrainFactory.GetGrain)), Is.EqualTo(new[] { ComposedTree }),
                "The runtime catalog must be read from the composed tree the gate authorized, never the bare caller name.");
        });
    }

    [Test]
    public void A_confined_tenants_reserved_name_is_refused_before_the_gate_and_the_admin()
    {
        var gate = new LedgerGate();
        var h = Create(gate, new FixedTenantResolver(Acme));

        Assert.Multiple(() =>
        {
            Assert.That(async () => await h.Control.SetPolicyAsync("sys-schema-policy", JsonPolicy()),
                Throws.TypeOf<LatticeTenantAccessDeniedException>(),
                "A confined tenant naming a reserved sys- tree must be refused at resolution.");
            Assert.That(gate.Requests, Is.Empty, "The refusal must precede the gate.");
            Assert.That(h.Admin.ReceivedCalls(), Is.Empty, "A refused escape must never reach the admin.");
        });
    }

    [Test]
    public void A_confined_tenants_malformed_tenant_name_is_refused_before_the_gate_and_the_admin()
    {
        var gate = new LedgerGate();
        var h = Create(gate, new FixedTenantResolver(Acme));

        Assert.Multiple(() =>
        {
            Assert.That(async () => await h.Control.SetPolicyAsync("t/x", JsonPolicy()),
                Throws.TypeOf<LatticeTenantAccessDeniedException>(),
                "A confined tenant naming a malformed t/ id with no owner must be refused at resolution.");
            Assert.That(gate.Requests, Is.Empty, "The refusal must precede the gate.");
            Assert.That(h.Admin.ReceivedCalls(), Is.Empty, "A refused escape must never reach the admin.");
        });
    }

    [Test]
    public void An_unattributed_caller_is_refused_before_the_gate_and_the_admin()
    {
        var syncGate = new LedgerGate();
        var sync = Create(syncGate, new FixedTenantResolver(default));
        var asyncGate = new LedgerGate();
        var slow = Create(asyncGate, new FixedTenantResolver(default, synchronous: false));

        Assert.Multiple(() =>
        {
            Assert.That(async () => await sync.Control.SetPolicyAsync(Tree, JsonPolicy()),
                Throws.TypeOf<LatticeTenantAccessDeniedException>(),
                "A caller resolved synchronously to no tenant must be refused, not defaulted.");
            Assert.That(async () => await slow.Control.SetPolicyAsync(Tree, JsonPolicy()),
                Throws.TypeOf<LatticeTenantAccessDeniedException>(),
                "A caller resolved asynchronously to no tenant must be refused, not defaulted.");
            Assert.That(syncGate.Requests, Is.Empty, "The synchronous refusal must precede the gate.");
            Assert.That(asyncGate.Requests, Is.Empty, "The asynchronous refusal must precede the gate.");
            Assert.That(sync.Admin.ReceivedCalls(), Is.Empty, "An unattributed caller must never reach the admin (sync path).");
            Assert.That(slow.Admin.ReceivedCalls(), Is.Empty, "An unattributed caller must never reach the admin (async path).");
        });
    }

    [Test]
    public void Another_tenants_qualified_name_reaches_the_gate_unchanged_and_is_denied_without_its_grant()
    {
        const string Foreign = "t/other/orders";
        var gate = new LedgerGate(_ => false);
        var h = Create(gate, new FixedTenantResolver(Acme));

        Assert.Multiple(() =>
        {
            Assert.That(async () => await h.Control.SetPolicyAsync(Foreign, JsonPolicy()),
                Throws.TypeOf<LatticeAuthorizationDeniedException>(),
                "A crossing into another tenant without its grant must be denied by the gate.");
            Assert.That(gate.Requests, Is.EqualTo(new[] { (Foreign, LatticeOperation.SchemaAdmin) }),
                "A well-formed foreign id must reach the gate unchanged - neither refused early nor double-composed.");
            Assert.That(h.Admin.ReceivedCalls(), Is.Empty, "A denied crossing must never reach the admin.");
        });
    }

    [Test]
    public async Task A_schema_mutation_with_only_read_authority_is_denied_and_writes_nothing()
    {
        var gate = new LedgerGate(request => request.Operation == LatticeOperation.Read);
        var h = Create(gate, new FixedTenantResolver(Acme));

        Assert.That(async () => await h.Control.SetPolicyAsync(Tree, JsonPolicy()),
            Throws.TypeOf<LatticeAuthorizationDeniedException>(),
            "A mutation requires SchemaAdmin; Read authority alone must be denied.");
        Assert.That(TreesPassedTo(h.Admin, nameof(ILatticeSchemaAdmin.SetPolicyAsync)), Is.Empty,
            "A denied mutation must write nothing.");

        await h.Control.GetPolicyAsync(Tree);

        Assert.That(TreesPassedTo(h.Admin, nameof(ILatticeSchemaAdmin.GetPolicyAsync)), Is.EqualTo(new[] { ComposedTree }),
            "The same Read authority must still admit a read of the composed tree.");
    }

    [Test]
    public async Task The_compliance_scan_start_under_a_confined_tenant_authorizes_and_records_the_composed_tree()
    {
        var gate = new LedgerGate();
        var operations = new InMemoryOperationRunner();
        var compliance = Substitute.For<ILatticeSchemaComplianceAdmin>();
        compliance.ScanComplianceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => new LatticeSchemaComplianceReport
            {
                TreeId = call.ArgAt<string>(0),
                HasPolicy = false,
                CompliantCount = 0,
                NonCompliantCount = 0,
                ScannedCount = 0,
                RuleBreakdown = [],
            });
        var facade = new LatticeSchemaComplianceOperations(
            operations.Runner, compliance, new SchemaAccessAuthorizer(gate), new FixedTenantResolver(Acme));

        await facade.StartComplianceScanAsync(Tree, "scan-acme");
        await TestPoll.UntilAsync(
            () => Task.FromResult(compliance.ReceivedCalls().Any()), "the compliance scan to run");

        var record = operations.Record(Acme.Value!, "scan-acme");
        Assert.Multiple(() =>
        {
            Assert.That(gate.Requests, Is.EqualTo(new[] { (ComposedTree, LatticeOperation.Read) }),
                "The start must authorize Read over the tenant-composed tree exactly once.");
            Assert.That(record, Is.Not.Null, "The operation must be recorded under the caller's tenant.");
            Assert.That(record?.TreeIds, Is.EqualTo(new[] { ComposedTree }),
                "The recorded scope must be the composed tree the gate authorized.");
            Assert.That(TreesPassedTo(compliance, nameof(ILatticeSchemaComplianceAdmin.ScanComplianceAsync)),
                Is.EqualTo(new[] { ComposedTree }),
                "The scan must run over the composed tree the gate authorized.");
        });
    }

    /// <summary>
    /// An <see cref="ILatticeAccessGate"/> that records every (tree, operation)
    /// it is asked about and allows or denies by a predicate.
    /// </summary>
    private sealed class LedgerGate(Func<LatticeAccessRequest, bool>? allow = null) : ILatticeAccessGate
    {
        private readonly Func<LatticeAccessRequest, bool> _allow = allow ?? (_ => true);
        private readonly List<(string TreeId, LatticeOperation Operation)> _requests = [];

        public IReadOnlyList<(string TreeId, LatticeOperation Operation)> Requests
        {
            get
            {
                lock (_requests)
                {
                    return _requests.ToArray();
                }
            }
        }

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request, CancellationToken cancellationToken = default)
        {
            lock (_requests)
            {
                _requests.Add((request.TreeId, request.Operation));
            }

            return new ValueTask<LatticeAccessDecision>(
                _allow(request) ? LatticeAccessDecision.Allow() : LatticeAccessDecision.Deny("not granted"));
        }
    }

    /// <summary>
    /// Resolves a fixed tenant, synchronously or (when <paramref name="synchronous"/>
    /// is <c>false</c>) only through the asynchronous path.
    /// </summary>
    private sealed class FixedTenantResolver(TenantId tenant, bool synchronous = true) : ITenantContextResolver
    {
        public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default) => new(tenant);

        public bool TryResolveCurrent(out TenantId resolved)
        {
            resolved = synchronous ? tenant : default;
            return synchronous;
        }
    }
}
