using System.Reflection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Core.Vocabulary;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// The shared conformance guard for the Explorer's four-state plugin access
/// contract (issue #1854).
/// <para>
/// Before it existed, three plugin gates produced three different readings of
/// the same contract, measured live across three identities: two told an
/// <em>anonymous</em> visitor that a surface "is not available for your account"
/// where the honest answer was "sign in", and one reported
/// <see cref="ExplorerPluginAccessState.Allowed"/> to an identity holding no
/// grant at all, inviting it into a surface the server would refuse. A display
/// policy cannot rescue a state that was computed wrongly, so this fixture pins
/// the computation.
/// </para>
/// <para>
/// <b>Gates are discovered by reflection, never listed.</b> Every concrete
/// <see cref="IExplorerPluginAccessGate"/> in every loaded
/// <c>Orleans.Lattice.Explorer.*</c> assembly is enumerated, so a plugin added
/// later is covered by these assertions without anyone editing this file -
/// <see cref="A_gate_that_does_not_participate_in_the_contract_is_reported_without_editing_the_guard"/>
/// proves that claim against a non-conforming gate declared below.
/// </para>
/// <para>
/// <b>What is asserted, and why it is complete.</b> The structural guard proves
/// every gate resolves through <see cref="ExplorerPluginAccessContract"/> and
/// cannot re-declare the mapping, so the state a gate produces is that one
/// function of its facts. The behavioural guards then drive the real gate
/// instances for the two cells a harness can supply uniformly - anonymous, and
/// authenticated-without-a-grant - and the matrix guard drives the resolution
/// for every identity against each gate's own declared remedy, which covers the
/// remaining cells including <see cref="ExplorerPluginAccessState.Allowed"/>.
/// </para>
/// <para>
/// The identity matrix mirrors <c>reference-architecture/local-dev</c>'s
/// <c>identities.json</c>, modelled as doubles because this is a unit fixture:
/// standing up the Docker harness to answer a pure decision question would buy
/// nothing and cost a cluster.
/// </para>
/// <para>
/// This gating is advisory. The server is the sole enforcement point, so none of
/// these assertions is a security claim - they are honesty and usability claims.
/// </para>
/// </summary>
[TestFixture]
public sealed class ExplorerPluginAccessGateConformanceTests
{
    /// <summary>
    /// The identities the local-dev harness defines, with the cluster grants
    /// each one's group holds. Modelled by the permission names a gate's
    /// <see cref="ExplorerPluginAccessGate.Remedy"/> can name, so the matrix
    /// stays meaningful for a plugin added later without being edited.
    /// </summary>
    private static readonly TestIdentity[] IdentityMatrix =
    [
        new("anonymous", IsAuthenticated: false, []),
        new("data-reader", IsAuthenticated: true, ["Read", "RangeRead"]),
        new(
            "region-operator",
            IsAuthenticated: true,
            [
                "Read", "Write", "Delete", "RangeRead", "RangeDelete", "CrdtApply", "AtomicWrite",
                "BulkLoad", "Admin",
            ]),
        new("auditor", IsAuthenticated: true, ["Telemetry"]),
        new(
            "platform-admin",
            IsAuthenticated: true,
            ["Read", "Write", "Delete", "RangeRead", "Admin", "Backup", "Telemetry", "Tenant admin"]),
    ];

    /// <summary>
    /// Every concrete plugin access gate the Explorer ships, discovered rather
    /// than listed.
    /// </summary>
    private static IEnumerable<Type> DiscoveredGates => GateTypes(ExplorerAssemblies());

    [Test]
    public void The_discovery_sweep_finds_the_gates_it_is_meant_to_guard()
    {
        var gates = DiscoveredGates.ToArray();

        // Without this the whole fixture would pass vacuously if the assembly
        // sweep ever stopped reaching the plugin packages.
        Assert.That(
            gates,
            Has.Length.GreaterThanOrEqualTo(6),
            "the reflection sweep must reach the Explorer's plugin packages; found: "
            + Describe(gates.Select(g => g.FullName ?? g.Name)));
    }

    [Test]
    public void Every_plugin_access_gate_resolves_through_the_shared_contract()
    {
        var violations = DiscoveredGates.SelectMany(ContractParticipationViolations).ToArray();

        Assert.That(
            violations,
            Is.Empty,
            "Every plugin access gate must derive from ExplorerPluginAccessGate and leave "
            + "ProbeAsync alone, so the four states are resolved once by "
            + "ExplorerPluginAccessContract instead of each plugin re-deriving the ordering. "
            + "That is the defect issue #1854 fixes: hand-rolled mappings disagreed with each "
            + "other. Report the facts your probe observed from EvaluateAsync and let the "
            + "contract decide the state."
            + Environment.NewLine
            + Describe(violations));
    }

    [Test]
    public void Every_plugin_access_gate_declares_a_remedy_naming_a_permission_and_an_audience()
    {
        var violations = new List<string>();

        foreach (var gate in DiscoveredGates)
        {
            if (Construct(gate, authenticated: true) is not ExplorerPluginAccessGate instance)
            {
                violations.Add($"{gate.Name}: could not be constructed from substituted dependencies");
                continue;
            }

            if (!instance.Remedy.IsSpecified)
            {
                violations.Add($"{gate.Name}: Remedy names no permission and/or no audience");
            }
        }

        Assert.That(
            violations,
            Is.Empty,
            "A denial must state a remedy. \"X is not available for your account\" tells the "
            + "reader nothing they can act on: which permission is missing, and who issues it. "
            + "Declare a cached ExplorerAccessRemedy.Requiring(permission, audience) on the gate "
            + "so the shell can render an actionable denial."
            + Environment.NewLine
            + Describe(violations));
    }

    [Test]
    public void Every_plugin_access_gate_names_a_declared_audience()
    {
        // One concept, one name (an acceptance criterion of #1853). The audience
        // was a literal on each gate, so the gates drifted from the copy layer
        // and the console said "ask a platform administrator" in the rail and
        // "ask an operator" in the panel, for the same grant in the same
        // session.
        //
        // The invariant is that an audience is DECLARED in the vocabulary, not
        // that every gate names the same one: a self-service tenant surface is
        // administered by the tenant's own administrator, who is genuinely not
        // the platform operator. Flattening those two would replace a wording
        // inconsistency with a wrong instruction.
        var declared = new[]
        {
            ExplorerVocabulary.GrantAudience,
            ExplorerVocabulary.TenantGrantAudience,
        };

        var violations = new List<string>();

        foreach (var gate in DiscoveredGates)
        {
            if (Construct(gate, authenticated: true) is not ExplorerPluginAccessGate instance)
            {
                violations.Add($"{gate.Name}: could not be constructed from substituted dependencies");
                continue;
            }

            var audience = instance.Remedy.Audience;
            if (!declared.Contains(audience, StringComparer.Ordinal))
            {
                violations.Add($"{gate.Name}: audience \"{audience}\" is not declared in ExplorerVocabulary");
            }
        }

        Assert.That(
            violations,
            Is.Empty,
            "A gate must name its audience with a term declared in ExplorerVocabulary rather than "
            + "its own literal, so the rail and the panel cannot describe the same remedy in two "
            + "registers. If a gate genuinely addresses a new audience, declare it there first."
            + Environment.NewLine
            + Describe(violations));
    }

    [Test]
    public void Every_plugin_access_gate_reads_the_caller_credential_from_a_real_sign_in_seam()
    {
        var violations = DiscoveredGates
            .Where(gate => !Constructors(gate)
                .Any(c => c.GetParameters().Any(p => p.ParameterType == typeof(IExplorerAuthSession))))
            .Select(gate => $"{gate.Name}: no constructor accepts IExplorerAuthSession")
            .ToArray();

        Assert.That(
            violations,
            Is.Empty,
            "A gate decides between Denied and AuthenticationRequired from whether the caller "
            + "presented a credential, so it needs a real source for that fact. A gate that "
            + "hard-codes IsCallerAuthenticated to true turns every anonymous refusal back into "
            + "the denial this contract exists to prevent."
            + Environment.NewLine
            + Describe(violations));
    }

    [Test]
    public void An_anonymous_caller_is_never_denied_by_any_plugin_access_gate()
    {
        var violations = new List<string>();

        foreach (var gate in DiscoveredGates)
        {
            var resolved = Probe(gate, authenticated: false, out var fault);
            if (fault is not null)
            {
                violations.Add($"{gate.Name}: probe threw {fault}");
                continue;
            }

            if (resolved == ExplorerPluginAccessState.Denied)
            {
                violations.Add($"{gate.Name}: resolved Denied for an anonymous caller");
            }
        }

        Assert.That(
            violations,
            Is.Empty,
            "An anonymous visitor has no account to be refused for, so telling them a surface "
            + "is not available for their account states something untrue and hides the one "
            + "action that would help. Measured before this guard existed: Tenants and My Tenant "
            + "both answered Denied while signed out. An unauthenticated caller must resolve to "
            + "AuthenticationRequired (or Unavailable, when the cluster does not serve the "
            + "capability at all)."
            + Environment.NewLine
            + Describe(violations));
    }

    [Test]
    public void No_plugin_access_gate_allows_a_caller_that_holds_no_grant()
    {
        var violations = new List<string>();

        foreach (var gate in DiscoveredGates)
        {
            var resolved = Probe(gate, authenticated: true, out var fault);
            if (fault is not null)
            {
                violations.Add($"{gate.Name}: probe threw {fault}");
                continue;
            }

            if (resolved == ExplorerPluginAccessState.Allowed)
            {
                violations.Add($"{gate.Name}: resolved Allowed for a caller shown to hold no grant");
            }
        }

        Assert.That(
            violations,
            Is.Empty,
            "A gate must not report Allowed for an operation the caller demonstrably cannot "
            + "perform: the user is invited into the surface and meets a server-side denial "
            + "inside it, which is strictly worse than an honest disabled entry. Measured before "
            + "this guard existed: Backups rendered enabled for data-reader, an identity holding "
            + "only cluster Read and RangeRead and no backup grant whatsoever. \"The probe call "
            + "did not fail\" is not a grant - read a capability flag, not a status code."
            + Environment.NewLine
            + Describe(violations));
    }

    [Test]
    public void Every_plugin_access_gate_resolves_all_four_states_across_the_identity_matrix()
    {
        var violations = new List<string>();
        var cells = 0;

        foreach (var gate in DiscoveredGates)
        {
            if (Construct(gate, authenticated: true) is not ExplorerPluginAccessGate instance)
            {
                violations.Add($"{gate.Name}: could not be constructed from substituted dependencies");
                continue;
            }

            var remedy = instance.Remedy;

            foreach (var identity in IdentityMatrix)
            {
                cells += 4;

                // Unavailable: the cluster does not serve the capability, so no
                // credential and no grant can change the answer - it outranks
                // every caller-facing rule, for every identity.
                Check(
                    violations,
                    gate,
                    identity,
                    "capability absent",
                    ExplorerPluginAccessFacts.CapabilityAbsent(),
                    remedy,
                    ExplorerPluginAccessState.Unavailable);

                // Allowed: the caller demonstrably holds the grant. Driven
                // explicitly for every gate so the state is covered even for a
                // plugin whose permission no modelled identity happens to hold.
                Check(
                    violations,
                    gate,
                    identity,
                    "holds the grant",
                    ExplorerPluginAccessFacts.Granted,
                    remedy,
                    ExplorerPluginAccessState.Allowed);

                // Denied vs AuthenticationRequired: the same withheld grant must
                // read differently for an anonymous caller than for a signed-in
                // one. This is the cell the three gates disagreed on.
                var withheld = ExplorerPluginAccessContract.Resolve(
                    ExplorerPluginAccessFacts.Withheld,
                    remedy,
                    identity.IsAuthenticated);

                var expected = identity.IsAuthenticated
                    ? ExplorerPluginAccessState.Denied
                    : ExplorerPluginAccessState.AuthenticationRequired;

                if (withheld.State != expected)
                {
                    violations.Add(
                        $"{gate.Name} / {identity.Name} / grant withheld: expected {expected}, got {withheld.State}");
                }

                if (identity.IsAuthenticated && !withheld.Remedy.IsSpecified)
                {
                    violations.Add(
                        $"{gate.Name} / {identity.Name} / grant withheld: the denial carries no remedy");
                }

                // Finally the identity's real grants, so the matrix is exercised
                // with differentiated permissions rather than uniform ones.
                var holds = remedy.Permission is { } permission && identity.Holds(permission);
                Check(
                    violations,
                    gate,
                    identity,
                    "modelled grants",
                    holds ? ExplorerPluginAccessFacts.Granted : ExplorerPluginAccessFacts.Withheld,
                    remedy,
                    holds
                        ? ExplorerPluginAccessState.Allowed
                        : expected);
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(cells, Is.GreaterThan(0), "the matrix must exercise at least one gate");
            Assert.That(
                violations,
                Is.Empty,
                "Every gate must resolve the four-state contract identically: a capability the "
                + "cluster does not serve is Unavailable, a held grant is Allowed, a withheld "
                + "grant is Denied with a remedy for a signed-in caller and "
                + "AuthenticationRequired for an anonymous one."
                + Environment.NewLine
                + Describe(violations));
        });
    }

    [Test]
    public void A_gate_that_does_not_participate_in_the_contract_is_reported_without_editing_the_guard()
    {
        // The battery test for the smoke detector, and the proof of the issue's
        // acceptance criterion that a new plugin is covered automatically. The
        // gate below is declared in this file and named nowhere in the discovery
        // or verification helpers, exactly as a plugin added next month would be.
        var discovered = GateTypes([typeof(ExplorerPluginAccessGateConformanceTests).Assembly]).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(
                discovered,
                Does.Contain(typeof(UnregisteredPluginAccessGate)),
                "the sweep must discover a gate it was never told about");

            Assert.That(
                ContractParticipationViolations(typeof(UnregisteredPluginAccessGate)),
                Is.Not.Empty,
                "a gate that hand-rolls its own state mapping must be reported");

            Assert.That(
                ContractParticipationViolations(typeof(ConformingPluginAccessGate)),
                Is.Empty,
                "a gate that resolves through the shared contract must pass");
        });
    }

    [Test]
    public void The_contract_orders_capability_before_credential_before_grant()
    {
        var remedy = ExplorerAccessRemedy.Requiring("Backup", "an operator");

        Assert.Multiple(() =>
        {
            // An absent capability outranks everything: there is nothing to sign
            // in for and nothing to be granted.
            Assert.That(
                ExplorerPluginAccessContract
                    .Resolve(ExplorerPluginAccessFacts.CapabilityAbsent(), remedy, isCallerAuthenticated: false)
                    .State,
                Is.EqualTo(ExplorerPluginAccessState.Unavailable));

            // What the probe observed beats what the shell believes: a server
            // that answered Unauthenticated knows the credential never arrived.
            Assert.That(
                ExplorerPluginAccessContract
                    .Resolve(ExplorerPluginAccessFacts.CredentialMissing(), remedy, isCallerAuthenticated: true)
                    .State,
                Is.EqualTo(ExplorerPluginAccessState.AuthenticationRequired));

            // ... and the converse, so a stale shell view cannot manufacture a
            // sign-in prompt for a caller the server saw as authenticated.
            Assert.That(
                ExplorerPluginAccessContract.Resolve(
                        ExplorerPluginAccessFacts.Withheld
                            .WithAuthentication(ExplorerPluginCallerAuthentication.Authenticated),
                        remedy,
                        isCallerAuthenticated: false)
                    .State,
                Is.EqualTo(ExplorerPluginAccessState.Denied));

            // A denial carries the structured remedy #1850 renders.
            var denial = ExplorerPluginAccessContract
                .Resolve(ExplorerPluginAccessFacts.Withheld, remedy, isCallerAuthenticated: true);
            Assert.That(denial.Remedy.Permission, Is.EqualTo("Backup"));
            Assert.That(denial.Remedy.Audience, Is.EqualTo("an operator"));
            Assert.That(denial.Remedy.Describe(), Is.EqualTo("Requires the Backup permission - ask an operator."));

            // An admission never carries one - there is nothing to remedy.
            Assert.That(
                ExplorerPluginAccessContract
                    .Resolve(ExplorerPluginAccessFacts.Granted, remedy, isCallerAuthenticated: true)
                    .Remedy
                    .IsSpecified,
                Is.False);

            // Default facts fail closed without hiding a surface that exists.
            Assert.That(
                ExplorerPluginAccessContract
                    .Resolve(default, ExplorerAccessRemedy.None, isCallerAuthenticated: true)
                    .State,
                Is.EqualTo(ExplorerPluginAccessState.Denied));
        });
    }

    /// <summary>
    /// The reasons <paramref name="gate"/> does not participate in the shared
    /// contract, or an empty sequence when it does. Named by neither the gate
    /// nor its package, so it applies unchanged to a plugin added later.
    /// </summary>
    /// <param name="gate">The discovered gate type.</param>
    private static IEnumerable<string> ContractParticipationViolations(Type gate)
    {
        if (!typeof(ExplorerPluginAccessGate).IsAssignableFrom(gate))
        {
            yield return $"{gate.Name}: does not derive from ExplorerPluginAccessGate";
            yield break;
        }

        // Deriving is not enough on its own: a gate could still re-implement the
        // interface member and route around the contract.
        var redeclared = gate
            .GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly)
            .Any(m => m.Name.EndsWith(nameof(IExplorerPluginAccessGate.ProbeAsync), StringComparison.Ordinal)
                && m.GetParameters() is [{ ParameterType: var first }, ..]
                && first == typeof(IExplorerPluginHostContext));

        if (redeclared)
        {
            yield return $"{gate.Name}: re-declares ProbeAsync instead of overriding EvaluateAsync";
        }
    }

    /// <summary>
    /// Resolves <paramref name="facts"/> for one matrix cell and records a
    /// violation when the state is not <paramref name="expected"/>.
    /// </summary>
    private static void Check(
        List<string> violations,
        Type gate,
        TestIdentity identity,
        string cell,
        in ExplorerPluginAccessFacts facts,
        in ExplorerAccessRemedy remedy,
        ExplorerPluginAccessState expected)
    {
        var actual = ExplorerPluginAccessContract.Resolve(facts, remedy, identity.IsAuthenticated).State;
        if (actual != expected)
        {
            violations.Add($"{gate.Name} / {identity.Name} / {cell}: expected {expected}, got {actual}");
        }
    }

    /// <summary>
    /// Probes a real gate instance built from substituted dependencies, with the
    /// sign-in seam reporting <paramref name="authenticated"/>. Substituted
    /// transports answer nothing, which is precisely "the caller was not shown
    /// to hold the grant" - the input both measured defects mishandled.
    /// </summary>
    private static ExplorerPluginAccessState? Probe(Type gate, bool authenticated, out string? fault)
    {
        fault = null;

        if (Construct(gate, authenticated) is not IExplorerPluginAccessGate instance)
        {
            fault = "could not be constructed from substituted dependencies";
            return null;
        }

        try
        {
            var context = Substitute.For<IExplorerPluginHostContext>();
            context.PluginId.Returns("conformance");
            context.Connection.Returns(new ExplorerPluginConnectionStatus(ExplorerPluginConnectionState.Connected));
            context.Tenant.Returns(new ExplorerPluginTenantScope(
                IsActive: true,
                ActiveTenantId: "conformance-tenant",
                ExplorerPluginTenantVisibility.ActiveTenant));

            return instance.ProbeAsync(context, CancellationToken.None).AsTask().GetAwaiter().GetResult().State;
        }
        catch (Exception ex)
        {
            // A gate must fold a fault into facts rather than throw: the host
            // contains a throwing gate at Denied, which silently destroys the
            // very distinction this contract preserves.
            fault = ex.GetType().Name;
            return null;
        }
    }

    /// <summary>
    /// Builds <paramref name="gate"/> from substituted dependencies, wiring the
    /// sign-in seam to <paramref name="authenticated"/>. Returns
    /// <see langword="null"/> when no constructor can be satisfied.
    /// </summary>
    private static object? Construct(Type gate, bool authenticated)
    {
        foreach (var constructor in Constructors(gate).OrderByDescending(c => c.GetParameters().Length))
        {
            var parameters = constructor.GetParameters();
            var arguments = new object?[parameters.Length];
            var satisfied = true;

            for (var i = 0; i < parameters.Length; i++)
            {
                if (!TryResolve(parameters[i], authenticated, out arguments[i]))
                {
                    satisfied = false;
                    break;
                }
            }

            if (!satisfied)
            {
                continue;
            }

            try
            {
                return constructor.Invoke(arguments);
            }
            catch (TargetInvocationException)
            {
                // A constructor that rejects a substituted argument is not a
                // conformance failure by itself; try a narrower one.
            }
        }

        return null;
    }

    private static bool TryResolve(ParameterInfo parameter, bool authenticated, out object? argument)
    {
        var type = parameter.ParameterType;

        if (type == typeof(IExplorerAuthSession))
        {
            var session = Substitute.For<IExplorerAuthSession>();
            session.IsAuthenticated.Returns(authenticated);
            argument = session;
            return true;
        }

        if (type.IsInterface)
        {
            argument = Substitute.For([type], []);
            return true;
        }

        if (parameter.HasDefaultValue)
        {
            argument = parameter.DefaultValue;
            return true;
        }

        if (type.IsValueType)
        {
            argument = Activator.CreateInstance(type);
            return true;
        }

        argument = null;
        return false;
    }

    private static IEnumerable<ConstructorInfo> Constructors(Type gate) =>
        gate.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);

    /// <summary>
    /// Every concrete plugin access gate declared in <paramref name="assemblies"/>.
    /// <para>
    /// The framework's own fixed and delegate gates are excluded: they report
    /// the decision their caller handed them, so there is no probe of theirs to
    /// hold to the contract.
    /// </para>
    /// </summary>
    private static IEnumerable<Type> GateTypes(IEnumerable<Assembly> assemblies) =>
        assemblies
            .SelectMany(SafeTypes)
            .Where(t => t is { IsClass: true, IsAbstract: false })
            .Where(t => typeof(IExplorerPluginAccessGate).IsAssignableFrom(t))
            .Where(t => t.DeclaringType != typeof(ExplorerPluginAccessGates))
            .OrderBy(t => t.FullName, StringComparer.Ordinal);

    /// <summary>
    /// The Explorer's own assemblies, walked transitively from this one so a
    /// plugin package added to the test project's references is swept without
    /// this fixture naming it.
    /// </summary>
    private static IEnumerable<Assembly> ExplorerAssemblies()
    {
        const string Prefix = "Orleans.Lattice.Explorer";

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var found = new List<Assembly>();
        var pending = new Queue<Assembly>();
        pending.Enqueue(typeof(ExplorerPluginAccessGateConformanceTests).Assembly);

        while (pending.Count > 0)
        {
            var assembly = pending.Dequeue();
            foreach (var reference in assembly.GetReferencedAssemblies())
            {
                if (reference.Name is not { } name
                    || !name.StartsWith(Prefix, StringComparison.Ordinal)
                    || !seen.Add(name))
                {
                    continue;
                }

                Assembly loaded;
                try
                {
                    loaded = Assembly.Load(reference);
                }
                catch (FileNotFoundException)
                {
                    continue;
                }
                catch (BadImageFormatException)
                {
                    continue;
                }

                found.Add(loaded);
                pending.Enqueue(loaded);
            }
        }

        return found;
    }

    private static IEnumerable<Type> SafeTypes(Assembly assembly)
    {
        try
        {
            return assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            return ex.Types.Where(t => t is not null)!;
        }
    }

    private static string Describe(IEnumerable<string> lines) => string.Join(Environment.NewLine, lines);

    /// <summary>
    /// One local-dev identity: whether it presented a credential, and the
    /// cluster grants its group holds.
    /// </summary>
    /// <param name="Name">The identity's name in <c>identities.json</c>.</param>
    /// <param name="IsAuthenticated">Whether a credential is applied.</param>
    /// <param name="Grants">The permission names the identity's group holds.</param>
    private sealed record TestIdentity(string Name, bool IsAuthenticated, string[] Grants)
    {
        public bool Holds(string permission) => Grants.Contains(permission, StringComparer.Ordinal);
    }

    /// <summary>
    /// A gate that resolves through the shared contract, so the guard passes it.
    /// Exists only to prove the guard is not simply reporting everything.
    /// </summary>
    private sealed class ConformingPluginAccessGate : ExplorerPluginAccessGate
    {
        public override ExplorerAccessRemedy Remedy { get; } =
            ExplorerAccessRemedy.Requiring("Example", "an administrator");

        protected override bool IsCallerAuthenticated => false;

        protected override ValueTask<ExplorerPluginAccessFacts> EvaluateAsync(
            IExplorerPluginHostContext context,
            CancellationToken cancellationToken) =>
            new(ExplorerPluginAccessFacts.Withheld);
    }

    /// <summary>
    /// A gate that hand-rolls its own state mapping, exactly as every gate did
    /// before issue #1854. The guard must report it purely by discovering it.
    /// </summary>
    private sealed class UnregisteredPluginAccessGate : IExplorerPluginAccessGate
    {
        public ValueTask<ExplorerPluginAccess> ProbeAsync(
            IExplorerPluginHostContext context,
            CancellationToken cancellationToken = default) =>
            new(ExplorerPluginAccess.Denied);
    }
}
