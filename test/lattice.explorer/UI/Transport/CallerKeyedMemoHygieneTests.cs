using System.Collections;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Suggestions;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>
/// The design rule behind issue #4019: every per-circuit service that remembers
/// something between calls files it under the caller (<see cref="ShellCallerKey"/>),
/// so an answer read for one sign-in, endpoint or tenant is never served to the
/// next. A memo keyed on the tenant alone, on a tree id alone, or on nothing, is
/// exactly the defect.
/// </summary>
/// <remarks>
/// <para>
/// The population is every scoped service <c>AddLatticeExplorerShell</c> registers,
/// plus every suggestion source, completion source, area and accessible-tenant
/// source, which scoped services own for the circuit's life. A type is stateful
/// when it holds a field that is not an injected dependency: a writable field, or
/// a read-only mutable collection. A stateful type passes only when that state
/// stores a <see cref="ShellCallerKey"/> (directly, or inside a tuple, a generic
/// argument or a nested record), or when it is listed in <see cref="Reviewed"/>
/// with the reason its state is not a cluster answer.
/// </para>
/// <para>
/// Adding a new memo therefore fails here until it is keyed on the caller: the
/// fix is to store <c>ShellCaller.Current</c> beside the memo, serve the memo only
/// while it still equals the current key, and write it back only when the caller
/// did not change while it was read.
/// </para>
/// </remarks>
[TestFixture]
public sealed class CallerKeyedMemoHygieneTests
{
    /// <summary>Per-circuit state that has been reviewed and is not a memo of a cluster answer.</summary>
    private static readonly Dictionary<Type, string> Reviewed = new()
    {
        [typeof(ShellCaller)] = "It is the caller key itself: a counter of sign-in and connection changes.",
        [typeof(ShellTransportChannel)] = "The channel is rebuilt for the connection; every call carries the credential and tenant of the moment.",
        [typeof(LtToastService)] = "Notices this circuit raised; no cluster answer is remembered.",
        [typeof(Orleans.Lattice.Explorer.UI.Layout.Appearance.ShellAppearance)] = "Theme, contrast and density: a display preference, not a cluster answer.",
        [typeof(Orleans.Lattice.Explorer.UI.Layout.ShellChromeInterop)] = "The imported JavaScript module.",
        [typeof(BackupsInterop)] = "The imported JavaScript module.",
        [typeof(Orleans.Lattice.Explorer.UI.Areas.Replication.JsReplicationPageVisibility)] = "The page-visibility JavaScript callback.",
        [typeof(ExplorerAreaDirectory)] = "The registered areas by key; availability is asked of each area, which keys its own verdict.",
        [typeof(ClusterCommandSignals)] = "A palette command waiting for its page: a command id, not a cluster answer.",
        [typeof(SchemaCommandSignals)] = "A palette command waiting for its page: a command id, not a cluster answer.",
        [typeof(Orleans.Lattice.Explorer.UI.Session.SessionChromeState)] = "Session chrome state that follows the sign-in itself.",
        [typeof(Orleans.Lattice.Explorer.UI.Session.SessionConnectionAnnouncer)] = "Whether the connection announcement has started; no cluster answer.",
        [typeof(AppsLifecycleIntents)] = "Lifecycle intents this circuit's user started and has not yet finished; no cluster answer is remembered.",
        [typeof(BackupOperations)] = "Staged operations this circuit started, each pinned to the tenant it began in; progress of work, not a memo.",
        [typeof(SchemaOperations)] = "Schema operations this circuit started; progress of work, not a memo.",
        [typeof(ExplorerSuggestions)] = "It holds the circuit's suggestion sources, not their answers; each source keys its own memo.",
        [typeof(TreeSuggestionSource)] = CallerKeyedProjection,
        [typeof(ClusterTreeSuggestionSource)] = CallerKeyedProjection,
        [typeof(Orleans.Lattice.Explorer.UI.Areas.Access.AccessRuleIdSuggestionSource)] = CallerKeyedProjection,
    };

    private const string CallerKeyedProjection =
        "A projection of a caller-keyed catalogue, rebuilt whenever the catalogue returns another list instance, so it never outlives the caller it was read for.";

    private static readonly Type[] OwnedSeams =
    [
        typeof(ILtSuggestionSource),
        typeof(IAddressCompletionSource),
        typeof(IExplorerArea),
        typeof(IExplorerAccessibleTenantSource),
    ];

    [Test]
    public void Every_per_circuit_memo_is_keyed_on_the_caller()
    {
        var violations = new List<string>();
        foreach (var type in Population())
        {
            var state = StateFields(type);
            if (state.Count == 0 || Reviewed.ContainsKey(type))
            {
                continue;
            }

            if (!state.Any(field => Mentions(field.FieldType, typeof(ShellCallerKey), [])))
            {
                violations.Add($"{type.FullName} remembers {string.Join(", ", state.Select(field => field.Name))} without a ShellCallerKey");
            }
        }

        Assert.That(violations, Is.Empty, string.Join(Environment.NewLine, violations));
    }

    [Test]
    public void The_scan_sees_the_memos_of_issue_4019()
    {
        // Battery test: the sweep must reach the memos the issue named and judge
        // each stateful, or it passes vacuously.
        var population = Population().ToHashSet();
        Type[] known =
        [
            typeof(AppsAccess),
            typeof(AppInstallFlowStore),
            typeof(ClusterArea),
            typeof(ClusterTreeCatalog),
            typeof(TenantSuggestionSource),
            typeof(RegionSuggestionSource),
            typeof(Orleans.Lattice.Explorer.UI.Areas.Access.AccessCatalog),
            typeof(SchemaTreeCatalog),
            typeof(SchemaAccess),
            typeof(BackupsAccess),
            typeof(DataAdminGate),
            typeof(DataDirectory),
            typeof(TenancyCatalog),
            typeof(TenancyAccessibleTenantSource),
            typeof(Orleans.Lattice.Explorer.UI.Areas.Telemetry.TelemetryCatalogCache),
        ];

        Assert.Multiple(() =>
        {
            foreach (var type in known)
            {
                Assert.That(population, Does.Contain(type), type.Name + " is in the population");
                Assert.That(StateFields(type), Is.Not.Empty, type.Name + " is judged stateful");
            }
        });
    }

    [Test]
    public void The_rule_rejects_a_memo_keyed_on_the_tenant_alone()
    {
        Assert.Multiple(() =>
        {
            Assert.That(StateFields(typeof(TenantKeyedProbe)), Is.Not.Empty);
            Assert.That(StateFields(typeof(TenantKeyedProbe)).Any(field => Mentions(field.FieldType, typeof(ShellCallerKey), [])), Is.False);
            Assert.That(StateFields(typeof(CallerKeyedProbe)).Any(field => Mentions(field.FieldType, typeof(ShellCallerKey), [])), Is.True);
        });
    }

    [Test]
    public void Every_reviewed_exemption_is_a_stateful_per_circuit_type()
    {
        var population = Population().ToHashSet();
        Assert.Multiple(() =>
        {
            foreach (var (type, reason) in Reviewed)
            {
                Assert.That(population, Does.Contain(type), type.Name + " is per-circuit");
                Assert.That(StateFields(type), Is.Not.Empty, type.Name + " still holds state; drop the exemption");
                Assert.That(reason, Is.Not.Empty, type.Name + " says why");
            }
        });
    }

    private static IEnumerable<Type> Population()
    {
        var assembly = typeof(IExplorerArea).Assembly;
        var services = new ServiceCollection().AddLatticeExplorerShell();
        var registered = services
            .Where(descriptor => descriptor.Lifetime == ServiceLifetime.Scoped)
            .Select(descriptor => descriptor.IsKeyedService
                ? descriptor.KeyedImplementationType ?? descriptor.ServiceType
                : descriptor.ImplementationType ?? descriptor.ServiceType);
        var owned = assembly.GetTypes().Where(type => OwnedSeams.Any(seam => seam.IsAssignableFrom(type)));

        return registered.Concat(owned)
            .Where(type => type.Assembly == assembly && type.IsClass && !type.IsAbstract)
            .Distinct();
    }

    /// <summary>The fields that hold state rather than an injected dependency, declared on the type or a base in the same assembly.</summary>
    private static List<FieldInfo> StateFields(Type type)
    {
        var fields = new List<FieldInfo>();
        for (var current = type; current is not null && current.Assembly == type.Assembly; current = current.BaseType)
        {
            foreach (var field in current.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
            {
                if (typeof(Delegate).IsAssignableFrom(field.FieldType) || IsCapturedParameter(field))
                {
                    continue;
                }

                if (!field.IsInitOnly || IsMutableCollection(field.FieldType))
                {
                    fields.Add(field);
                }
            }
        }

        return fields;
    }

    // A primary-constructor parameter captured by the compiler: an injected dependency.
    private static bool IsCapturedParameter(FieldInfo field) => field.Name.StartsWith('<') && field.Name.EndsWith(">P", StringComparison.Ordinal);

    private static bool IsMutableCollection(Type type) =>
        !type.IsInterface
        && !type.IsArray
        && (typeof(IDictionary).IsAssignableFrom(type)
            || type.GetInterfaces().Any(candidate => candidate.IsGenericType
                && (candidate.GetGenericTypeDefinition() == typeof(ICollection<>) || candidate.GetGenericTypeDefinition() == typeof(IDictionary<,>))));

    private static bool Mentions(Type type, Type key, HashSet<Type> seen)
    {
        if (type == key)
        {
            return true;
        }

        if (!seen.Add(type))
        {
            return false;
        }

        if (type.HasElementType && Mentions(type.GetElementType()!, key, seen))
        {
            return true;
        }

        if (type.IsGenericType && type.GetGenericArguments().Any(argument => Mentions(argument, key, seen)))
        {
            return true;
        }

        // A record or struct the memo declares for itself, such as a remembered answer and its key.
        return type.Assembly == typeof(IExplorerArea).Assembly
            && !type.IsInterface
            && type.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                .Any(field => Mentions(field.FieldType, key, seen));
    }

    private sealed class TenantKeyedProbe
    {
        private string? _tenant;
        private object? _answer;

        public void Remember(string? tenant, object answer) => (_tenant, _answer) = (tenant, answer);

        public override string ToString() => _tenant + _answer;
    }

    private sealed class CallerKeyedProbe
    {
        private ShellCallerKey _caller;
        private object? _answer;

        public void Remember(ShellCallerKey caller, object answer) => (_caller, _answer) = (caller, answer);

        public override string ToString() => _caller.ToString() + _answer;
    }
}
