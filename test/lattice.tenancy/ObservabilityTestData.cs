using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Deterministic, timing-free doubles for the per-tenant observability tests: a
/// substitutable warm usage index, a substitutable overage-billing seam, and a
/// hand-written <see cref="ILatticeAccessGate"/> fake (NSubstitute cannot mock the
/// gate's <c>in</c> parameter). Every double serves fixed in-memory state, so the
/// tests are exact and never depend on ordering, delays, or the wall clock.
/// </summary>
internal static class ObservabilityTestData
{
    /// <summary>Builds a usage view from quotas and the global usage fold.</summary>
    internal static TenantUsageView View(TenantQuotas quotas, LocalUsageSample globalUsage) =>
        new(quotas, globalUsage, globalUsage);

    /// <summary>
    /// A substitutable <see cref="ITenantUsageIndex"/> serving a fixed tenant-to-view
    /// map. <see cref="EnsureWarmAsync"/> is a synchronous no-op and both reads are
    /// pure dictionary lookups, so there is no timing dependency.
    /// </summary>
    internal sealed class FakeTenantUsageIndex : ITenantUsageIndex
    {
        private readonly Dictionary<string, TenantUsageView> _views = new(StringComparer.Ordinal);

        /// <summary>The number of times <see cref="EnsureWarmAsync"/> was awaited.</summary>
        public int WarmCount { get; private set; }

        /// <summary>Adds or replaces a tenant's warm view.</summary>
        public FakeTenantUsageIndex With(TenantId tenant, TenantUsageView view)
        {
            _views[tenant.Value!] = view;
            return this;
        }

        public bool TryGetView(TenantId tenant, out TenantUsageView view) =>
            _views.TryGetValue(tenant.Value ?? string.Empty, out view);

        public Task EnsureWarmAsync(CancellationToken cancellationToken = default)
        {
            WarmCount++;
            return Task.CompletedTask;
        }

        public IReadOnlyDictionary<string, TenantUsageView> EnumerateViews() => _views;
    }

    /// <summary>
    /// A substitutable <see cref="ITenantOverageBilling"/> serving a fixed
    /// tenant-to-overage map. A tenant with no entry reads as
    /// <see cref="TenantOverageSample.Empty"/>, and the list projection streams only
    /// the metered tenants.
    /// </summary>
    internal sealed class FakeTenantOverageBilling : ITenantOverageBilling
    {
        private readonly Dictionary<string, TenantOverageSample> _overage = new(StringComparer.Ordinal);

        /// <summary>Adds or replaces a tenant's converged metered overage.</summary>
        public FakeTenantOverageBilling With(TenantId tenant, TenantOverageSample overage)
        {
            _overage[tenant.Value!] = overage;
            return this;
        }

        public Task<TenantOverageSample> GetMeteredOverageAsync(TenantId tenant, CancellationToken cancellationToken = default) =>
            Task.FromResult(_overage.TryGetValue(tenant.Value ?? string.Empty, out var sample)
                ? sample
                : TenantOverageSample.Empty);

#pragma warning disable CS1998 // synchronous fake enumerator
        public async IAsyncEnumerable<TenantMeteredOverage> ListMeteredOverageAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var (id, overage) in _overage)
            {
                yield return new TenantMeteredOverage(TenantId.Parse(id), overage);
            }
        }
#pragma warning restore CS1998
    }

    /// <summary>
    /// A minimal hand-written <see cref="ILatticeAccessGate"/> whose per-request
    /// decision is a supplied predicate over the request. Records the call count so
    /// a test can assert the gate was (or was not) consulted.
    /// </summary>
    internal sealed class FakeAccessGate(Func<LatticeAccessRequest, LatticeAccessDecision> decide) : ILatticeAccessGate
    {
        /// <summary>The number of authorize calls the gate received.</summary>
        public int CallCount { get; private set; }

        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default)
        {
            var copy = request;
            CallCount++;
            return new ValueTask<LatticeAccessDecision>(decide(copy));
        }
    }

    /// <summary>A gate that allows every request.</summary>
    internal static FakeAccessGate AllowingGate() => new(_ => LatticeAccessDecision.Allow());

    /// <summary>A gate that denies every request.</summary>
    internal static FakeAccessGate DenyingGate() => new(_ => LatticeAccessDecision.Deny("nope"));
}
