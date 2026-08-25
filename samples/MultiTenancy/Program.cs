using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Samples.MultiTenancy;
using Orleans.Lattice.Tenancy;

// ---------------------------------------------------------------------------
// MultiTenancy - the opt-in tenancy layer on a single silo.
//
// One in-process Orleans silo runs the full control-plane stack: Membership
// (identity) + Auth (a fail-closed gate) + Tenancy (the tenant registry and
// isolation seams) + the Tenant-Admin API facade. With none of these packages
// referenced the core tree is byte-for-byte unchanged; adding them turns on
// multi-tenancy without editing a line of core.
//
// The sample walks four acts:
//
//   1. Tenant tree naming. A tenant-scoped tree id self-describes its owner:
//      LatticeTenantTrees.Compose(tenant, name) -> "t/{tenant}/{name}", and
//      TryGetTenant reverses it. This structural prefix is what the isolation
//      gate checks - a caller in tenant "acme" can only ever name "t/acme/*".
//   2. Tenant lifecycle as a platform operator. A bootstrap administrator
//      creates two tenants and reads back their lifecycle status.
//   3. Lifecycle transitions and guards. Suspend / resume a tenant, delete a
//      tenant (cascading its trees), and prove create is not upsert - a second
//      create of the same id is refused (TenantAlreadyExistsException).
//   4. Fail-closed control plane. The reserved "default" tenant can never be
//      deleted or suspended (ReservedTenantOperationException), and a caller
//      who is not a platform operator is denied every lifecycle op
//      (LatticeAuthorizationDeniedException) under the default-deny gate.
// ---------------------------------------------------------------------------

const string Scheme = DemoAuthenticator.Scheme;

using var host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(logging =>
    {
        logging.ClearProviders();
        logging.SetMinimumLevel(LogLevel.None);
    })
    .UseOrleans(silo =>
    {
        silo.UseLocalhostClustering();
        silo.AddMemoryGrainStorageAsDefault();
        silo.UseInMemoryReminderService();
        silo.AddLattice((services, name) => services.AddMemoryGrainStorage(name));

        // Membership resolves the ambient caller credential into a subject.
        silo.AddLatticeMembership();

        // Auth installs the enforcement gate. Default-deny (the production
        // posture): tenant-lifecycle operations authorize the cluster-wide Admin
        // capability, which only a bootstrap administrator (or an explicitly
        // authored cluster-wide Admin rule) holds - so the operator seam is
        // fail-closed against every other caller.
        silo.AddLatticeAuth(options =>
        {
            options.DefaultEffect = LatticeEffect.Deny;
            options.BootstrapAdministrators.Add("platform-operator");
        });

        // Tenancy turns on the tenant registry, isolation seams, and the default
        // tenant. The Tenant-Admin API adds the operator control-plane facade.
        silo.AddLatticeTenancy();
        silo.AddLatticeTenantAdminApi();

        silo.Services.AddSingleton<ILatticeCredentialAuthenticator, DemoAuthenticator>();
    })
    .Build();

Console.Write("Silo starting...");
await host.StartAsync();
Console.WriteLine(" ready.\n");

var admin = host.Services.GetRequiredService<ILatticeTenantAdmin>();

// -- Act 1: tenant tree naming ----------------------------------------------
Console.WriteLine("== Act 1: a tenant-scoped tree id self-describes its owner ==");
var acme = TenantId.Parse("acme");
var composed = LatticeTenantTrees.Compose(acme, "catalog");
Console.WriteLine($"  Compose(acme, 'catalog')     -> '{composed}'");
Console.WriteLine($"  IsTenantScoped('{composed}') -> {LatticeTenantTrees.IsTenantScoped(composed)}");
Console.WriteLine(
    LatticeTenantTrees.TryGetTenant(composed, out var owner)
        ? $"  TryGetTenant('{composed}')   -> '{owner}'  (isolation gate checks this prefix)\n"
        : "  TryGetTenant failed\n");

// -- Act 2: create tenants as a platform operator ---------------------------
Console.WriteLine("== Act 2: create tenants as a platform operator ==");
TenantCreationResult acmeCreated;
TenantCreationResult globexCreated;
using (LatticeCredentialContext.Use("platform-operator", scheme: Scheme))
{
    acmeCreated = await admin.CreateTenantAsync("acme");
    globexCreated = await admin.CreateTenantAsync("globex");
}

Console.WriteLine($"  created '{acmeCreated.TenantId}'   -> {acmeCreated.Status}");
Console.WriteLine($"  created '{globexCreated.TenantId}' -> {globexCreated.Status}\n");

// -- Act 3: lifecycle transitions and guards --------------------------------
Console.WriteLine("== Act 3: lifecycle transitions and guards ==");
using (LatticeCredentialContext.Use("platform-operator", scheme: Scheme))
{
    var suspended = await admin.SuspendTenantAsync("acme");
    Console.WriteLine($"  suspend 'acme'  -> {suspended.PreviousStatus} => {suspended.NewStatus} (changed: {suspended.Changed})");

    var resumed = await admin.ResumeTenantAsync("acme");
    Console.WriteLine($"  resume  'acme'  -> {resumed.PreviousStatus} => {resumed.NewStatus} (changed: {resumed.Changed})");

    var deleted = await admin.DeleteTenantAsync("globex");
    Console.WriteLine($"  delete  'globex' -> removed, {deleted.CascadedTreeCount} tree(s) cascaded");

    // Create is not upsert: a second create of the same id is refused.
    try
    {
        await admin.CreateTenantAsync("acme");
        Console.WriteLine("  re-create 'acme' -> UNEXPECTEDLY allowed");
    }
    catch (TenantAlreadyExistsException)
    {
        Console.WriteLine("  re-create 'acme' -> refused (TenantAlreadyExistsException)");
    }
}

Console.WriteLine();

// -- Act 4: fail-closed control plane ---------------------------------------
Console.WriteLine("== Act 4: fail-closed control plane ==");
var reservedProtected = false;
using (LatticeCredentialContext.Use("platform-operator", scheme: Scheme))
{
    try
    {
        await admin.DeleteTenantAsync(TenantId.DefaultId);
        Console.WriteLine("  delete reserved 'default' -> UNEXPECTEDLY allowed");
    }
    catch (ReservedTenantOperationException)
    {
        Console.WriteLine("  delete reserved 'default' -> refused (ReservedTenantOperationException)");
        reservedProtected = true;
    }
}

var operatorGated = false;
using (LatticeCredentialContext.Use("mallory", scheme: Scheme))
{
    try
    {
        await admin.CreateTenantAsync("mallory-corp");
        Console.WriteLine("  create as non-operator 'mallory' -> UNEXPECTEDLY allowed");
    }
    catch (LatticeAuthorizationDeniedException)
    {
        Console.WriteLine("  create as non-operator 'mallory' -> denied (LatticeAuthorizationDeniedException)");
        operatorGated = true;
    }
}

Console.WriteLine();
var ok = reservedProtected && operatorGated;
Console.WriteLine(ok
    ? "[OK] tenant lifecycle ran end-to-end; the reserved tenant and the operator seam stayed fail-closed."
    : "[FAIL] a control-plane guard did not hold.");

await host.StopAsync();
return ok ? 0 : 1;
