using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema;

/// <summary>
/// Default <see cref="ILatticeSchemaControl"/> implementation. Registered as a silo
/// singleton by <c>AddLatticeSchemaApi</c>; it drives the in-process schema admin
/// surfaces (policy, versioning, remediation, compliance audit) and gates every
/// operation through the shared <see cref="SchemaAccessAuthorizer"/> fail-closed
/// before touching the admin plane.
/// </summary>
/// <remarks>
/// Schema versioning is registered by a separate add-on
/// (<c>AddLatticeSchemaVersioning</c>), so the version admin is resolved optionally:
/// a version operation invoked on a silo without versioning registered throws a
/// clear <see cref="InvalidOperationException"/> rather than a DI resolution failure.
/// </remarks>
internal sealed class LatticeSchemaControl : ILatticeSchemaControl
{
    private readonly ILatticeSchemaAdmin _admin;
    private readonly ILatticeSchemaRemediationAdmin _remediation;
    private readonly ILatticeSchemaComplianceAdmin _compliance;
    private readonly SchemaAccessAuthorizer _authorizer;
    private readonly ILatticeSchemaVersionAdmin? _versionAdmin;

    /// <summary>Initializes a new <see cref="LatticeSchemaControl"/>.</summary>
    /// <param name="admin">The schema policy / dead-letter admin. Must not be <c>null</c>.</param>
    /// <param name="remediation">The schema remediation admin. Must not be <c>null</c>.</param>
    /// <param name="compliance">The read-only compliance-audit admin. Must not be <c>null</c>.</param>
    /// <param name="authorizer">The fail-closed schema authorization seam. Must not be <c>null</c>.</param>
    /// <param name="options">The facade options. Must not be <c>null</c>.</param>
    /// <param name="services">The silo service provider, used to resolve the optional schema version admin. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">A required dependency is <c>null</c>.</exception>
    public LatticeSchemaControl(
        ILatticeSchemaAdmin admin,
        ILatticeSchemaRemediationAdmin remediation,
        ILatticeSchemaComplianceAdmin compliance,
        SchemaAccessAuthorizer authorizer,
        IOptions<LatticeApiSchemaOptions> options,
        IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(admin);
        ArgumentNullException.ThrowIfNull(remediation);
        ArgumentNullException.ThrowIfNull(compliance);
        ArgumentNullException.ThrowIfNull(authorizer);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(services);

        _admin = admin;
        _remediation = remediation;
        _compliance = compliance;
        _authorizer = authorizer;
        _versionAdmin = services.GetService<ILatticeSchemaVersionAdmin>();
    }

    /// <inheritdoc />
    public async Task SetPolicyAsync(string treeId, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(policy);
        await _authorizer.AuthorizeManageAsync(treeId, cancellationToken).ConfigureAwait(false);
        await _admin.SetPolicyAsync(treeId, policy, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<bool> ClearPolicyAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeManageAsync(treeId, cancellationToken).ConfigureAwait(false);
        return await _admin.ClearPolicyAsync(treeId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<LatticeSchemaPolicy?> GetPolicyAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeReadAsync(treeId, cancellationToken).ConfigureAwait(false);
        return await _admin.GetPolicyAsync(treeId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<LatticeSchemaDeadLetterEntry> ListDeadLettersAsync(
        string treeId,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeReadAsync(treeId, cancellationToken).ConfigureAwait(false);
        await foreach (var entry in _admin.ListDeadLettersAsync(treeId, cancellationToken).ConfigureAwait(false))
        {
            yield return entry;
        }
    }

    /// <inheritdoc />
    public async Task<int> CountDeadLettersAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeReadAsync(treeId, cancellationToken).ConfigureAwait(false);
        return await _admin.CountDeadLettersAsync(treeId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task SetVersionConfigAsync(
        string treeId, LatticeSchemaVersionConfig config, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeManageAsync(treeId, cancellationToken).ConfigureAwait(false);
        await RequireVersionAdmin().SetVersionConfigAsync(treeId, config, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<LatticeSchemaVersionConfig?> GetVersionConfigAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeReadAsync(treeId, cancellationToken).ConfigureAwait(false);
        return await RequireVersionAdmin().GetVersionConfigAsync(treeId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<LatticeSchemaVersionConfig> AdvanceTargetVersionAsync(
        string treeId, uint newTargetVersion, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeManageAsync(treeId, cancellationToken).ConfigureAwait(false);
        return await RequireVersionAdmin()
            .AdvanceTargetVersionAsync(treeId, newTargetVersion, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<LatticeSchemaRemediationReport> AdvanceAndMigrateAsync(
        string treeId, uint newTargetVersion, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeManageAsync(treeId, cancellationToken).ConfigureAwait(false);
        return await RequireVersionAdmin()
            .AdvanceAndMigrateAsync(treeId, newTargetVersion, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<LatticeSchemaRemediationReport> MigrateToTargetVersionAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeManageAsync(treeId, cancellationToken).ConfigureAwait(false);
        return await RequireVersionAdmin()
            .MigrateToTargetVersionAsync(treeId, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<bool> ClearVersionConfigAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeManageAsync(treeId, cancellationToken).ConfigureAwait(false);
        return await RequireVersionAdmin().ClearVersionConfigAsync(treeId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<LatticeSchemaRemediationReport> RemediateAsync(
        string treeId,
        LatticeValueTransform transform,
        LatticeSchemaPolicy targetPolicy,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(targetPolicy);
        await _authorizer.AuthorizeManageAsync(treeId, cancellationToken).ConfigureAwait(false);
        return await _remediation
            .RemediateAsync(treeId, transform, targetPolicy, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<LatticeSchemaRemediationReport> GetRemediationStatusAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeReadAsync(treeId, cancellationToken).ConfigureAwait(false);
        return await _remediation.GetRemediationStatusAsync(treeId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<LatticeSchemaComplianceReport> ScanComplianceAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        await _authorizer.AuthorizeReadAsync(treeId, cancellationToken).ConfigureAwait(false);
        return await _compliance.ScanComplianceAsync(treeId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<LatticeSchemaCapabilities> ProbeCapabilitiesAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        // The gate exposes two schema capabilities for a tree: ordinary Read
        // authority (inspect verbs and the compliance audit) and SchemaAdmin
        // authority (policy / version / remediation mutations). Probe both with no
        // side effects, then map each control operation onto the grant it requires.
        var canRead = await _authorizer.IsReadAuthorizedAsync(treeId, cancellationToken).ConfigureAwait(false);
        var canManage = await _authorizer.IsManageAuthorizedAsync(treeId, cancellationToken).ConfigureAwait(false);

        return new LatticeSchemaCapabilities
        {
            TreeId = treeId,
            CanViewPolicy = canRead,
            CanViewDeadLetters = canRead,
            CanViewVersionConfig = canRead,
            CanViewRemediationStatus = canRead,
            CanScanCompliance = canRead,
            CanManagePolicy = canManage,
            CanManageVersion = canManage,
            CanRemediate = canManage,
        };
    }

    private ILatticeSchemaVersionAdmin RequireVersionAdmin() =>
        _versionAdmin ?? throw new InvalidOperationException(
            "Schema versioning is not registered. Call AddLatticeSchemaVersioning(...) on the silo before " +
            "using version operations on the schema control API.");
}
