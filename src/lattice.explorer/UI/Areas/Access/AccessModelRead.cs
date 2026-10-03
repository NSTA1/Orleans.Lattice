using Orleans.Lattice.Api.Auth;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// One read of the cluster's access model: the model, or, when there is none,
/// whether the caller was refused it. A refusal says nothing about the cluster's
/// identity directory - only that this caller may not search it - so a picker
/// must not read it as "no directory".
/// </summary>
/// <param name="Model">The access model, or <see langword="null"/> when it could not be read.</param>
/// <param name="Denied">Whether the cluster refused the caller the access model.</param>
internal readonly record struct AccessModelRead(AccessModelDescriptor? Model, bool Denied);
