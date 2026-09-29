using System.Collections.Immutable;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.Shell.Areas.Apps.Catalogue;

/// <summary>One predicted failure, in words.</summary>
/// <param name="Kind">What would fail.</param>
/// <param name="Text">The explanation shown to the operator.</param>
internal sealed record AppActivationIssue(AppActivationIssueKind Kind, string Text);
