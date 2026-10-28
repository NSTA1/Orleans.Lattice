---
title: Agent Docs
agent_spec: "docs/agents/index.json"
---

# Agent Docs

Orleans.Lattice is documented twice. The pages on this site are written for
people. Beside them is a set of machine-readable specifications written for
agents that operate a Lattice deployment: an agent that calls its APIs or MCP
tools, or runs a procedure such as a backup, a restore or a reshard. This page
is the way into that set for both of them: a person can see what it holds, and
an agent can find where to start.

The specifications are YAML and JSON files, published as they are rather than
rendered as pages. Each one is listed below with a link to the raw file.

An agent changing the Orleans.Lattice repository itself, rather than operating a
deployment, needs the repository's
[AGENTS.md](https://github.com/NSTA1/Orleans.Lattice/blob/main/AGENTS.md)
instead: its build and test commands, conventions and hygiene gates.

## Where an agent starts

1. **The manifest**, [docs/agents/index.json](docs/agents/index.json). It lists
   every specification with its kind, format, schema id and a one-line summary;
   it names, under `coverage`, the API surfaces that have no file of their own;
   and it states the conventions every specification follows: identifiers,
   source references, uncertainty markers, and the availability rule below.
2. **[llms.txt](llms.txt)**, the index of every page of this site, which lists
   the same specifications under "Agent specifications".
3. **Any page of this site.** Each page's head carries a
   `rel="describedby"` link to the specification that describes it, or to the
   manifest when the page names none. A page that names its specification also
   carries it as `agent_spec` in the front matter of its markdown alternate.

## What a host actually offers

The specifications describe every surface Lattice ships, but a host exposes only
the packages it registers. A surface or tool missing from a host is host
configuration, not a fault. Before relying on a surface, an agent reads its
`availability`; it confirms an MCP tool by finding it in the session's
`tools/list`, which is further filtered to the caller's effective permissions;
and it never emulates an unavailable operation through a different surface.
Every procedure's first precondition checks that its surface is available.

## How the specifications are kept honest

- **Each one has a schema.** Every kind of specification has a JSON Schema
  (draft 2020-12) in the set, and every file names the schema id it follows.
- **Each claim names its source.** Entries cite the code that backs them as
  `repo/relative/path#Symbol`, resolved against the repository at the ref the
  site was built from. A claim the source cannot settle is marked `uncertain`,
  with the reason and the source that would settle it, rather than guessed.
- **They are deterministic.** No timestamps, commit hashes or release numbers;
  entity lists are sorted by id, apart from lists whose order means something,
  such as the steps of a procedure.
- **The build checks them.** The repository's consistency tests check that the
  manifest lists every file and nothing else, that every file matches its
  schema, and that every cross-reference resolves. The site build fails if a
  file is missing from the manifest or would render as a page.

## The specifications

<!-- lattice:agent-specs -->
