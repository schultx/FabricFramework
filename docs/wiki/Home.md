# Monza

Monza is Columbus Data & AI DK's Microsoft Fabric data platform framework: three workspaces per
environment (Data / Ingestion / Code), medallion lakehouses (Landing → Bronze → Silver
*[conditional]* → Gold), a metadata-driven ingestion model where one row in `ingestion.Table`
drives a source table's entire Source → Landing → Bronze flow, one-notebook git-synced
deployment, and Azure DevOps CI/CD. It's original work — the connector-type inventory used FMD
Framework's own list as a reference, but no FMD or AquaVilla code is reused.

This wiki exists for one purpose: **let a consultant who has never seen this repo stand up the
same structure for a new client, quickly, with a consistent, repeatable delivery.**

## Where to start

- **New to this repo and onboarding a client right now?** Start at
  [New-Client-Onboarding](New-Client-Onboarding.md) — the ordered, hands-on runbook.
- **Understanding how it's put together?** [Architecture](Architecture.md) for the workspace/
  lakehouse topology, then [Metadata-Model](Metadata-Model.md) for how ingestion is actually
  driven.
- **Adding a client's source system?** [Connector-Types](Connector-Types.md).
- **How does incremental loading actually work, end to end?** [Incremental-Loading](Incremental-Loading.md).
- **Curious about Materialized Lake Views for dim/fact?** [Materialized-Lake-Views](Materialized-Lake-Views.md) — a forward-looking design showcase, not current-state.
- **Deploying, or setting up WIF / the metadata Connection?** [Deployment-Guide](Deployment-Guide.md).
- **Something broke?** [Operations-Guide](Operations-Guide.md) — a troubleshooting playbook built
  from real incidents hit while building and testing this exact framework, not generic advice.
- **Deciding what to improve next?** [Improvement-Roadmap](Improvement-Roadmap.md) (round 1, completed) and [Improvement-Roadmap-2](Improvement-Roadmap-2.md) (round 2, including governance/security research — not yet actioned).

## Pages

| Page | What's in it |
|---|---|
| [Architecture](Architecture.md) | Workspace topology, medallion lakehouses, folders, naming |
| [Metadata-Model](Metadata-Model.md) | `ingestion.Connection`/`Database`/`Table`, Full/Delta load, watermarking, audit |
| [Incremental-Loading](Incremental-Loading.md) | One continuous walkthrough of a Delta-load table, Landing → Gold |
| [Connector-Types](Connector-Types.md) | All 9 source connector types + how to add a new source table |
| [Materialized-Lake-Views](Materialized-Lake-Views.md) | Design showcase: MLVs for a dim/fact setup (not built today) |
| [Deployment-Guide](Deployment-Guide.md) | How `NB_DEPLOY` works, WIF setup, `metadata_connection_guid` setup |
| [Operations-Guide](Operations-Guide.md) | Running `PL_RUN_ALL`, audit trail, troubleshooting playbook |
| [New-Client-Onboarding](New-Client-Onboarding.md) | The ordered runbook for a brand-new client engagement |
| [Improvement-Roadmap](Improvement-Roadmap.md) | Round 1: 33 findings, all fixed and live-tested |
| [Improvement-Roadmap-2](Improvement-Roadmap-2.md) | Round 2: deeper code review + Fabric governance/security/platform research — not yet actioned |

## Ground truth

This wiki describes the framework as it stands after live end-to-end testing against
development, test, and production in the real tenant — not just a design intent. Where something
is known to be broken, missing, or manual today, the relevant page says so plainly rather than
describing the aspirational version.
