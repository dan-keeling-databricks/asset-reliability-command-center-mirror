# Asset Reliability Command Center

Data & AI World Tour  
Manufacturing and energy generation

Real-time pump monitoring with governed autonomous maintenance.

---

## The cost of equipment downtime

Industrial benchmarks show why earlier intervention matters:

- **$50 billion annually:** estimated annual cost of unplanned downtime to industrial
  manufacturers.
- **5-20% of productive capacity:** potential reduction from poor maintenance strategies.
- **27% of industrial-system electricity:** share represented by pumps in manufacturing.

Sources: [Deloitte Insights, 2017](https://www2.deloitte.com/us/en/insights/focus/industry-4-0/using-predictive-technologies-for-asset-maintenance.html)
and [U.S. Department of Energy, 2006](https://www.energy.gov/sites/prod/files/2014/05/f16/pump.pdf).

---

## Autonomous incident review

1. **Detect and assess.** The App checks Lakebase every second. A vibration threshold breach
   creates a contextual prompt. The agent loads the governed reliability skill and estimates
   failure risk before the next maintenance window.
2. **Prepare maintenance.** The agent discovers tools from `asset_reliability_mcp`, gathers
   evidence, and prepares a bearing replacement work order. The activity timeline exposes every
   model and tool event.
3. **Pause for policy.** Unity AI Gateway blocks the work-order call with `ASK`. The current user
   approves in context. The agent resumes, submits the order, and returns to the command center.

A threshold breach starts the workflow. The operator intervenes only when policy requires
approval.

---

## End-to-end Databricks architecture

One telemetry stream supports low-latency operations, governed history, and autonomous action.

```text
Synthetic pump telemetry -> Kafka
    +-> Spark Real-Time Mode -> Lakebase -> Databricks App command center
    |                                      +-> autonomous agent
    |                                          -> UC Skill
    |                                          -> Unity AI Gateway MCP
    |                                          -> ASK approval
    |                                          -> work order
    +-> SQL Lakeflow Declarative Pipeline
        -> Unity Catalog telemetry history
        -> Genie Agent
```

---

## Illustrative site value case

### Assumptions

- 24 critical pumps.
- 8 forced incidents a year.
- 7.5 downtime hours for each incident.
- £80,000 operational impact for each downtime hour.
- Target: 25% less downtime.

### Funding executive

- Baseline exposure: `8 x 7.5 x £80,000 = £4.8M a year`.
- Target exposure: `45.0 x £80,000 = £3.6M a year`.
- Projected annual value: **£1.2M**.

### Reliability and operations leader

- Baseline: **60.0 downtime hours a year**.
- Target: **45.0 downtime hours a year**.
- Capacity recovered: **15.0 hours**.
- Operational target: shift two forced failures into planned work.

These figures are illustrative assumptions for a target site, not observed prototype results.
Replace them with the site's asset register, failure history, downtime duration, and impact per
hour before making an investment decision.

Google Slides source:
<https://docs.google.com/presentation/d/1ZjGVpcxIoZDMIautKpRJVBGNnRTJNg9WKH3mFv0_1kI/edit>

