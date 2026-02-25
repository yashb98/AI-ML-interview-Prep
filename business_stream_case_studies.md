# Business Stream Data Scientist Interview: 13 Advanced Business Case Studies
## Strategos Presents: The Boardroom Crucible Series

---

## INTRODUCTION: THE TYRANNY OF THE QUARTERLY REPORT

Welcome to the Business Stream Data Scientist interview. These case studies are designed to test not just your technical prowess, but your ability to navigate the complex intersection of data science, utilities operations, regulatory compliance, and stakeholder management. 

Business Stream operates in a sector where a single burst pipe can cost millions, where OFWAT watches every penny, and where "customer" means everything from a corner shop to a multinational hospital trust. The successful candidate must demonstrate they can translate data into decisions—and decisions into pounds saved and customers retained.

**Candidate Context**: Yash B. brings strong technical skills and retail analytics experience but limited utilities domain knowledge. These questions test domain adaptation capabilities.

---

## QUESTION 1: THE £5 MILLION LEAKAGE DETECTION GAMBIT

### Scenario Context

Business Stream is considering a £5 million investment in an AI-powered leakage detection system. The system promises to reduce leakage by 8-12% across the network. Current annual leakage costs are estimated at £18 million (including water loss, repair costs, regulatory penalties, and reputational damage). The system requires:
- £3.5M upfront for sensors and infrastructure
- £1M for AI platform implementation
- £500K annual operating costs
- 18-month implementation timeline

Your task: Build the business case for the Executive Committee.

### The Strategic Challenge

How do you structure a compelling business case that accounts for:
1. Uncertain leakage reduction percentages (8-12% range)
2. Difficulty quantifying reputational damage
3. OFWAT's increasing pressure on leakage targets
4. Competing capital projects (smart metering, customer portal, workforce management)
5. The "hockey stick" problem—benefits don't materialize until Year 2

### Strong Answer Framework

**1. Risk-Adjusted ROI Analysis**
- Present three scenarios: Conservative (8%), Base (10%), Optimistic (12%)
- Calculate NPV for each scenario using appropriate discount rate (typically 7-10% for utilities)
- Include sensitivity analysis on key variables
- Show payback period under each scenario

**2. Beyond Direct Cost Savings**
- OFWAT leakage target compliance: Quantify penalty avoidance
- Reputational risk: Use proxy metrics (customer complaints, media mentions, TrustPilot scores)
- Insurance premium reductions from proactive risk management
- Employee safety: Reduced emergency callouts in dangerous conditions

**3. Strategic Alignment**
- Map to Business Stream values: "Dependable" (reliable supply), "Progressive" (AI adoption)
- Competitor analysis: What are other water retailers doing?
- Future-proofing: How does this position us for PR24 (next price review)?

**4. Implementation Risk Mitigation**
- Phased rollout plan with decision gates
- Pilot program in high-leakage area to validate assumptions
- Vendor performance guarantees and SLAs
- Contingency budget (typically 15-20%)

### Key Insights Interviewers Want to See

| Insight | Why It Matters |
|---------|---------------|
| Understanding of PR24 implications | Shows regulatory awareness |
| Recognition of leakage as both cost AND reputational issue | Demonstrates business breadth |
| Risk-adjusted thinking over point estimates | Shows maturity in financial modeling |
| Stakeholder-specific messaging | Can tailor to Finance (ROI), Operations (efficiency), Compliance (regulatory) |
| Phased approach with kill criteria | Shows pragmatism and risk management |

### Common Mistakes Candidates Make

1. **Single-point estimates**: "We'll save £1.8M" without acknowledging uncertainty
2. **Ignoring implementation timeline**: Not accounting for 18-month delay in benefits
3. **Omitting regulatory context**: Failing to mention OFWAT leakage targets
4. **Purely technical focus**: Getting lost in algorithm details rather than business outcomes
5. **No competitor analysis**: Not knowing what Thames Water, Anglian, etc. are doing
6. **Missing ongoing costs**: Forgetting annual operating costs in year 2+

### Competency Mapping
- **Making Things Happen**: Building structured business case with clear recommendations
- **Accuracy/Analysis/Decision Making**: Risk-adjusted financial modeling
- **Customer Focus**: Understanding impact on service reliability
- **Purposeful**: Alignment with regulatory and strategic objectives

---

## QUESTION 2: THE CUSTOMER SATISFACTION CONUNDRUM

### Scenario Context

Business Stream's customer satisfaction (CSAT) scores have plateaued at 72% for 18 months, below the industry average of 78%. The CEO has challenged the COO function to achieve 85% CSAT within 24 months. You've been asked to define the KPI framework and measurement strategy.

Current state:
- CSAT measured via quarterly email survey (12% response rate)
- NPS collected annually (18% response rate)
- Complaint resolution time: 5.2 days average
- First-contact resolution rate: 43%
- Customer churn: 8% annually (vs. 5% industry best)

### The Strategic Challenge

Design a comprehensive KPI framework that:
1. Moves beyond vanity metrics to actionable insights
2. Enables early warning before satisfaction drops
3. Differentiates between customer segments (SME vs. Enterprise)
4. Balances leading and lagging indicators
5. Accounts for survey bias and low response rates
6. Links satisfaction metrics to financial outcomes

### Strong Answer Framework

**1. Multi-Layer KPI Architecture**

```
STRATEGIC (Board Level)
├── Customer Lifetime Value (CLV) trend
├── Net Revenue Retention (NRR)
└── Brand sentiment index (social + survey)

TACTICAL (Management Level)
├── NPS by segment (quarterly)
├── Customer Effort Score (CES) - post-interaction
├── First Contact Resolution (FCR) rate
└── Complaint-to-compliment ratio

OPERATIONAL (Team Level)
├── Average handle time (with quality weighting)
├── Response time SLA compliance
├── Escalation rate
└── Callback rate (same issue within 30 days)
```

**2. Leading vs. Lagging Indicators**

| Lagging (Outcome) | Leading (Predictive) |
|-------------------|---------------------|
| CSAT score | Call sentiment analysis (AI) |
| Churn rate | Login frequency drop |
| NPS | Support ticket volume spike |
| Complaint volume | Bill query patterns |

**3. Addressing Survey Bias**
- Implement transactional surveys (post-interaction, not quarterly batch)
- Use SMS for immediate feedback (higher response rates)
- Weight results by customer value and engagement level
- A/B test survey timing and channels
- Apply statistical correction for non-response bias

**4. Segment-Specific Metrics**

| Segment | Primary KPI | Rationale |
|---------|-------------|-----------|
| Enterprise (500+ sites) | Dedicated account manager satisfaction | Relationship-driven |
| Mid-Market (50-499) | Self-service portal adoption | Efficiency-focused |
| SME (1-49) | Bill clarity score | Price-sensitive |
| Public Sector | Compliance reporting satisfaction | Regulation-heavy |

**5. Financial Linkage Model**
- Correlate satisfaction scores with retention probability
- Model impact of 1-point CSAT improvement on churn
- Calculate cost of customer acquisition vs. retention
- Build satisfaction-to-revenue bridge for board reporting

### Key Insights Interviewers Want to See

1. **Understanding that CSAT is a symptom, not a diagnosis**: Need operational metrics to understand WHY
2. **Recognition of survey methodology flaws**: Low response rates create biased samples
3. **Segment differentiation**: One size does not fit all in B2B utilities
4. **Leading indicator focus**: Want to predict problems, not just report them
5. **Financial linkage**: Satisfaction must ultimately drive business outcomes

### Common Mistakes Candidates Make

1. **KPI soup**: Listing 50+ metrics without hierarchy or focus
2. **Ignoring response bias**: Treating 12% survey response as representative
3. **No financial linkage**: Metrics without business impact
4. **One-size-fits-all**: Same metrics for corner shop and NHS trust
5. **Lagging indicator obsession**: Only measuring what already happened
6. **No baseline or target**: Metrics without context

### Competency Mapping
- **Improving Service**: Designing metrics that drive service improvement
- **Accuracy/Analysis**: Statistical rigor in survey methodology
- **Customer Focus**: Segment-specific understanding
- **Collaboration**: Cross-functional KPI ownership

---

## QUESTION 3: THE PREDICTIVE MAINTENANCE PARADOX

### Scenario Context

Your team has developed a machine learning model that predicts pump failures 14 days in advance with 87% precision and 82% recall. The Operations Director is skeptical—"We've managed fine with scheduled maintenance for 30 years."

Current maintenance approach:
- Time-based preventive maintenance every 6 months
- Average pump lifespan: 8 years
- Emergency repair cost: £15,000 per incident
- Scheduled maintenance cost: £3,500 per pump
- Average 23 emergency failures annually across 450 pumps
- Pump replacement cost: £45,000

Model deployment costs:
- Integration: £180,000
- Annual licensing: £45,000
- Training and change management: £75,000
- Ongoing data infrastructure: £25,000/year

### The Strategic Challenge

Calculate and present the ROI case for predictive maintenance, addressing:
1. The precision/recall trade-off in business terms
2. Hidden costs of both approaches (opportunity cost, customer impact)
3. Risk of model failure (false negatives = catastrophic)
4. Cultural resistance from operations teams
5. How to structure a pilot to prove value

### Strong Answer Framework

**1. The Mathematics of Model Performance**

With 450 pumps and 23 annual failures (5.1% failure rate):
- True Positives: 23 × 0.82 = 19 pumps correctly flagged
- False Negatives: 23 - 19 = 4 pumps missed (EMERGENCY!)
- False Positives: (450-23) × (1-0.87) = 56 false alarms

**Cost Analysis:**

| Scenario | Annual Cost |
|----------|-------------|
| Current (time-based) | £1,575,000 (450 × £3,500) + £345,000 (23 × £15,000) = £1,920,000 |
| Predictive (ideal) | £665,000 (19 × £3,500 early) + £60,000 (4 × £15,000) + £196,000 (56 × £3,500) = £921,000 |

*Note: False positives still incur maintenance cost*

**2. The Hidden Cost Ledger**

| Cost Category | Time-Based | Predictive |
|---------------|------------|------------|
| Direct maintenance | High | Lower |
| Emergency response | High | Lower |
| Customer disruption | High (unplanned) | Lower (planned) |
| Regulatory reporting | Standard | Enhanced |
| Staff overtime | High (emergency) | Lower (scheduled) |
| Reputation risk | Higher | Lower |

**3. Risk-Adjusted ROI Calculation**

Year 1:
- Implementation: -£300,000
- Savings: £921,000 - £1,920,000 = £999,000
- Net: £699,000
- ROI: 233%

Year 2+:
- Ongoing costs: £70,000
- Savings: £999,000
- Net: £929,000/year

**4. The False Negative Problem**

Four pumps will still fail unexpectedly. Mitigation:
- Critical pumps get redundant monitoring
- Maintain 10% time-based buffer for highest-risk assets
- Emergency response protocol for model misses
- Continuous model improvement loop

**5. Pilot Structure**

Phase 1 (Months 1-3): 
- 50 pumps, shadow mode (model runs but doesn't drive decisions)
- Validate predictions against actual outcomes

Phase 2 (Months 4-6):
- Model drives maintenance scheduling for pilot group
- Compare costs to control group

Phase 3 (Months 7-9):
- Expand to 150 pumps if Phase 2 successful
- Refine based on operational feedback

Go/No-Go Decision at Month 9

### Key Insights Interviewers Want to See

1. **Precision/recall business translation**: Understanding that 87% precision means 56 unnecessary maintenance events
2. **False negative risk management**: Not ignoring the 4 pumps that will still fail
3. **Hidden cost awareness**: Customer disruption, overtime, reputation
4. **Pilot discipline**: Structured validation before full deployment
5. **Cultural sensitivity**: Acknowledging 30 years of "the old way"

### Common Mistakes Candidates Make

1. **Ignoring false positives**: "87% precision is great!" without calculating 56 false alarms
2. **No risk mitigation**: Pretending model will catch everything
3. **Overlooking change management**: Technical solution without adoption plan
4. **Static analysis**: Not showing Year 2+ benefits
5. **Missing regulatory angle**: Not mentioning OFWAT service standards
6. **Binary thinking**: All-or-nothing rather than hybrid approach

### Competency Mapping
- **Making Things Happen**: Structured pilot and rollout plan
- **Accuracy/Analysis**: Rigorous financial modeling with uncertainty
- **Collaboration**: Working with skeptical Operations Director
- **Improving Service**: Better customer outcomes through proactive maintenance

---

## QUESTION 4: THE SKEPTICAL EXECUTIVE DILEMMA

### Scenario Context

The Finance Director has publicly questioned the value of the data science function: "I see pretty dashboards, but where's the P&L impact?" She's blocked approval for three data projects this quarter. The COO has asked you to prepare a 15-minute presentation for the next Executive Committee meeting to address her concerns.

Your team's track record:
- 6 dashboards deployed (adoption: 35% of target users)
- 2 predictive models in production (one showing value, one underperforming)
- 1 automated report saving 20 hours/week
- Several "interesting insights" that didn't lead to action

The Finance Director's background: 20 years in utilities finance, MBA, skeptical of "tech hype," data-literate but not data-native.

### The Strategic Challenge

Design a compelling narrative that:
1. Addresses her specific concerns with concrete evidence
2. Reframes data science from "cost center" to "profit enabler"
3. Establishes credibility without being defensive
4. Proposes a governance model that gives her confidence
5. Secures approval for at least one blocked project

### Strong Answer Framework

**1. The Opening: Acknowledge and Reframe**

"You're right to ask tough questions. Data science should be held to the same ROI standards as any other investment. Let me show you where we've delivered—and where we need to improve."

**2. The Evidence Wall (P&L Impact)**

| Initiative | Investment | Return | Status |
|------------|------------|--------|--------|
| Automated billing reports | £15K | £52K/year (FTE savings) | ✅ Delivered |
| Churn prediction model | £45K | £180K/year (retention) | ✅ Delivered |
| Demand forecasting | £60K | Under review | ⚠️ Investigating |
| Leakage detection POC | £25K | TBD (pilot phase) | 🔄 In Progress |

**3. The Dashboard Adoption Problem**

"Our 35% adoption rate is unacceptable. Here's our diagnosis and fix:"
- Root cause: Dashboards built without user input
- Solution: Embedded analysts in business units for 3 months
- New target: 70% adoption within 6 months
- Accountability: Adoption metrics in our quarterly review

**4. The Governance Model**

```
PROJECT PROPOSAL
       ↓
[Business Case Required]
       ↓
    ┌──┴──┐
  Quick   Strategic
  Win     Initiative
  (<£50K) (≥£50K)
    ↓       ↓
  COO     ExCo
Approval  Vote
    ↓       ↓
30-60-90  Quarterly
Day Reviews Reviews
```

**5. The Ask (Specific and Justified)**

"We're requesting approval for the customer segmentation project—blocked in March. Here's why it meets your criteria:"
- Business case: £320K NPV over 3 years
- Risk: Low (proven methodology, external benchmarks)
- Alignment: Supports your cost-to-serve reduction initiative
- Accountability: Monthly steering committee with Finance representative

**6. The Commitment**

"By Q3, we will:
- Report P&L impact of all live initiatives
- Achieve 70% dashboard adoption
- Kill or fix the underperforming model
- Present 2 new proposals with full business cases"

### Key Insights Interviewers Want to See

1. **No defensiveness**: Acknowledging legitimate concerns builds credibility
2. **Financial fluency**: Speaking in NPV, ROI, P&L terms
3. **Self-awareness**: Admitting failures (adoption, underperforming model)
4. **Governance proposal**: Addressing root cause of skepticism (lack of oversight)
5. **Specific ask**: Not vague "support data science" but concrete project approval
6. **Accountability**: Willing to be measured and to kill failing initiatives

### Common Mistakes Candidates Make

1. **Becoming defensive**: "She just doesn't understand data science"
2. **Technical jargon**: Talking about algorithms instead of business outcomes
3. **Vague promises**: "Data science will transform the business"
4. **Ignoring adoption**: Focusing on deployment, not usage
5. **No governance**: Expecting trust without oversight mechanism
6. **Us-vs-them framing**: Creating adversarial dynamic

### Competency Mapping
- **Communicating with Impact**: Tailored messaging for Finance audience
- **Collaboration**: Working with skepticism, not against it
- **Making Things Happen**: Structured approach to securing approval
- **Developing Self & Others**: Learning from feedback

---

## QUESTION 5: THE OFWAT COMPLIANCE LABYRINTH

### Scenario Context

Business Stream must submit its annual regulatory return to OFWAT in 90 days. The return requires:
- Water consumption data by customer segment (450,000 accounts)
- Leakage metrics with supporting evidence
- Customer service performance against guaranteed standards
- Financial data on cost allocation
- New this year: Carbon reporting for Scope 1, 2, and 3 emissions

Your data team has identified issues:
- 12% of consumption records have data quality flags
- Leakage calculation methodology differs between regions
- Customer service data is spread across 3 systems
- Carbon data is largely estimated, not measured
- Previous year's submission had 47 data corrections post-submission

### The Strategic Challenge

Develop a strategy to deliver an accurate, defensible regulatory submission while:
1. Managing the 90-day timeline
2. Addressing data quality issues without delaying submission
3. Building capability for future compliance (not just this year)
4. Balancing accuracy with pragmatic estimation where appropriate
5. Creating audit trail for OFWAT scrutiny

### Strong Answer Framework

**1. The Triage Framework**

```
DATA QUALITY ASSESSMENT
         ↓
    ┌────┼────┐
 Critical Major Minor
  (Stop) (Fix) (Note)
    ↓     ↓      ↓
 Must    Prioritize Document
 resolve  for fix  & explain
```

Critical (blocks submission):
- Missing consumption for >5% of revenue
- No leakage data for entire region
- Financial data gaps

Major (affects accuracy):
- Inconsistent methodology
- Estimated carbon data
- System integration gaps

Minor (disclosure items):
- Formatting inconsistencies
- Historical corrections
- Documentation gaps

**2. The 90-Day Battle Plan**

| Week | Activity | Deliverable |
|------|----------|-------------|
| 1-2 | Data quality audit | Issue register with severity |
| 3-4 | Critical issue resolution | Clean core datasets |
| 5-8 | Calculation and validation | Draft submission |
| 9-10 | Internal review and stress-test | Reviewed submission |
| 11-12 | Final preparation and submission | OFWAT return |
| 13 | Buffer for corrections | Final submission |

**3. The Estimation Strategy**

For data that cannot be fully resolved:
- Document estimation methodology
- Apply sensitivity analysis (best/worst case)
- Disclose confidence intervals
- Commit to measurement improvement for next year

Example: Carbon Scope 3
- Current: 80% estimated based on industry benchmarks
- Next year: 60% measured, 40% estimated
- Target Year 3: 90% measured

**4. The Audit Trail**

Every data point must be traceable:
- Source system
- Transformation logic
- Quality flags applied
- Estimation methodology (if applicable)
- Reviewer and approval

**5. The Capability Building Plan**

This year's pain becomes next year's process:
- Automated data quality monitoring
- Standardized leakage methodology across regions
- Integrated customer service data platform
- Carbon measurement infrastructure
- Regulatory submission workflow tool

### Key Insights Interviewers Want to See

1. **Pragmatic triage**: Not trying to fix everything in 90 days
2. **Transparency with regulators**: Better to disclose estimation than hide it
3. **Audit trail discipline**: Understanding OFWAT scrutiny
4. **Future-focused**: Building capability, not just meeting deadline
5. **Risk communication**: Clear escalation path for issues

### Common Mistakes Candidates Make

1. **Perfectionism**: Trying to fix all data quality issues in 90 days
2. **Estimation shame**: Hiding estimation rather than documenting it
3. **No audit trail**: Data without provenance
4. **One-off mindset**: Treating as annual crisis rather than building process
5. **Silent issues**: Not escalating problems early
6. **Ignoring carbon**: Treating new requirement as checkbox exercise

### Competency Mapping
- **Accuracy/Analysis**: Rigorous data quality assessment
- **Making Things Happen**: Structured 90-day plan
- **Improving Service**: Building long-term capability
- **Purposeful**: Understanding regulatory importance

---

## QUESTION 6: THE CUSTOMER SEGMENTATION IMPERATIVE

### Scenario Context

Business Stream serves 450,000 business customers ranging from single-site corner shops to multinational corporations with 1,000+ sites. Currently, customers are segmented only by consumption volume:
- Small: <£500/year (78% of customers, 12% of revenue)
- Medium: £500-£10,000/year (19% of customers, 31% of revenue)
- Large: >£10,000/year (3% of customers, 57% of revenue)

The CCO believes this misses critical behavioral and needs-based differences. She's asked you to design a more sophisticated segmentation to enable:
- Targeted retention campaigns
- Personalized service offerings
- Pricing optimization
- Proactive churn prevention

### The Strategic Challenge

Design a customer segmentation strategy that:
1. Goes beyond simple volume-based tiers
2. Is actionable for marketing, service, and sales teams
3. Can be operationalized with available data
4. Enables measurable business outcomes
5. Accounts for the "long tail" of small customers cost-effectively

### Strong Answer Framework

**1. The Multi-Dimensional Segmentation Model**

```
                    VALUE (Revenue + Growth Potential)
                    High          Low
                 ┌─────────┬─────────┐
         High    │ STRATEGIC│ GROWTH  │
   NEEDS         │  (VIP)   │ (Nurture)│
 COMPLEXITY      ├─────────┼─────────┤
                 │ EFFICIENCY│ SELF-  │
         Low     │  (Stream) │ SERVICE│
                 │           │ (Digital)│
                 └─────────┴─────────┘
```

**2. Segment Definitions and Data Requirements**

| Segment | Definition | Data Inputs | Size |
|---------|------------|-------------|------|
| Strategic | High value, complex needs | Revenue >£100K, multi-site, regulated sector | 150 accounts |
| Growth | Lower value, high potential | Revenue £10-100K, growth trend, expansion signals | 2,000 accounts |
| Efficiency | High value, simple needs | Revenue >£50K, single-site, stable consumption | 1,500 accounts |
| Self-Service | Lower value, simple needs | Revenue <£10K, digital-native, low touch | 446,350 accounts |

**3. Behavioral Signals for Classification**

```python
# Example scoring model
strategic_score = (
    revenue_weight * log(annual_revenue) +
    complexity_weight * (num_sites + regulatory_flag) +
    engagement_weight * (contact_frequency + support_tickets) +
    potential_weight * (industry_growth_rate + expansion_signals)
)
```

**4. The Long Tail Solution**

For 446K small customers, manual segmentation is impossible:
- Automated classification using ML model
- Self-service portal as primary channel
- Trigger-based interventions (payment issues, usage spikes)
- Community support and chatbot for queries
- Annual review for upward mobility signals

**5. Actionable Playbooks by Segment**

| Segment | Retention Strategy | Service Model | Pricing Approach |
|---------|-------------------|---------------|------------------|
| Strategic | Dedicated account manager, executive relationship | White-glove, 24/7, proactive | Custom contracts, multi-year |
| Growth | Quarterly business reviews, expansion incentives | High-touch digital + phone | Competitive, usage-based |
| Efficiency | Self-service excellence, automation rewards | Digital-first, chat support | Standard, auto-pay discount |
| Self-Service | App-based engagement, community | Fully digital, AI support | Transparent, no-frills |

**6. Measurement Framework**

| KPI | Strategic | Growth | Efficiency | Self-Service |
|-----|-----------|--------|------------|--------------|
| Retention Rate | 98% | 92% | 88% | 82% |
| CSAT | 90+ | 85+ | 80+ | 75+ |
| Cost to Serve | £2,000/year | £500/year | £150/year | £15/year |
| Revenue Growth | 5% | 15% | 2% | N/A |

### Key Insights Interviewers Want to See

1. **Multi-dimensional thinking**: Beyond just revenue
2. **Actionability**: Segments must drive different treatment
3. **Operational reality**: Can be implemented with available data
4. **Long tail pragmatism**: Cost-effective approach for small customers
5. **Measurement discipline**: Clear KPIs for each segment
6. **Dynamic view**: Segments can change over time

### Common Mistakes Candidates Make

1. **Over-segmentation**: Creating 20 segments that are unmanageable
2. **Data fantasy**: Segments requiring data you don't have
3. **No actionability**: Interesting segments but no different treatment
4. **Ignoring cost-to-serve**: Not accounting for service model economics
5. **Static segments**: Not allowing for customer movement
6. **Missing self-service**: Trying to high-touch everyone

### Competency Mapping
- **Customer Focus**: Deep understanding of customer needs
- **Accuracy/Analysis**: Data-driven segmentation model
- **Making Things Happen**: Operationalization plan
- **Collaboration**: Working with marketing, sales, service

---

## QUESTION 7: THE PROJECT PRIORITIZATION MAZE

### Scenario Context

Your data team has capacity for 3 major projects this quarter. You've received 8 proposals:

1. **Churn Prediction 2.0** (£80K): Upgrade existing model with real-time signals
2. **Smart Meter Analytics** (£120K): Analyze smart meter data for usage insights
3. **Automated Regulatory Reporting** (£60K): Reduce OFWAT submission effort by 60%
4. **Customer Lifetime Value Model** (£45K): Enable value-based segmentation
5. **Leakage Detection POC** (£40K): Pilot ML-based leak detection
6. **Demand Forecasting Enhancement** (£55K): Improve accuracy from 85% to 92%
7. **Self-Service Portal Analytics** (£35K): Track and optimize digital journey
8. **Workforce Optimization** (£70K): Optimize engineer scheduling and routing

Constraints:
- Total budget: £200K
- Team capacity: 3 projects
- Strategic priority: Customer retention (CEO priority)
- Compliance deadline: OFWAT submission in 6 months

### The Strategic Challenge

Develop a prioritization framework and recommend which 3 projects to pursue, addressing:
1. Multiple dimensions of value (financial, strategic, risk)
2. Dependencies between projects
3. Resource constraints and sequencing
4. Stakeholder alignment across functions
5. How to handle the "rejected" projects

### Strong Answer Framework

**1. The Prioritization Matrix**

Score each project (1-5) on:
- Financial Impact (NPV/ROI)
- Strategic Alignment (CEO priorities)
- Compliance/Risk (regulatory deadlines)
- Implementation Feasibility (team capability)
- Time to Value (speed of benefit realization)

| Project | Financial | Strategic | Compliance | Feasibility | Speed | TOTAL |
|---------|-----------|-----------|------------|-------------|-------|-------|
| Churn 2.0 | 5 | 5 | 2 | 4 | 4 | 20 |
| Smart Meter | 4 | 3 | 1 | 3 | 2 | 13 |
| Auto Reg | 3 | 4 | 5 | 5 | 5 | 22 |
| CLV Model | 4 | 4 | 1 | 5 | 4 | 18 |
| Leakage POC | 3 | 3 | 3 | 3 | 3 | 15 |
| Demand Forecast | 3 | 2 | 1 | 4 | 3 | 13 |
| Portal Analytics | 3 | 4 | 1 | 5 | 5 | 18 |
| Workforce Opt | 4 | 3 | 1 | 3 | 3 | 14 |

**2. The Recommended Portfolio**

**Project 1: Automated Regulatory Reporting (£60K)**
- Why: Compliance deadline, high feasibility, quick win
- Risk if delayed: OFWAT penalties, reputational damage
- Stakeholder: CFO, Compliance Director

**Project 2: Churn Prediction 2.0 (£80K)**
- Why: CEO priority, highest financial impact, builds on existing
- Synergy: CLV model can use churn predictions
- Stakeholder: CCO, Sales Director

**Project 3: Self-Service Portal Analytics (£35K)**
- Why: Low cost, quick win, supports retention through digital
- Feasibility: High, can run parallel to others
- Stakeholder: Digital Director, CCO

**Total: £175K (within budget, capacity for contingency)**

**3. The Dependency Map**

```
Churn 2.0 ──────┐
                ├──► CLV Model (Phase 2)
Portal Analytics┘

Auto Reg ───────► All future compliance projects

Leakage POC ────► Full Leakage System (Next year)
```

**4. Handling Rejected Projects**

| Project | Status | Next Step |
|---------|--------|-----------|
| Smart Meter | Phase 2 candidate | Revisit after portal analytics proves value |
| CLV Model | Dependent | Queue after Churn 2.0 completes |
| Leakage POC | Strategic reserve | Fast-track if OFWAT tightens leakage targets |
| Demand Forecast | Low priority | Review if accuracy issues cause problems |
| Workforce Opt | Q3 candidate | Revisit with Operations Director input |

**5. The Governance Model**

Monthly steering committee:
- Review progress against milestones
- Reallocate resources if projects finish early/overrun
- Fast-track emergency projects (regulatory changes)
- Kill criteria: Projects exceeding budget/timeline by >20%

### Key Insights Interviewers Want to See

1. **Multi-criteria evaluation**: Not just financial, but strategic and risk
2. **Constraint awareness**: Budget AND capacity limits
3. **Dependency thinking**: Understanding project relationships
4. **Stakeholder consideration**: Who cares about each project
5. **Portfolio balance**: Mix of quick wins and strategic investments
6. **Graceful rejection**: Clear path for deferred projects

### Common Mistakes Candidates Make

1. **Single criterion**: Only picking highest ROI projects
2. **Ignoring constraints**: Selecting 4 projects when capacity is 3
3. **No dependencies**: Missing that CLV needs churn model first
4. **All-or-nothing**: Not having contingency plans
5. **Missing compliance**: Ignoring regulatory deadlines
6. **No governance**: No process for revisiting decisions

### Competency Mapping
- **Making Things Happen**: Structured prioritization process
- **Accuracy/Analysis**: Multi-criteria scoring
- **Collaboration**: Stakeholder alignment
- **Improving Service**: Balancing multiple improvement opportunities

---

## QUESTION 8: THE DASHBOARD ADOPTION CRISIS

### Scenario Context

Your team spent 6 months building a comprehensive operational dashboard for regional managers. It includes:
- Real-time leakage alerts
- Customer complaint trends
- Workforce utilization metrics
- Financial performance by area
- Predictive maintenance schedules

Six weeks post-launch, adoption metrics show:
- Only 28% of target users log in weekly
- Average session duration: 3 minutes
- Most viewed page: "Export to Excel"
- User feedback: "Too complicated," "Doesn't match how I work," "I just ask my analyst"

The COO is questioning the £85K investment and wants a recovery plan in 48 hours.

### The Strategic Challenge

Develop a change management and adoption strategy that:
1. Diagnoses root causes without blame
2. Addresses both technical and behavioral barriers
3. Wins back skeptical users
4. Prevents future adoption failures
5. Demonstrates value to justify the investment

### Strong Answer Framework

**1. The Root Cause Diagnosis**

```
ADOPTION FAILURE ANALYSIS
         ↓
    ┌────┴────┐
 Technical  Behavioral
    ↓          ↓
• Too many    • No training
  metrics     • Fear of
• Poor          transparency
  navigation  • "That's how
• No mobile     I've always
  version       done it"
• Slow load   • No executive
  times         sponsorship
```

**2. The Recovery Plan (90 Days)**

**Phase 1: Emergency Triage (Weeks 1-2)**
- Interview 10 non-users: "What would make this useful?"
- Identify 3 "power users" as champions
- Create simplified "morning briefing" view (5 key metrics)
- Fix critical performance issues

**Phase 2: Co-Design Sprint (Weeks 3-6)**
- Embed analyst with 3 regional managers for 2 weeks each
- Redesign dashboard based on actual workflow
- Create role-specific views (not one-size-fits-all)
- Build mobile-responsive version

**Phase 3: Structured Rollout (Weeks 7-10)**
- Launch "Dashboard 2.0" with fanfare
- Mandatory 30-minute training sessions
- Executive sponsorship: COO references in weekly meetings
- Gamification: Regional leaderboard (opt-in)

**Phase 4: Sustainment (Weeks 11-12)**
- Weekly adoption reports to regional directors
- Monthly "power user" recognition
- Continuous improvement based on feedback
- Kill criteria: If adoption <50% by Week 12, reassess

**3. The Behavioral Change Toolkit**

| Barrier | Intervention |
|---------|--------------|
| "I don't have time" | Morning briefing email with 5 key numbers |
| "It's not my job" | Include dashboard metrics in performance reviews |
| "I don't trust it" | Show data lineage and validation |
| "I prefer Excel" | One-click export with formatting preserved |
| "No one else uses it" | Public leaderboard, executive references |

**4. The Executive Sponsorship Plan**

- COO mentions dashboard in every all-hands
- Weekly email: "Here's what I learned from the dashboard this week"
- Regional directors held accountable for team adoption
- Dashboard metrics included in ExCo presentations

**5. Success Metrics**

| Metric | Current | 30-Day | 60-Day | 90-Day |
|--------|---------|--------|--------|--------|
| Weekly adoption | 28% | 45% | 60% | 75% |
| Avg session duration | 3 min | 5 min | 8 min | 10 min |
| "Export to Excel" % | 85% | 60% | 40% | 25% |
| User satisfaction | 2.1/5 | 3.0/5 | 3.8/5 | 4.2/5 |

**6. Future Prevention**

- User research BEFORE build, not after
- MVP approach: Launch with 20% of features, expand based on usage
- Adoption metrics from Day 1
- Kill criteria defined upfront

### Key Insights Interviewers Want to See

1. **User-centric diagnosis**: Finding out WHY not just WHAT
2. **Co-design approach**: Users must be involved in solution
3. **Executive sponsorship**: Top-down support is critical
4. **Behavioral science**: Understanding resistance to change
5. **Clear metrics**: Defined success with timeline
6. **Willingness to kill**: If it doesn't work, admit it

### Common Mistakes Candidates Make

1. **Blaming users**: "They just don't understand data"
2. **Technical fix only**: Better UI without addressing behavior
3. **No executive sponsorship**: Trying to drive adoption from bottom
4. **Missing mobile**: Ignoring how field managers work
5. **All-or-nothing**: Not having phased approach
6. **No kill criteria**: Throwing good money after bad

### Competency Mapping
- **Improving Service**: User-centered design
- **Collaboration**: Working with resistant users
- **Making Things Happen**: Structured recovery plan
- **Communicating with Impact**: Executive sponsorship

---

## QUESTION 9: THE UNCOMFORTABLE TRUTH

### Scenario Context

Your analysis of customer service data reveals a disturbing pattern: customers in postcodes with higher deprivation indices receive significantly slower response times (2.3 days vs. 1.1 days for affluent areas) and have lower first-contact resolution rates (31% vs. 58%).

The data is statistically significant (p<0.001) and controls for case complexity, time of contact, and channel. You've presented initial findings to your manager, who suggests "we should probably dig deeper before sharing this widely."

The CCO is preparing a presentation to the board on "Our Commitment to Fair Customer Service."

### The Strategic Challenge

Navigate this ethical and professional dilemma:
1. How do you interpret these findings?
2. Who needs to know, and in what order?
3. How do you present this without creating a crisis?
4. What actions should be taken?
5. How do you protect yourself professionally?

### Strong Answer Framework

**1. The Interpretation Framework**

Before concluding discrimination, investigate:
- Staff allocation: Are fewer resources assigned to deprived areas?
- Case types: Different issues requiring different expertise?
- Customer behavior: Repeat calls inflating metrics?
- Infrastructure: Older systems in certain areas?
- Staff experience: Less experienced teams in certain regions?

Hypothesis categories:
- **Intentional bias**: Unlikely but must be ruled out
- **Structural inequality**: Resource allocation, infrastructure
- **Operational factors**: Training, process differences
- **Data artifact**: Measurement bias, categorization issues

**2. The Stakeholder Communication Plan**

```
IMMEDIATE (24 hours)
    ↓
Manager: "We need to validate before wider sharing"
    ↓
Data validation: Check for confounding variables
    ↓
WEEK 1
    ↓
CCO (private): "I've found something that needs investigation"
    ↓
HR/Compliance (if bias suspected): Confidential briefing
    ↓
WEEK 2-3
    ↓
Working group: Operations, HR, Customer Service, Legal
    ↓
Root cause analysis and action plan
    ↓
WEEK 4
    ↓
Board (if material): Via CCO, with remediation plan
```

**3. The Presentation Strategy**

Frame as opportunity, not accusation:

"Our data has revealed a service quality gap we weren't aware of. This is a chance to improve fairness and demonstrate our values. Here's what we know, what we're investigating, and how we'll fix it."

Include:
- Data methodology and limitations
- Multiple hypotheses (not just bias)
- Validation plan
- Proposed actions (regardless of root cause)
- Timeline for resolution

**4. The Action Plan**

Immediate (regardless of root cause):
- Resource rebalancing audit
- Staff training refresh on equitable service
- Customer communication review
- Escalation pathway check

Medium-term (based on findings):
- If structural: Redesign resource allocation model
- If training: Enhanced coaching program
- If infrastructure: System upgrade prioritization
- If bias: Disciplinary and cultural intervention

**5. Professional Protection**

- Document everything: Data, analysis, communications
- Escalate through proper channels
- Don't go rogue (media, whistleblowing) without exhausting internal options
- Seek advice from trusted mentor or legal if blocked
- Maintain professional tone (fact-based, not accusatory)

**6. The Regulatory Consideration**

- OFWAT has customer service standards that may be relevant
- Equality Act 2010 may apply if discrimination proven
- GDPR considerations in data analysis
- Reputational risk if story breaks externally

### Key Insights Interviewers Want to See

1. **Investigative rigor**: Not jumping to conclusions
2. **Ethical courage**: Willing to surface uncomfortable truths
3. **Political awareness**: Understanding organizational dynamics
4. **Solution orientation**: Bringing recommendations, not just problems
5. **Professional protection**: Knowing how to protect yourself
6. **Regulatory awareness**: Understanding legal implications

### Common Mistakes Candidates Make

1. **Immediate escalation**: Going to board/press without investigation
2. **Cover-up**: Accepting manager's suggestion to bury it
3. **Accusatory tone**: Framing as intentional discrimination
4. **No solutions**: Problem without recommendations
5. **Going rogue**: Bypassing proper channels
6. **Ignoring legal**: Not considering regulatory implications

### Competency Mapping
- **Purposeful**: Acting with integrity despite discomfort
- **Accuracy/Analysis**: Rigorous investigation before conclusion
- **Collaboration**: Working across functions to address
- **Communicating with Impact**: Sensitive messaging

---

## QUESTION 10: THE RESOURCE ALLOCATION RIDDLE

### Scenario Context

You have a £150K budget to allocate across four data initiatives. Each has different risk/return profiles:

**Option A: Safe Bet**
- Investment: £150K (all-in)
- Expected NPV: £400K
- Probability of success: 85%
- Timeline: 12 months

**Option B: Balanced Portfolio**
- Investment: £50K each in 3 projects
- Expected NPV: £200K per project (total £600K)
- Probability of success: 60% per project
- Timeline: 9 months
- Note: Projects are independent

**Option C: Moonshot**
- Investment: £150K (all-in)
- Expected NPV: £1.2M
- Probability of success: 25%
- Timeline: 18 months
- If successful: transformational

**Option D: Hybrid**
- Investment: £100K in Safe Bet, £50K in new exploratory
- Expected NPV: £300K (Safe) + £150K (exploratory)
- Probability: 85% (Safe), 40% (exploratory)
- Timeline: Mixed

Additional context:
- Your team has never delivered a project with <60% success probability
- The CEO has expressed interest in "transformational" initiatives
- The CFO prefers predictable returns
- You need to demonstrate value within 12 months for budget renewal

### The Strategic Challenge

Recommend an allocation strategy that:
1. Maximizes expected value while managing risk
2. Balances stakeholder preferences
3. Accounts for team capability and track record
4. Positions you for future budget negotiations
5. Can be clearly articulated and defended

### Strong Answer Framework

**1. The Expected Value Calculation**

| Option | Calculation | Risk-Adjusted NPV |
|--------|-------------|-------------------|
| A | £400K × 0.85 | £340K |
| B | 3 × (£200K × 0.60) | £360K |
| C | £1.2M × 0.25 | £300K |
| D | (£300K × 0.85) + (£150K × 0.40) | £315K |

Pure EV ranking: B > A > D > C

**2. The Risk Analysis**

Probability of total loss:
- A: 15%
- B: 0.4³ = 6.4% (all three fail)
- C: 75%
- D: 15% × 60% = 9% (both fail)

Probability of at least one success:
- B: 1 - 0.4³ = 93.6%
- D: 1 - (0.15 × 0.60) = 91%

**3. The Stakeholder Mapping**

| Stakeholder | Preference | How to Address |
|-------------|------------|----------------|
| CEO | Transformational | Position exploratory as innovation portfolio |
| CFO | Predictable | Lead with risk-adjusted returns |
| COO | Deliverable | Emphasize track record alignment |
| Team | Achievable | Stay within proven success range |

**4. The Recommended Strategy: Modified Option B**

Allocation:
- £60K: Proven initiative (70% success, £250K NPV)
- £50K: Stretch initiative (55% success, £200K NPV)
- £40K: Exploratory (40% success, £150K NPV)

Risk-adjusted NPV: £175K + £110K + £60K = £345K

**Why this works:**
- Highest probability of at least one success (94%)
- Strong risk-adjusted return
- Includes exploratory element for CEO
- Stays within team capability (nothing below 40%)
- 12-month deliverable for budget renewal

**5. The Narrative**

"We're recommending a portfolio approach that balances proven value creation with strategic exploration. This gives us:
- High probability of delivering measurable results
- Optionality for transformational discovery
- Alignment with our demonstrated capabilities
- Strong risk-adjusted returns"

**6. The Governance**

Quarterly review gates:
- Month 3: Progress check, reallocate if needed
- Month 6: Go/kill/hold decision on each project
- Month 9: Preliminary results, budget renewal prep
- Month 12: Final outcomes, next year planning

### Key Insights Interviewers Want to See

1. **Risk-adjusted thinking**: Not just raw NPV
2. **Portfolio approach**: Diversification benefits
3. **Stakeholder balance**: Addressing multiple preferences
4. **Capability awareness**: Not overreaching team track record
5. **Governance discipline**: Clear review points
6. **Clear narrative**: Can explain to non-technical audience

### Common Mistakes Candidates Make

1. **Raw NPV only**: Ignoring probability of success
2. **All-or-nothing**: Not considering portfolio effects
3. **Ignoring track record**: Picking moonshot despite team history
4. **Single stakeholder focus**: Only pleasing CEO or CFO
5. **No governance**: No plan for monitoring and adjustment
6. **Complex explanation**: Can't articulate simply

### Competency Mapping
- **Accuracy/Analysis**: Risk-adjusted financial modeling
- **Making Things Happen**: Structured decision-making
- **Collaboration**: Balancing stakeholder needs
- **Improving Service**: Portfolio of improvement initiatives

---

## QUESTION 11: THE PRESCRIPTIVE ANALYTICS PROOF POINT

### Scenario Context

Business Stream has invested £200K in "prescriptive analytics" capabilities over 18 months. The promise was moving from "what happened" (descriptive) and "what will happen" (predictive) to "what should we do" (prescriptive).

Current capabilities:
- Descriptive: 15 dashboards reporting historical performance
- Predictive: 3 models forecasting demand, churn, and leakage
- Prescriptive: 1 pilot recommending optimal pricing for 500 customers

The CEO is asking for evidence that prescriptive analytics is delivering value beyond what descriptive/predictive could achieve. She's specifically questioning whether the additional investment is justified.

### The Strategic Challenge

Demonstrate the incremental value of prescriptive analytics by:
1. Defining clear metrics that differentiate prescriptive from predictive
2. Showing concrete examples of decisions changed by prescriptive insights
3. Quantifying the business impact of those changed decisions
4. Addressing the "couldn't we have done this with predictive + human judgment?" question
5. Building the case for continued/p expanded investment

### Strong Answer Framework

**1. The Analytics Maturity Value Chain**

```
DESCRIPTIVE        PREDICTIVE         PRESCRIPTIVE
"What happened"    "What will happen" "What should we do"
     ↓                  ↓                    ↓
Revenue dropped   Churn risk: 75%    Offer: 10% discount
15% last quarter  for Customer X     + service upgrade
     ↓                  ↓                    ↓
Value: Awareness  Value: Focus       Value: Action
£10/insight       £50/insight        £500/insight
```

**2. The Prescriptive Differentiation**

| Dimension | Predictive | Prescriptive |
|-----------|------------|--------------|
| Output | Probability | Recommendation |
| Action | Human decides | System suggests optimal action |
| Optimization | Single variable | Multi-variable trade-offs |
| Learning | Model improves | Model + outcomes improve |
| Scale | Analyst-limited | Automated at scale |

**3. The Pricing Pilot Evidence**

**Before (Predictive Only):**
- Model identified 500 at-risk customers
- Account managers chose retention offers
- Results: 45% retention, average discount 18%

**After (Prescriptive):**
- Model recommends optimal offer per customer
- Results: 62% retention, average discount 12%
- Incremental value: £85K annually from pilot group

**Why prescriptive added value:**
- Optimized discount level (not just "give them something")
- Considered customer lifetime value in offer design
- Automated what account managers couldn't scale

**4. The Decision Change Audit**

Survey account managers: "How many decisions changed based on prescriptive recommendation?"
- 73% said at least one decision changed per week
- Average impact: £1,200 per changed decision
- Annualized: £187K from 500-customer pilot

**5. The Counterfactual Analysis**

"Couldn't predictive + human judgment achieve the same?"

Test: Give 100 at-risk customers to experienced account managers WITHOUT prescriptive recommendations
- Results: 48% retention (vs. 62% with prescriptive)
- Average discount: 21% (vs. 12% with prescriptive)
- Conclusion: Experience helps, but optimization adds value

**6. The Scale Argument**

Prescriptive value increases with scale:
- 500 customers: £187K value
- 5,000 customers: £1.5M value (not linear, but significant)
- 50,000 customers: £8M value (automation economies)

Predictive-only approach doesn't scale decision quality.

**7. The Investment Case**

| Investment | Annual Value | ROI |
|------------|--------------|-----|
| £200K over 18 months | £187K (pilot only) | 56% (pilot) |
| Scale to 5,000 customers | £1.5M | 650% |
| Full deployment (50K) | £8M | 3,900% |

### Key Insights Interviewers Want to See

1. **Clear differentiation**: What prescriptive does that predictive cannot
2. **Concrete evidence**: Specific numbers, not theoretical benefits
3. **Counterfactual thinking**: What would happen without prescriptive
4. **Scale argument**: Why automation matters
5. **Decision audit**: Proof that recommendations change behavior
6. **Honest limitations**: Not overselling current capabilities

### Common Mistakes Candidates Make

1. **Vague claims**: "Prescriptive is better" without proof
2. **Ignoring counterfactual**: Not addressing "predictive + human" argument
3. **No decision audit**: Assuming recommendations are followed
4. **Overselling scale**: Projecting value without validation
5. **Technical focus**: Talking about algorithms not outcomes
6. **No learning loop**: Not showing how system improves

### Competency Mapping
- **Accuracy/Analysis**: Rigorous measurement of incremental value
- **Making Things Happen**: Building proof points
- **Communicating with Impact**: Clear differentiation narrative
- **Improving Service**: Demonstrating service improvement through analytics

---

## QUESTION 12: THE CONFLICTING REQUIREMENTS QUAGMIRE

### Scenario Context

Three departments have submitted requirements for the same customer data platform:

**Marketing wants:**
- Real-time customer segmentation updates
- Campaign performance tracking
- Integration with email platform
- Self-service audience building
- Timeline: 6 months

**Customer Service wants:**
- 360-degree customer view
- Interaction history across all channels
- Predictive next-best-action
- Integration with phone system
- Timeline: 4 months

**Finance wants:**
- Accurate revenue attribution
- Cost-to-serve analysis
- Regulatory reporting compliance
- Audit trail for all data
- Timeline: 3 months (regulatory deadline)

Each department has secured budget for their requirements. Each believes their needs are critical. Each has escalated to their respective directors.

### The Strategic Challenge

Develop an approach that:
1. Resolves apparent conflicts without alienating stakeholders
2. Delivers value to all departments within reasonable timelines
3. Builds a unified platform rather than three separate systems
4. Manages expectations and escalations
5. Creates governance to prevent future conflicts

### Strong Answer Framework

**1. The Conflict Analysis**

```
REQUIREMENTS OVERLAP ANALYSIS
         ↓
    ┌────┴────┐
 Genuine    Artificial
 Conflict   Conflict
    ↓          ↓
Technical  Priority/
Trade-off  Resource
    ↓          ↓
Resolve    Negotiate
with       with
architecture stakeholders
```

Actual conflicts:
- Real-time (Marketing) vs. accuracy (Finance)
- Self-service (Marketing) vs. audit trail (Finance)
- Timeline pressures (all three)

Overlap opportunities:
- Customer view needed by all three
- Integration requirements can be shared
- Data foundation is common

**2. The Unified Architecture**

```
┌─────────────────────────────────────────┐
│         PRESENTATION LAYER              │
│  Marketing  │  Customer  │   Finance    │
│  Dashboard  │   Service  │   Reports    │
│             │   Console  │              │
├─────────────────────────────────────────┤
│         APPLICATION LAYER               │
│  Segmentation │ 360 View │  Attribution│
│  Campaign Mgmt│ Next Best│  Cost Model │
├─────────────────────────────────────────┤
│           DATA LAYER                    │
│  Customer Master │ Transaction │ Events │
│  Reference Data  │  History    │ Log    │
├─────────────────────────────────────────┤
│        INTEGRATION LAYER                │
│  Email │ Phone │ ERP │ Regulatory │ etc │
└─────────────────────────────────────────┘
```

**3. The Phased Delivery Plan**

**Phase 1: Foundation (Months 1-3)**
- Data layer: Customer master, transaction history
- Finance requirements: Revenue attribution, audit trail
- Deliverable: Regulatory compliance + data foundation

**Phase 2: Customer View (Months 4-6)**
- 360-degree view for Customer Service
- Interaction history integration
- Next-best-action pilot
- Deliverable: Service console + initial intelligence

**Phase 3: Marketing Intelligence (Months 7-9)**
- Real-time segmentation
- Campaign performance tracking
- Self-service audience builder
- Deliverable: Marketing platform

**Phase 4: Optimization (Months 10-12)**
- Performance tuning
- Advanced analytics
- Cross-functional features
- Deliverable: Fully integrated platform

**4. The Stakeholder Alignment Strategy**

Individual meetings with each director:

**Finance Director:**
- "Your regulatory deadline is protected—Phase 1 priority"
- "Your audit trail becomes foundation for all other uses"
- "Early delivery builds platform for future finance initiatives"

**Customer Service Director:**
- "4-month timeline becomes 6 months, but with richer data"
- "360-view benefits from Finance's data quality work"
- "Pilot program can start Month 5 with real data"

**Marketing Director:**
- "6-month timeline extends to 9, but with production-grade foundation"
- "Self-service built on validated, governed data"
- "Campaign tracking includes full customer journey"

**5. The Governance Model**

Cross-functional steering committee:
- Monthly review of progress and priorities
- Escalation pathway for conflicts
- Change control process
- Shared KPIs (not siloed metrics)

Decision rights:
- Architecture: Data/IT
- Prioritization: Steering committee
- Requirements: Department leads
- Timeline: Collaborative negotiation

**6. The Conflict Resolution Framework**

When conflicts arise:
1. Document both requirements clearly
2. Assess technical feasibility of each
3. Quantify business impact of each
4. Propose alternatives/compromises
5. Escalate to steering committee if unresolved
6. Decision communicated with rationale

### Key Insights Interviewers Want to See

1. **Systems thinking**: Seeing connections, not just conflicts
2. **Architectural approach**: Building unified platform, not point solutions
3. **Stakeholder empathy**: Understanding each department's real needs
4. **Phased delivery**: Sequencing for dependencies and priorities
5. **Governance discipline**: Process for ongoing conflict resolution
6. **Negotiation skill**: Trading timeline for quality/foundation

### Common Mistakes Candidates Make

1. **Technical solution only**: Architecture without stakeholder management
2. **Winner-takes-all**: Picking one department over others
3. **Separate systems**: Building three platforms to avoid conflict
4. **Ignoring timeline pressure**: Not addressing regulatory deadline
5. **No governance**: Solving current conflict without preventing future ones
6. **Top-down mandate**: Forcing solution without buy-in

### Competency Mapping
- **Collaboration**: Working across conflicting stakeholders
- **Making Things Happen**: Structured delivery approach
- **Accuracy/Analysis**: Requirements analysis and trade-offs
- **Communicating with Impact**: Stakeholder alignment

---

## QUESTION 13: THE 90-DAY CEO CHALLENGE

### Scenario Context

You've been in the Data Scientist role for 3 weeks. The CEO calls you into her office:

"I've approved the headcount and budget for this data science function, but I'm getting pressure from the board to show value. I need you to demonstrate tangible business impact from data science within 90 days. Not 'insights'—impact. Something I can put in the quarterly report."

"I know 90 days isn't ideal for building models, so I'm not asking for perfection. I'm asking for proof of concept that justifies continued investment."

"What are you going to deliver, and how will I know it worked?"

### The Strategic Challenge

Design a 90-day plan that:
1. Delivers measurable business impact quickly
2. Sets foundation for longer-term capabilities
3. Manages expectations (what's possible in 90 days)
4. Builds credibility and relationships across the organization
5. Creates a narrative for the quarterly report
6. Positions for Phase 2 expansion

### Strong Answer Framework

**1. The 90-Day Impact Philosophy**

```
QUICK WIN → CREDIBILITY → FOUNDATION → EXPANSION
    ↓           ↓            ↓            ↓
  30 days    60 days      90 days     180 days
  Show       Build        Prove        Scale
  value      trust        concept      impact
```

**2. The Three-Pillar 90-Day Plan**

**PILLAR 1: Immediate Value Extraction (Days 1-30)**

Identify existing data assets that can deliver value without new infrastructure:

| Initiative | Data Required | Expected Impact | Effort |
|------------|---------------|-----------------|--------|
| Billing anomaly detection | Invoice + payment data | Reduce queries 20% | 1 week |
| Customer segmentation | CRM + billing | Target retention campaigns | 2 weeks |
| Report automation | Existing dashboards | Save 15 hrs/week | 1 week |
| Usage pattern analysis | Consumption data | Identify upsell opportunities | 2 weeks |

**Deliverable by Day 30:**
- Automated billing report saving 15 hours/week (£25K annual value)
- Customer segmentation model for retention targeting
- 3 specific upsell opportunities identified

**PILLAR 2: Proof-of-Concept Model (Days 31-60)**

Build one focused predictive model with clear business case:

**Churn Prediction MVP:**
- Scope: Top 1,000 customers by revenue
- Features: Billing patterns, service interactions, contract status
- Output: Risk score + top 3 risk factors
- Validation: Back-test on historical churners

**Deliverable by Day 60:**
- Working model with 75%+ accuracy
- List of 50 highest-risk customers
- Recommended retention actions per customer
- Business case for full deployment

**PILLAR 3: Business Validation (Days 61-90)**

Work with business to validate and measure impact:

- Partner with Customer Service on retention pilot
- Track outcomes of data-driven interventions
- Document lessons learned and refinement needs
- Build business case for Phase 2

**Deliverable by Day 90:**
- Retention pilot results (target: 10% improvement)
- Quantified business impact (£50K+ saved/retained)
- Phase 2 proposal with stakeholder buy-in

**3. The Stakeholder Engagement Plan**

| Week | Activity | Stakeholder |
|------|----------|-------------|
| 1 | Discovery meetings | All department heads |
| 2 | Data inventory | IT, Data Engineering |
| 3 | Quick win delivery | Finance (report automation) |
| 4 | Value communication | CEO, ExCo |
| 5-6 | Model development | Customer Service |
| 7-8 | Pilot design | Customer Service, Sales |
| 9-10 | Pilot execution | Customer Service |
| 11-12 | Results + Phase 2 proposal | CEO, All stakeholders |

**4. The Success Metrics Dashboard**

| Metric | Target | Measurement |
|--------|--------|-------------|
| Time saved (reporting) | 15 hrs/week | Timesheet tracking |
| Retention pilot improvement | 10% | Churn rate comparison |
| Revenue protected | £50K | Customer value retained |
| Stakeholder satisfaction | 4/5 | Survey |
| Model accuracy | 75% | Back-test validation |

**5. The Quarterly Report Narrative**

"In 90 days, our new data science function has:
- Delivered £25K annual savings through automation
- Identified £50K in at-risk revenue with retention plan
- Built predictive capability with 75% accuracy
- Established foundation for expanded Phase 2

This validates our investment and positions us for significant value creation in the next quarter."

**6. The Phase 2 Proposal**

Based on 90-day learnings:
- Scale churn model to full customer base
- Expand to demand forecasting
- Build self-service analytics capability
- Investment request: £XXX for £XXX return

### Key Insights Interviewers Want to See

1. **Pragmatic ambition**: High impact but achievable in 90 days
2. **Existing asset leverage**: Not building from scratch
3. **Business partnership**: Working WITH not FOR business
4. **Measurable outcomes**: Clear metrics, not vague "insights"
5. **Foundation thinking**: Quick wins enable longer-term value
6. **Communication discipline**: CEO-ready narrative

### Common Mistakes Candidates Make

1. **Over-ambition**: Promising production ML model in 90 days
2. **Technical isolation**: Building without business involvement
3. **No quick wins**: Only working on long-term projects
4. **Vague metrics**: "We'll provide insights" without quantification
5. **Ignoring stakeholders**: Not building relationships early
6. **No Phase 2 vision**: 90 days as end state, not foundation

### Competency Mapping
- **Making Things Happen**: Delivering under pressure
- **Customer Focus**: Business impact, not technical elegance
- **Accuracy/Analysis**: Measurable outcomes
- **Communicating with Impact**: CEO-ready narrative
- **Collaboration**: Cross-functional partnership
- **Developing Self & Others**: Building foundation for growth

---

## CONCLUSION: THE BOARDROOM CRUCIBLE

These 13 case studies represent the reality of data science leadership in the utilities sector. Success requires more than technical skill—it demands:

1. **Business fluency**: Speaking the language of P&L, ROI, and regulatory compliance
2. **Stakeholder sophistication**: Navigating competing interests with diplomacy
3. **Ethical courage**: Surfacing uncomfortable truths when necessary
4. **Pragmatic ambition**: Delivering value while building for the future
5. **Systems thinking**: Seeing connections across technical, operational, and strategic domains

The candidate who can demonstrate these capabilities—while showing humility about their utilities domain knowledge gaps and eagerness to learn—will thrive in the Business Stream environment.

Remember: In the boardroom crucible, data science is not about algorithms. It's about decisions. And decisions drive the business forward.

---

*Document prepared by Strategos, Oracle of Business Acumen*
*For Business Stream Data Scientist Interview Preparation*
