# Data Engineering Team

Team members:
- Wanyi
- Jane
- Jack
- Tony
- Mary

Business Info:\
the 5 pipelines provides key metrics for engineers and investors to gauge the financial and operational situation of users of the application product in the company.

## 5 Pipelines

1. Profit
- Metric: profit (revenue - costs) 

2. Growth
- Metric: Daily growth = current day's users / previous day's users
- Purpose: needed for experiments

3. Engagement
- Metric: user engagement (users' interaction involved in the product)
- Purpose: report to investors

4. Aggregation pipeline to investors
- Metric 1: Aggregated growth 
- Metric 2: Aggregated profit 
- Metric 3: Aggregated engagement
- Purpose: report to investors

5. Aggregation pipeline to experiment team
- Metric 1: Unit-level profit (profit / # of users)
- Metric 2: Daily growth (current day's users / previous day's users)
- Purpose: needed for experiments

## Runbook

First Owner: Wanyi\
Second Owner: Jane

**Pipeline - Profit**
1. Pipeline name: Profit
2. Types of data: 
- revenue from user accounts
- advertisement revenue
3. Owners: Finance team\
Secondary owner: data engineering team
4. Common issues:
Numbers can be not aligned with what on the accountant side. When this happens, numbers need to be verified with accountants.
5. SLA's:
- latest daily data can be arrived late by 4 hours after midnight.
- Numbers will be reviewed once a month by finance team.
6. Oncall schedule:
- Monitored by BI analysts in finance team.
- On holidays: One ocall data engineer  needs to be assigned. 

**Pipeline - Growth**
1. Pipeline name: Growth
2. Types of data: 
- Changes of newly-registered user #.
- changes of users # who start to subscribe.
- changes of users # who stop subscribing.
3. Owners:
Accounts team.\
Secondary owner: data engineering team
4. Common issues:
data will contained latest 
5. SLA's:
- Some previous-step records can be missing.
6. Oncall schedule:
- No oncall during after-work hour. Any bug will be reviewed in daytime working hours.
- On holidays: One oncall data engineer  needs to be assigned. 

**Pipeline - Engagement**
1. Pipeline name: Engagement
2. Types of data:
- User clicks and other interaction in the app.
3. Owners: Software Frontend team\
Secondary owner: data engineering team
4. Common issues:
- data associated with clicks can arrive at Kafka extremely late.
- Kafka can go down, which cause data not sent to the pipeline.
- same events can come through the pipeline multiple time. So dedup data.
5. SLA's:
- Issues will be fixed in one week.
6. Oncall schedule:
- one person on DE team will be oncall for each week.
- Software Frontend team should have a person as POC for questions in working hours.
- Next week: 30 min meeting to transfer onboarding to next oncall person.
- On holidays: One data engineer and one software engineer need to be assigned. 

**Pipeline - Aggregation pipeline to investors**
1. Pipeline name: Aggregation pipeline to investors
2. Types of data:
- aggregated growth/profit/engagement metrics by different dimention for the month.
3. Owners: Business Analytics team.\
Secondary owner: data engineering team
4. Common issues:
- Spark can have OOM issues when data volume peaks and table joins.
- Issues with stale data of previous pipeline - backfill needed.
- Missing data can raise divided-by-0 issue or NA values.
5. SLA's:
- Issues will be fixed at end of the month, before the analytics report hanging to executives and investors.
6. Oncall schedule:
- At end of each month, one data engineer should be assigned to review the pipeline health.
- BA team should assign a person as POC to answer question from DE side.

**Pipeline - Aggregation pipeline to experiments**
1. Pipeline name: Aggregation pipeline to experiments
2. Types of data:
- Unit-level profit
- Daily growth
- Daily engagement
3. Owners: Business Analytics team.\
Secondary owner: data engineering team
4. Common issues:
- Spark can have OOM issues when data volume peaks and table joins.
- Issues with stale data of previous pipeline - backfill needed.
- Missing data can raise divided-by-0 issue or NA values.
5. SLA's:
- data associated with clicks can arrive at Kafka extremely late.
- Kafka can go down, which cause data not sent to the pipeline.
6. Oncall schedule:
- one data engineer should be assigned to review the pipeline health.
- Software Frontend team should assign a person as POC to answer question from DE side.