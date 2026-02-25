# Business Stream Data Scientist Interview Questions
## Advanced Machine Learning & Statistical Inference Assessment
### Designed for Senior Data Scientist Role - Utilities Sector

---

## PREAMBLE: The Mathematics Do Not Lie

*"In the regulated utilities sector, our models must not only predict accurately but also explain themselves. Every coefficient, every threshold, every decision boundary carries regulatory and societal implications. Let us derive these questions from first principles."*

---

## QUESTION 1: Time Series Forecasting for Water Demand Prediction

### The Problem

You are tasked with forecasting daily water demand for a region serving 500,000 customers. Your dataset contains 5 years of hourly consumption data. You observe:
- **Intraday seasonality**: Morning and evening peaks
- **Weekly seasonality**: Lower weekend consumption
- **Annual seasonality**: Higher summer demand (gardening, outdoor use)
- **Trend**: Gradual efficiency improvements reducing per-capita consumption
- **External regressors**: Temperature, rainfall, public holidays, COVID-19 lockdown periods

### Part A: Mathematical Foundation

Derive the state-space representation of a SARIMAX(p,d,q)(P,D,Q)s model and explain how you would select the optimal orders using information criteria.

### Expected Answer

The SARIMAX model extends ARIMA with seasonal components and exogenous variables:

$$
\Phi_P(B^s)\phi_p(B)\nabla^d\nabla_s^D y_t = \Theta_Q(B^s)\theta_q(B)\varepsilon_t + \sum_{i=1}^{k}\beta_i x_{i,t}
$$

Where:
- $\phi_p(B) = 1 - \phi_1B - \phi_2B^2 - ... - \phi_pB^p$ (AR polynomial)
- $\theta_q(B) = 1 + \theta_1B + \theta_2B^2 + ... + \theta_qB^q$ (MA polynomial)
- $\Phi_P(B^s), \Theta_Q(B^s)$ are seasonal AR and MA polynomials
- $\nabla^d = (1-B)^d$ is the differencing operator
- $\nabla_s^D = (1-B^s)^D$ is the seasonal differencing operator

**State-Space Representation:**

$$
\begin{aligned}
y_t &= Z\alpha_t + \Gamma x_t + \varepsilon_t, \quad \varepsilon_t \sim N(0, H) \\
\alpha_{t+1} &= T\alpha_t + R\eta_t, \quad \eta_t \sim N(0, Q)
\end{aligned}
$$

**Model Selection via AIC/BIC:**

$$
AIC = 2k - 2\ln(\hat{L}), \quad BIC = k\ln(n) - 2\ln(\hat{L})
$$

For water demand, we prefer BIC due to its consistency property—critical when models inform infrastructure investment decisions.

### Part B: Practical Application

**How would you handle the following real-world complexities?**

1. **Multiple seasonalities** (daily + weekly + annual): Standard SARIMAX assumes single seasonality. Would you use:
   - TBATS (Trigonometric seasonality, Box-Cox transformation, ARMA errors, Trend, Seasonal components)?
   - Prophet with custom seasonalities?
   - Deep learning (LSTM/Temporal Fusion Transformers)?

2. **Temperature-demand relationship is non-linear**: Demand increases non-linearly above 20°C (gardening). How would you model this?

3. **COVID-19 created a structural break**: Pre- and post-pandemic consumption patterns differ fundamentally.

### Expected Answer

**Multiple Seasonalities:**

For water demand with m=24 (hourly), s=168 (weekly), and annual=8760:

$$
y_t = \mu_t + \sum_{j=1}^{3} s_{j,t} + \varepsilon_t
$$

Where each $s_{j,t}$ represents a seasonal component. TBATS handles this via:

$$
s_{j,t} = \sum_{k=1}^{K_j} a_{j,k}\cos\left(\frac{2\pi k t}{m_j}\right) + b_{j,k}\sin\left(\frac{2\pi k t}{m_j}\right)
$$

**Non-linear Temperature Effects:**

Use a Generalized Additive Model (GAM):

$$
\ln(y_t) = \beta_0 + f_1(t) + f_2(T_t) + f_3(T_t, \text{is\_weekend}) + \varepsilon_t
$$

Where $f_2(T_t)$ is a spline capturing the threshold effect at 20°C.

**Structural Breaks:**

Apply the Chow test:

$$
F = \frac{(RSS_p - (RSS_1 + RSS_2))/k}{(RSS_1 + RSS_2)/(n_1 + n_2 - 2k)}
$$

If significant, either:
- Segment the data and build separate models
- Include regime indicators in the model
- Use time-varying coefficient models

### Why This Matters

Water utilities must maintain pressure across the network. Under-forecasting leads to pressure drops and customer complaints; over-forecasting leads to energy waste from over-pumping. The mathematics of accurate forecasting directly translates to operational efficiency and customer satisfaction.

---

## QUESTION 2: Anomaly Detection for Leakage Identification

### The Problem

Leakage detection presents a classic anomaly detection problem with extreme class imbalance:
- Normal flow: 99.97% of observations
- Leakage events: 0.03% of observations
- You have 2 years of flow sensor data from 1,200 DMAs (District Metered Areas)
- Each DMA has: inlet flow, pressure, night flow (2am-4am baseline)

### Part A: Mathematical Foundation

**Derive the decision boundary for an Isolation Forest and explain why it's particularly suited for high-dimensional anomaly detection.**

### Expected Answer

**Isolation Forest Principle:**

Anomalies are "few and different"—they are isolated closer to the root of random trees.

For a random tree, the expected path length for a point $x$ is:

$$
E[h(x)] = c(n) = 2H(n-1) - \frac{2(n-1)}{n}
$$

Where $H(i) = \ln(i) + \gamma$ (harmonic number).

The anomaly score is:

$$
s(x, n) = 2^{-\frac{E[h(x)]}{c(n)}}
$$

Points with $s(x,n) \approx 1$ are anomalies; $s(x,n) \ll 0.5$ are normal.

**Why Isolation Forest for Leakage:**
1. **No distance computation**: O(n log n) vs O(n²) for LOF
2. **Handles high dimensions**: Flow + pressure + time features
3. **No distributional assumptions**: Leakage patterns are non-Gaussian

### Part B: Practical Application

**Address the following operational constraints:**

1. **False Positive Trade-off**: Each alert requires a crew dispatch (£500 cost). Missing a leak costs £2,000/day in water loss. Derive the optimal classification threshold.

2. **Temporal Context**: A leak develops gradually. How do you distinguish between legitimate increased consumption (new housing development) and leakage?

3. **Feature Engineering**: What derived features would you create from raw flow/pressure data?

### Expected Answer

**Optimal Threshold via Cost-Sensitive Learning:**

Define the cost matrix:

|  | Predicted Normal | Predicted Leak |
|---|---|---|
| Actual Normal | 0 | £500 (FP) |
| Actual Leak | £2,000/day × days_undetected | £500 (TP) |

Expected cost for threshold $t$:

$$
C(t) = C_{FP} \cdot P(\hat{y}=1|y=0) \cdot P(y=0) + C_{FN} \cdot P(\hat{y}=0|y=1) \cdot P(y=1)
$$

The optimal threshold minimizes:

$$
t^* = \arg\min_t \left[ 500 \cdot FPR(t) \cdot 0.9997 + 2000 \cdot FNR(t) \cdot 0.0003 \right]
$$

Given the severe imbalance, we likely want precision > 0.8 even at the cost of lower recall.

**Distinguishing Leakage from Legitimate Increase:**

Use **night flow analysis**. Legitimate consumption has low night flow; leakage continues 24/7.

Define the Night Line Factor:

$$
NLF = \frac{\text{Flow}_{2am-4am}}{\text{Flow}_{24hr}}
$$

If NLF increases while total consumption increases → leakage suspected.
If NLF constant while consumption increases → legitimate growth.

**Feature Engineering:**

```python
# 1. Rate of change features
flow_delta = flow_t - flow_{t-24}  # Same hour yesterday

# 2. Pressure-Flow relationship (inverse for leaks)
pf_ratio = pressure / flow

# 3. Minimum night flow (MNF) statistics
mnf_30day = min(flow[2am-4am] for last 30 days)

# 4. Excess night flow
excess_night = night_flow - mnf_baseline

# 5. CUSUM statistic for change detection
S_t = max(0, S_{t-1} + (x_t - \mu_0) - k)
```

### Why This Matters

The UK water industry loses ~3 billion litres/day to leakage. Reducing this by even 1% saves millions of pounds and reduces environmental abstraction. The mathematics of anomaly detection directly addresses regulatory pressure (Ofwat) and environmental sustainability goals.

---

## QUESTION 3: Customer Churn Prediction for Utilities

### The Problem

Business Stream operates in a competitive retail water market. You need to predict which business customers are at risk of switching to competitors. Challenges:
- **Contract structures**: Fixed-term contracts with renewal windows
- **Censored data**: Many customers haven't churned yet (right-censored)
- **Competitive intelligence**: Limited visibility into competitor pricing
- **B2B complexity**: Consumption patterns vary enormously by sector

### Part A: Mathematical Foundation

**Derive the Cox Proportional Hazards model and explain how it handles censored observations. Contrast this with standard classification approaches.**

### Expected Answer

**Survival Analysis Framework:**

Let $T$ be the time to churn. The hazard function:

$$
h(t) = \lim_{\Delta t \to 0} \frac{P(t \leq T < t + \Delta t | T \geq t)}{\Delta t}
$$

**Cox Proportional Hazards Model:**

$$
h(t|X) = h_0(t) \exp(\beta_1 X_1 + \beta_2 X_2 + ... + \beta_p X_p)
$$

The partial likelihood (handling censoring):

$$
L(\beta) = \prod_{i: \delta_i=1} \frac{\exp(\beta^T X_i)}{\sum_{j \in R(t_i)} \exp(\beta^T X_j)}
$$

Where $R(t_i)$ is the risk set at time $t_i$, and $\delta_i = 1$ if event observed, 0 if censored.

**Why Not Standard Classification?**

| Aspect | Classification | Survival Analysis |
|---|---|---|
| Handles censoring | No | Yes |
| Time-varying features | Difficult | Natural |
| Predicts when | No | Yes |
| Contract renewal timing | Ignored | Central |

### Part B: Practical Application

**How would you operationalize this model?**

1. **Feature Engineering**: What signals indicate churn risk in B2B water retail?

2. **Intervention Strategy**: Given limited retention budget, how do you prioritize customers?

3. **Model Validation**: How do you evaluate a survival model with time-dependent covariates?

### Expected Answer

**Churn Risk Features:**

```python
# Contract features
contract_features = {
    'days_to_renewal': contract_end_date - today,
    'contract_length': contract_end - contract_start,
    'renewal_history': num_previous_renewals,
    'price_increase_last_year': (current_price - previous_price) / previous_price
}

# Consumption behavior
consumption_features = {
    'consumption_trend': slope of 12-month consumption,
    'payment_delays': days late on last 6 invoices,
    'query_frequency': support tickets per quarter,
    'usage_vs_contract': actual_consumption / contracted_volume
}

# Competitive risk
competitive_features = {
    'sector_competitiveness': num_competitors_in_sector,
    'price_sensitivity_score': elasticity_estimate,
    'geographic_competition': competitor_presence_in_region
}
```

**Intervention Prioritization:**

Use expected value framework:

$$
EV_i = P(\text{churn}_i | X_i) \times \text{Customer\_Value}_i \times \text{Retention\_Probability}_i
$$

Rank customers by $EV_i$ and intervene on top N where budget allows.

**Model Validation:**

- **Concordance Index (C-index)**: Probability that model correctly orders two randomly chosen subjects

$$
C = \frac{\sum_{i,j} 1_{T_i < T_j} \cdot 1_{\hat{\eta}_i > \hat{\eta}_j} \cdot \delta_i}{\sum_{i,j} 1_{T_i < T_j} \cdot \delta_i}
$$

- **Time-dependent AUC**: Evaluate discrimination at specific time horizons (6 months, 12 months)
- **Calibration**: Compare predicted vs actual survival probabilities using Kaplan-Meier stratification

### Why This Matters

Customer acquisition cost in B2B utilities is 5-10x retention cost. Accurate churn prediction enables proactive retention, directly impacting revenue. The mathematics of survival analysis respects the temporal structure of contractual relationships—essential for actionable predictions.

---

## QUESTION 4: Model Interpretability for Regulatory Compliance

### The Problem

Ofwat (the economic regulator) requires that pricing and operational decisions be "transparent and explainable." You've built a gradient boosting model (XGBoost) for demand forecasting that achieves superior accuracy but is a black box.

### Part A: Mathematical Foundation

**Derive SHAP (SHapley Additive exPlanations) values from cooperative game theory principles and explain why they provide consistent feature attributions.**

### Expected Answer

**Shapley Values from Game Theory:**

For a prediction $f(x)$ and feature coalition $S$:

$$
\phi_j = \sum_{S \subseteq N \setminus \{j\}} \frac{|S|!(|N| - |S| - 1)!}{|N|!} \left[ f(S \cup \{j\}) - f(S) \right]
$$

Where $f(S) = E[f(x) | x_S]$ is the conditional expectation.

**SHAP Properties (Axioms):**

1. **Efficiency**: $\sum_j \phi_j = f(x) - E[f(x)]$
2. **Symmetry**: If $f(S \cup \{i\}) = f(S \cup \{j\})$ for all $S$, then $\phi_i = \phi_j$
3. **Dummy**: If $f(S \cup \{i\}) = f(S)$ for all $S$, then $\phi_i = 0$
4. **Additivity**: For $f = f_1 + f_2$, $\phi_j(f) = \phi_j(f_1) + \phi_j(f_2)$

**Kernel SHAP Approximation:**

$$
\phi = \arg\min_{\phi} \sum_{S \subseteq N} \pi_x(S) \left[ f(S) - \phi_0 - \sum_{j \in S} \phi_j \right]^2
$$

Where $\pi_x(S) = \frac{|N|-1}{(|S|)(|N|-|S|)}$ is the Shapley kernel weight.

### Part B: Practical Application

**Regulatory Context:**

1. **Explain to a non-technical regulator**: How would you present SHAP values for a demand forecast that informed infrastructure investment?

2. **Contrast with LIME**: When would you use LIME instead of SHAP? What are the trade-offs?

3. **Global vs Local Interpretability**: How do you ensure the model is fair across customer segments?

### Expected Answer

**Regulatory Presentation:**

Use SHAP summary plots showing:
- Feature importance (mean absolute SHAP value)
- Direction of effect (positive/negative SHAP values)
- Interaction effects (SHAP dependence plots)

Example narrative: *"Temperature above 20°C increases demand by an average of 15%, with 95% confidence interval [12%, 18%]. This relationship is consistent across all customer segments."*

**SHAP vs LIME:**

| Aspect | SHAP | LIME |
|---|---|---|
| Theoretical foundation | Game theory (axiomatic) | Local linear approximation |
| Consistency | Guaranteed | Not guaranteed |
| Computation | Expensive (2^N) | Faster |
| Local fidelity | High | Approximate |
| Use case | Regulatory reports, model debugging | Real-time explanations, prototyping |

**Fairness Across Segments:**

Compute SHAP values stratified by:
- Customer size (small/medium/large)
- Geographic region
- Industry sector

Test for significant differences using ANOVA:

$$
F = \frac{\sum_{g} n_g (\bar{\phi}_{j,g} - \bar{\phi}_j)^2 / (G-1)}{\sum_{g} \sum_{i \in g} (\phi_{j,i} - \bar{\phi}_{j,g})^2 / (n-G)}
$$

If significant, the model may embed unfair biases requiring investigation.

### Why This Matters

Regulated utilities face scrutiny from Ofwat, the Competition and Markets Authority, and customer advocacy groups. Models that cannot explain their decisions risk regulatory rejection, fines, and reputational damage. The mathematics of interpretability transforms black-box predictions into defensible, auditable decisions.

---

## QUESTION 5: Experimental Design for A/B Testing Pricing Strategies

### The Problem

Business Stream wants to test a new dynamic pricing model where prices vary based on real-time demand. You need to design an experiment to measure:
- Price elasticity of demand
- Customer satisfaction impact
- Churn risk from price volatility

### Part A: Mathematical Foundation

**Derive the minimum detectable effect (MDE) formula for a two-sample t-test and explain how you would calculate required sample size for detecting a 5% demand reduction with 80% power.**

### Expected Answer

**Hypothesis Testing Framework:**

$$
H_0: \mu_t = \mu_c \quad \text{vs} \quad H_1: \mu_t \neq \mu_c
$$

**Minimum Detectable Effect:**

For two-sided test with significance level $\alpha$ and power $1-\beta$:

$$
MDE = (z_{1-\alpha/2} + z_{1-\beta}) \cdot \sigma \cdot \sqrt{\frac{2}{n}}
$$

Solving for sample size:

$$
n = \frac{2\sigma^2(z_{1-\alpha/2} + z_{1-\beta})^2}{MDE^2}
$$

**Example Calculation:**

To detect 5% reduction in demand ($\sigma = 20\%$ of mean, typical for water):

$$
n = \frac{2 \times (0.20)^2 \times (1.96 + 0.84)^2}{(0.05)^2} \approx 250 \text{ customers per arm}
$$

### Part B: Practical Application

**Address the following experimental challenges:**

1. **Network Effects**: Water is a shared resource. If treatment group reduces consumption, control group benefits from better pressure. How do you handle interference?

2. **Spillover**: Customers talk. Treatment group customers may inform control group about pricing changes.

3. **Seasonality**: Water demand varies dramatically by season. How do you ensure balanced randomization?

### Expected Answer

**Network Effects (Interference):**

Standard SUTVA (Stable Unit Treatment Value Assumption) is violated. Solutions:

1. **Cluster-based randomization**: Randomize by DMA rather than individual customer
   
   Variance inflation factor:
   
   $$
   VIF = 1 + (m-1)\rho
   $$
   
   Where $m$ is cluster size and $\rho$ is intra-cluster correlation.

2. **Graph-based interference models**:

$$
Y_i = \alpha + \tau Z_i + \gamma \sum_{j \in N(i)} Z_j + \varepsilon_i
$$

Where $\sum_{j \in N(i)} Z_j$ is the treatment exposure from neighbors.

**Spillover Mitigation:**

- Geographic separation: Test in non-adjacent regions
- Temporal staggering: Different start dates
- Communication monitoring: Track social media/referral patterns

**Seasonal Balance:**

Use stratified randomization:

$$
P(Z_i = 1 | S_i = s) = 0.5 \quad \forall s \in \{\text{Summer, Winter}\}
$$

Post-stratification estimator:

$$
\hat{\tau}_{ps} = \sum_{s} \frac{n_s}{n} (\bar{Y}_{t,s} - \bar{Y}_{c,s})
$$

This ensures equal representation across seasons, improving precision.

### Why This Matters

Pricing experiments in utilities have real financial and social consequences. Poor experimental design leads to:
- Underpowered studies (wasted resources, no actionable insight)
- Biased estimates (wrong pricing decisions)
- Customer harm (unintended consequences)

The mathematics of experimental design ensures valid causal inference—essential for evidence-based pricing policy.

---

## QUESTION 6: Bayesian vs Frequentist Approaches for Demand Forecasting

### The Problem

You need to forecast water demand for a new housing development with only 6 months of historical data. The frequentist approach yields wide confidence intervals due to small sample size. Your manager asks about Bayesian methods.

### Part A: Mathematical Foundation

**Contrast the frequentist and Bayesian approaches to parameter estimation. Derive the posterior distribution for a simple Gaussian model and explain how prior information reduces uncertainty.**

### Expected Answer

**Frequentist Approach:**

Point estimate: $\hat{\theta}_{MLE} = \arg\max_\theta L(\theta | x)$

Confidence interval:

$$
P(\hat{\theta} - z_{\alpha/2} \cdot SE(\hat{\theta}) \leq \theta \leq \hat{\theta} + z_{\alpha/2} \cdot SE(\hat{\theta})) = 1 - \alpha
$$

Note: $\theta$ is fixed, interval is random.

**Bayesian Approach:**

Bayes' Theorem:

$$
p(\theta | x) = \frac{p(x | \theta) \cdot p(\theta)}{p(x)} = \frac{p(x | \theta) \cdot p(\theta)}{\int p(x | \theta) p(\theta) d\theta}
$$

**Conjugate Prior Example (Gaussian):**

Likelihood: $x_i \sim N(\mu, \sigma^2)$

Prior: $\mu \sim N(\mu_0, \tau^2)$

Posterior:

$$
\mu | x \sim N\left(\frac{\frac{\mu_0}{\tau^2} + \frac{n\bar{x}}{\sigma^2}}{\frac{1}{\tau^2} + \frac{n}{\sigma^2}}, \frac{1}{\frac{1}{\tau^2} + \frac{n}{\sigma^2}}\right)
$$

Posterior precision = Prior precision + Data precision

$$
\frac{1}{\text{Posterior Variance}} = \frac{1}{\tau^2} + \frac{n}{\sigma^2}
$$

### Part B: Practical Application

**How would you apply Bayesian methods to water demand forecasting?**

1. **Prior Elicitation**: Where would you get informative priors for a new development?

2. **Computational Approach**: Would you use MCMC, variational inference, or analytical solutions?

3. **Decision Making**: How do Bayesian credible intervals differ from frequentist confidence intervals in operational decisions?

### Expected Answer

**Prior Elicitation:**

Sources of informative priors:
- Similar developments (analogous reasoning)
- Engineering models (per-capita consumption standards)
- National statistics (OFWAT reported consumption by property type)

Example prior for per-capita consumption:

$$
\mu \sim N(150 \text{ L/person/day}, 30^2)
$$

Based on UK average of 142 L/person/day with uncertainty.

**Computational Approach:**

For hierarchical demand model:

$$
\begin{aligned}
y_{i,t} &\sim N(\mu_i + \beta_i t, \sigma^2) \\
\mu_i &\sim N(\mu_0, \tau_\mu^2) \\
\beta_i &\sim N(\beta_0, \tau_\beta^2) \\
\mu_0, \beta_0, \tau_\mu, \tau_\beta &\sim \text{Weakly Informative Priors}
\end{aligned}
$$

Use Stan/PyMC3 with NUTS sampler for full Bayesian inference.

**Decision Context:**

| Aspect | Frequentist CI | Bayesian Credible Interval |
|---|---|---|
| Interpretation | 95% of such intervals contain true parameter | 95% probability parameter is in this interval |
| Small sample | Wide, uninformative | Can be tight with good prior |
| Incorporating domain knowledge | Not possible | Natural |
| Operational decision | Conservative | Can incorporate utility function |

For infrastructure sizing, Bayesian approach allows:

$$
P(\text{demand} > \text{capacity}) = \int_{\text{capacity}}^{\infty} p(\text{demand} | \text{data}) \, d(\text{demand})
$$

Direct probability statements about capacity adequacy.

### Why This Matters

Water utilities often face data scarcity (new developments, rare events). Bayesian methods leverage domain knowledge to make better decisions with limited data. The mathematics of Bayesian inference transforms subjective expertise into quantifiable uncertainty—critical for risk management in infrastructure planning.

---

## QUESTION 7: Bias-Variance Tradeoff in Utility Models

### The Problem

You've developed a complex ensemble model (XGBoost + Neural Network stacking) for demand forecasting. Training RMSE is excellent, but validation performance degrades significantly. Your simpler linear model generalizes better.

### Part A: Mathematical Foundation

**Decompose the expected prediction error into bias, variance, and irreducible error components. Derive the mathematical relationship and explain the tradeoff.**

### Expected Answer

**Expected Prediction Error Decomposition:**

For a model $\hat{f}(x)$ predicting $y = f(x) + \varepsilon$ where $\varepsilon \sim N(0, \sigma^2)$:

$$
\begin{aligned}
E[(y - \hat{f}(x))^2] &= E[(f(x) + \varepsilon - \hat{f}(x))^2] \\
&= E[(f(x) - \hat{f}(x))^2] + \sigma^2 \\
&= (f(x) - E[\hat{f}(x)])^2 + E[(\hat{f}(x) - E[\hat{f}(x)])^2] + \sigma^2 \\
&= \text{Bias}^2 + \text{Variance} + \text{Irreducible Error}
\end{aligned}
$$

**The Tradeoff:**

- **High Bias (Underfitting)**: Model too simple, misses patterns
- **High Variance (Overfitting)**: Model too complex, fits noise

$$
\frac{\partial \text{Bias}^2}{\partial \text{Complexity}} < 0, \quad \frac{\partial \text{Variance}}{\partial \text{Complexity}} > 0
$$

Optimal complexity minimizes total error.

### Part B: Practical Application

**Diagnose and address the following scenarios in utility forecasting:**

1. **Your ensemble overfits**: Training RMSE = 5 ML/day, Validation RMSE = 18 ML/day. What techniques would you apply?

2. **Seasonal underfitting**: Your linear model misses summer peaks consistently. Is this bias or variance?

3. **Model selection**: Given limited data (3 years), should you prefer a simpler SARIMA or a complex LSTM?

### Expected Answer

**Overfitting Diagnosis and Remediation:**

The gap (5 vs 18 ML/day) indicates severe overfitting. Techniques:

1. **Regularization**:
   - L2: $\min_\theta \sum_i (y_i - \hat{f}(x_i))^2 + \lambda \|\theta\|_2^2$
   - L1: $\min_\theta \sum_i (y_i - \hat{f}(x_i))^2 + \lambda \|\theta\|_1$

2. **Ensemble regularization**:
   - Reduce base learner depth
   - Increase minimum samples per leaf
   - Add dropout to neural network component

3. **Data augmentation**:
   - Add noise to training data
   - Use synthetic samples (SMOTE for anomalies)

4. **Early stopping**: Monitor validation loss, stop when it increases

**Seasonal Underfitting:**

Consistent under-prediction of summer peaks is **bias**, not variance. The model systematically misses a pattern.

Solutions:
- Add temperature interaction terms
- Include seasonal dummy variables
- Use Fourier series for flexible seasonality

$$
s_t = \sum_{k=1}^{K} \left[ a_k \cos\left(\frac{2\pi k t}{365}\right) + b_k \sin\left(\frac{2\pi k t}{365}\right) \right]
$$

**Model Selection with Limited Data:**

For 3 years (1095 days) of data:

| Model | Parameters | Data Points per Parameter |
|---|---|---|
| SARIMA(2,1,2)(1,1,1)7 | ~8 | ~137 |
| LSTM (2 layers, 64 units) | ~50,000 | ~0.02 |

The LSTM will severely overfit. Prefer SARIMA or:
- Regularized regression (Elastic Net)
- Gradient boosting with strong regularization
- Prophet (which has ~10 hyperparameters)

Use cross-validation to empirically verify:

$$
\hat{f}^* = \arg\min_{f \in \mathcal{F}} \frac{1}{K} \sum_{k=1}^{K} \mathcal{L}(y_k, \hat{f}^{-k}(x_k))
$$

### Why This Matters

In utilities, model failure has operational consequences. An overfit model that misses a drought-induced demand spike could lead to supply failures. An underfit model that overestimates demand leads to wasted energy and capital. The mathematics of bias-variance guides the fundamental model selection decision.

---

## QUESTION 8: Feature Engineering for Time-Series Utility Data

### The Problem

You have raw hourly data: flow rate, pressure, temperature, rainfall. You need to engineer features for a leakage detection model. What transformations would you apply and why?

### Part A: Mathematical Foundation

**Explain the mathematical basis for lag features, rolling statistics, and Fourier transforms in time series feature engineering. When is each appropriate?**

### Expected Answer

**Lag Features:**

Capture temporal dependencies (autocorrelation):

$$
x_{t-l} = f(x_t) + \varepsilon_t
$$

Appropriate when: Past values directly influence current state (e.g., yesterday's consumption pattern predicts today's).

**Rolling Statistics:**

Moving average (smoothing):

$$
\bar{x}_t^{(w)} = \frac{1}{w} \sum_{i=0}^{w-1} x_{t-i}
$$

Exponentially weighted:

$$
\tilde{x}_t = \alpha x_t + (1-\alpha) \tilde{x}_{t-1}
$$

Rolling standard deviation (volatility):

$$
\sigma_t^{(w)} = \sqrt{\frac{1}{w} \sum_{i=0}^{w-1} (x_{t-i} - \bar{x}_t^{(w)})^2}
$$

Appropriate when: Recent trend/dispersion matters more than distant history.

**Fourier Transforms:**

Decompose into frequency components:

$$
X_k = \sum_{n=0}^{N-1} x_n e^{-i 2\pi k n / N}
$$

Power spectral density:

$$
S_{xx}(f) = |X(f)|^2
$$

Appropriate when: Cyclical patterns exist at known frequencies (daily, weekly, annual).

### Part B: Practical Application

**Engineer specific features for leakage detection:**

Given: Hourly flow (m³/hr), pressure (bar), temperature (°C), rainfall (mm)

### Expected Answer

```python
# 1. Lag features (autoregressive structure)
flow_lag_24 = flow.shift(24)  # Same hour yesterday
flow_lag_168 = flow.shift(168)  # Same hour last week

# 2. Rolling statistics (trend and volatility)
flow_ma_24 = flow.rolling(24).mean()  # Daily average
flow_std_168 = flow.rolling(168).std()  # Weekly volatility

# 3. Rate of change (anomaly indicator)
flow_delta = flow.diff()
flow_pct_change = flow.pct_change()

# 4. Night flow analysis (leakage signature)
night_mask = (hour >= 2) & (hour <= 4)
mnf = flow[night_mask].rolling(30*24).min()  # Min night flow (30-day window)
excess_night = flow[night_mask] - mnf

# 5. Pressure-flow relationship (network health)
pf_ratio = pressure / flow
pf_ratio_change = pf_ratio.pct_change()

# 6. Weather-adjusted consumption
# Temperature effect (non-linear)
temp_hot = np.maximum(0, temperature - 20)  # Threshold at 20°C

# Rainfall effect (reduces outdoor use)
rain_effect = np.exp(-rainfall / 10)  # Exponential decay

# 7. Calendar features (cyclical encoding)
hour_sin = np.sin(2 * np.pi * hour / 24)
hour_cos = np.cos(2 * np.pi * hour / 24)
dow_sin = np.sin(2 * np.pi * dayofweek / 7)
dow_cos = np.cos(2 * np.pi * dayofweek / 7)

# 8. CUSUM for change detection (leakage onset)
mu_0 = flow.rolling(168).mean()  # Baseline
k = 0.5 * flow.rolling(168).std()  # Slack parameter
S_pos = np.maximum(0, S_pos.shift(1) + (flow - mu_0) - k)
S_neg = np.maximum(0, S_neg.shift(1) - (flow - mu_0) - k)

# 9. Entropy features (pattern complexity)
def spectral_entropy(x):
    psd = np.abs(np.fft.fft(x))**2
    psd_norm = psd / psd.sum()
    return -np.sum(psd_norm * np.log2(psd_norm + 1e-10))

flow_entropy = flow.rolling(168).apply(spectral_entropy)

# 10. Cross-correlation features
flow_temp_corr = flow.rolling(168).corr(temperature)
```

### Why This Matters

Feature engineering often matters more than algorithm choice. In leakage detection, the difference between normal variation and leakage can be subtle—a 5% increase in night flow sustained over a week. The mathematics of time-series features transforms raw sensor data into leakage-sensitive signals, enabling detection that would be impossible with raw data alone.

---

## QUESTION 9: Cross-Validation Strategies for Temporal Data

### The Problem

You built a demand forecasting model using 5-fold cross-validation, achieving excellent metrics. In production, the model fails during the first seasonal transition. What went wrong?

### Part A: Mathematical Foundation

**Explain why standard k-fold cross-validation is invalid for time series data. Derive the mathematical relationship between temporal correlation and cross-validation bias.**

### Expected Answer

**The Problem with Random Cross-Validation:**

Standard k-fold randomly shuffles data, violating temporal ordering. For autocorrelated time series:

$$
\text{Cov}(y_t, y_{t-k}) = \gamma(k) \neq 0
$$

When training and validation sets contain adjacent time points, information "leaks" from training to validation:

$$
E[\hat{\mathcal{L}}_{CV}] < E[\hat{\mathcal{L}}_{test}]
$$

The cross-validation estimate is optimistically biased.

**Mathematical Derivation:**

For AR(1) process: $y_t = \phi y_{t-1} + \varepsilon_t$

If training includes $y_{t-1}$ and validation includes $y_t$:

$$
\text{Var}(\hat{y}_t - y_t) = \sigma^2_\varepsilon + \text{Var}(\hat{\phi}) y_{t-1}^2 - 2\text{Cov}(\hat{\phi}y_{t-1}, \phi y_{t-1})
$$

The model has seen $y_{t-1}$ during training, creating artificial predictive power.

### Part B: Practical Application

**Design appropriate validation strategies for:**

1. **Short-term forecasting** (next 24 hours)
2. **Long-term capacity planning** (next 5 years)
3. **Model selection** among competing architectures

### Expected Answer

**1. Short-Term Forecasting (24-hour horizon):**

Use **Time Series Split (Walk-Forward Validation)**:

```
Fold 1: Train [1], Test [2]
Fold 2: Train [1,2], Test [3]
Fold 3: Train [1,2,3], Test [4]
...
```

Mathematically:

$$
\hat{\mathcal{L}}_{WF} = \frac{1}{T-k} \sum_{t=k+1}^{T} \mathcal{L}(y_t, \hat{f}_{[1:t-1]}(x_t))
$$

Ensures no lookahead bias.

**2. Long-Term Capacity Planning (5-year horizon):**

Use **Blocked Cross-Validation** with sufficient gap:

```
Block 1: Train [1:365], Gap [366:395], Test [396:730]
Block 2: Train [1:730], Gap [731:760], Test [761:1095]
...
```

The gap (30 days) ensures temporal correlation decays:

$$
\gamma(30) = \phi^{30} \gamma(0) \approx 0 \text{ for typical } \phi < 0.9
$$

**3. Model Selection:**

Use **Purged Cross-Validation** (for overlapping labels):

```
For each fold:
  - Remove observations within embargo period of test set
  - Train on remaining data
  - Evaluate on test set
```

Prevents information leakage from overlapping time windows.

**Practical Implementation:**

```python
from sklearn.model_selection import TimeSeriesSplit

# For short-term forecasting
tscv = TimeSeriesSplit(n_splits=5)
for train_idx, test_idx in tscv.split(X):
    X_train, X_test = X[train_idx], X[test_idx]
    # Train and evaluate

# For model selection with gap
def blocked_cv(X, y, n_splits=5, gap=30):
    n_samples = len(X)
    fold_size = n_samples // n_splits
    
    scores = []
    for i in range(n_splits):
        test_start = (i + 1) * fold_size
        test_end = min(test_start + fold_size, n_samples)
        
        train_end = test_start - gap
        X_train, y_train = X[:train_end], y[:train_end]
        X_test, y_test = X[test_start:test_end], y[test_start:test_end]
        
        # Train and evaluate
        scores.append(model.score(X_test, y_test))
    
    return np.mean(scores)
```

### Why This Matters

Invalid cross-validation leads to models that fail in production. The mathematics of temporal validation ensures that evaluation mimics real-world deployment—where the future is genuinely unknown. For utilities, this prevents costly model failures during critical periods (droughts, heatwaves).

---

## QUESTION 10: When NOT to Use Machine Learning

### The Problem

Your manager asks you to build a machine learning model to predict which customers will call customer service. You suspect this may not be the right approach. How do you respond?

### Part A: Theoretical Foundation

**What are the conditions under which ML is inappropriate? Contrast with rule-based and optimization approaches.**

### Expected Answer

**Conditions Favoring Rule-Based Over ML:**

1. **Deterministic relationships**: When first-principles physics governs behavior
   - Example: Pressure drop in pipes follows Darcy-Weisbach equation

2. **Sparse data**: When examples are too few to learn patterns
   - Example: Catastrophic infrastructure failures (rare events)

3. **High interpretability requirements**: When every decision must be explainable
   - Example: Regulatory pricing decisions

4. **Stable environment**: When rules don't need adaptation
   - Example: Safety cutoff thresholds

**Decision Framework:**

$$
\text{Use ML if: } \underbrace{\text{Complex Pattern}}_{\text{Yes}} \times \underbrace{\text{Sufficient Data}}_{\text{Yes}} \times \underbrace{\text{Tolerable Opacity}}_{\text{Yes}} > \text{Threshold}
$$

**Mathematical Comparison:**

| Approach | Complexity | Data Required | Interpretability | Adaptability |
|---|---|---|---|---|
| Rule-based | Low | None | High | Manual |
| Optimization | Medium | Parameters | High | Parameter update |
| ML | High | Large | Variable | Automatic |

### Part B: Practical Application

**For each scenario, recommend ML, rule-based, or optimization approach:**

1. **Leakage detection in a DMA with 5 years of flow data**
2. **Determining optimal pump scheduling given electricity prices**
3. **Predicting customer satisfaction after a billing dispute**
4. **Setting pressure reducing valve thresholds for new development**
5. **Detecting meter tampering with 12 known historical cases**

### Expected Answer

**1. Leakage Detection (5 years of data):**
→ **ML (Anomaly Detection)**
- Complex patterns in flow/pressure relationships
- Sufficient historical data
- Can tolerate some false positives (human verification)

**2. Pump Scheduling:**
→ **Optimization (Linear/Integer Programming)**
- Deterministic constraints (demand, capacity, electricity prices)
- Well-defined objective function (minimize cost)
- Rules are known, just need optimal combination

Formulation:

$$
\begin{aligned}
\min_{x_{t,p}} \quad & \sum_t \sum_p c_t \cdot P_p \cdot x_{t,p} \\
\text{s.t.} \quad & \sum_p Q_p \cdot x_{t,p} \geq D_t \quad \forall t \\
& x_{t,p} \in \{0, 1\} \quad \text{(on/off)}
\end{aligned}
$$

**3. Customer Satisfaction Prediction:**
→ **ML (Classification)**
- Complex interaction of factors (billing history, issue type, communication channel)
- Large historical dataset available
- Interpretability helpful but not critical

**4. Pressure Reducing Valve Thresholds:**
→ **Rule-based (Engineering Standards)**
- Deterministic physics (pressure requirements by building type)
- Safety-critical (must be explainable)
- Standards exist (e.g., 1.5 bar minimum for residential)

**5. Meter Tampering Detection (12 cases):**
→ **Rule-based + Anomaly Detection hybrid**
- Insufficient positive examples for supervised ML
- Can engineer rules based on known tampering patterns
- Use unsupervised anomaly detection for unknown patterns

### Why This Matters

ML is a powerful tool, but not the right tool for every problem. The mathematics of decision theory tells us that simpler approaches often outperform complex models, especially with limited data or high interpretability requirements. In utilities, choosing the wrong approach can lead to regulatory rejection, operational failures, or wasted resources.

---

## QUESTION 11: Optimization Algorithms for Resource Allocation

### The Problem

Business Stream needs to optimize water treatment chemical dosing across 15 treatment plants. Each plant has:
- Variable raw water quality (turbidity, pH, contaminant levels)
- Different treatment capacities
- Time-varying electricity prices
- Regulatory constraints on effluent quality

### Part A: Mathematical Foundation

**Formulate this as a constrained optimization problem. Derive the KKT conditions and explain when gradient-based vs. gradient-free methods are appropriate.**

### Expected Answer

**Problem Formulation:**

Let $x_{i,t}$ be chemical dosing at plant $i$ during time $t$.

Objective (minimize total cost):

$$
\min_{x} \sum_{i=1}^{15} \sum_{t=1}^{T} \left[ c_{chem} \cdot x_{i,t} + c_{elec,t} \cdot E_i(x_{i,t}, q_{i,t}) \right]
$$

Subject to:

$$
\begin{aligned}
& \text{Quality constraint: } Q_i(x_{i,t}, q_{i,t}) \leq Q_{max} \quad \forall i, t \\
& \text{Capacity constraint: } x_{i,t} \leq C_i \quad \forall i, t \\
& \text{Non-negativity: } x_{i,t} \geq 0 \quad \forall i, t \\
& \text{Demand satisfaction: } \sum_i T_i(x_{i,t}) \geq D_t \quad \forall t
\end{aligned}
$$

Where $q_{i,t}$ is raw water quality, $E_i$ is energy consumption, $Q_i$ is effluent quality, $T_i$ is treated water output.

**KKT Conditions:**

Lagrangian:

$$
\mathcal{L}(x, \lambda, \mu) = f(x) + \sum_j \lambda_j g_j(x) + \sum_k \mu_k h_k(x)
$$

Stationarity: $\nabla_x \mathcal{L} = 0$

Primal feasibility: $g_j(x) \leq 0$, $h_k(x) = 0$

Dual feasibility: $\lambda_j \geq 0$

Complementary slackness: $\lambda_j g_j(x) = 0$

**Method Selection:**

| Method | When Appropriate | Complexity |
|---|---|---|
| Linear Programming | Linear objective + constraints | Polynomial |
| Quadratic Programming | Quadratic objective, linear constraints | Polynomial |
| Gradient Descent | Differentiable, convex | $O(1/\epsilon)$ |
| Newton's Method | Twice differentiable, convex | $O(\log \log(1/\epsilon))$ |
| Genetic Algorithms | Non-convex, discrete, black-box | Heuristic |
| Bayesian Optimization | Expensive function evaluations | $O(n^3)$ |

### Part B: Practical Application

**How would you solve this in practice?**

1. **Simplification**: What approximations make this tractable?

2. **Uncertainty**: Raw water quality is stochastic. How do you handle this?

3. **Real-time**: How do you solve this fast enough for operational decisions?

### Expected Answer

**Practical Solution Approach:**

**1. Simplification:**

Approximate energy and quality functions as linear:

$$
E_i(x, q) \approx a_i(q) + b_i(q) \cdot x
$$

This transforms the problem into a Linear Program (LP), solvable in polynomial time.

**2. Handling Uncertainty:**

Use **Stochastic Programming** or **Robust Optimization**:

**Stochastic approach** (if distribution known):

$$
\min_x E_\xi[f(x, \xi)] \quad \text{s.t. } P(g(x, \xi) \leq 0) \geq 1 - \alpha
$$

**Robust approach** (distribution-free):

$$
\min_x \max_{\xi \in \mathcal{U}} f(x, \xi) \quad \text{s.t. } g(x, \xi) \leq 0 \quad \forall \xi \in \mathcal{U}
$$

Where $\mathcal{U}$ is an uncertainty set (e.g., box or ellipsoidal).

**3. Real-Time Solution:**

- **Offline**: Pre-compute optimal policies for typical scenarios
- **Online**: Use model predictive control (MPC)

MPC formulation (receding horizon):

```python
for t in range(T):
    # Solve optimization over horizon H
    x_opt = solve_mpc(horizon=t:t+H, current_state=s_t)
    # Apply first control
    apply_control(x_opt[0])
    # Observe new state
    s_{t+1} = observe_state()
```

This handles model uncertainty and disturbances while maintaining feasibility.

**Implementation Stack:**

```python
import cvxpy as cp

# Decision variables
dose = cp.Variable((15, T), nonneg=True)

# Parameters (updated each timestep)
turbidity = cp.Parameter(15, T)
elec_price = cp.Parameter(T)

# Objective
cost = cp.sum(chem_cost * dose + elec_price * energy_consumption(dose, turbidity))

# Constraints
constraints = [
    effluent_quality(dose, turbidity) <= REGULATORY_LIMIT,
    dose <= PLANT_CAPACITY,
    cp.sum(treatment_output(dose), axis=0) >= DEMAND
]

# Solve
prob = cp.Problem(cp.Minimize(cost), constraints)
prob.solve(solver=cp.GUROBI)
```

### Why This Matters

Chemical dosing is a major operational cost (typically 10-20% of treatment costs). Optimization can reduce costs by 5-15% while maintaining regulatory compliance. The mathematics of constrained optimization transforms operational expertise into systematic, repeatable cost savings.

---

## QUESTION 12: Handling Concept Drift in Production Models

### The Problem

Your demand forecasting model, trained on 2018-2022 data, began systematically over-forecasting in 2023. Investigation reveals:
- Increased remote work post-COVID changed consumption patterns
- New water-efficient appliances reduced per-capita consumption
- Climate change shifted temperature-demand relationships

### Part A: Mathematical Foundation

**Define concept drift mathematically. Contrast sudden drift, gradual drift, and recurring concepts. Derive a statistical test for detecting drift.**

### Expected Answer

**Concept Drift Definition:**

Let $P_t(X, y)$ be the joint distribution at time $t$. Concept drift occurs when:

$$
\exists t: P_t(X, y) \neq P_{t+1}(X, y)
$$

**Types of Drift:**

1. **Sudden drift**: $P_t$ changes abruptly at $t^*$
   
   $$P_t = \begin{cases} P_0 & t < t^* \\ P_1 & t \geq t^* \end{cases}$$

2. **Gradual drift**: $P_t$ changes continuously
   
   $$P_t = (1 - \lambda(t)) P_0 + \lambda(t) P_1$$
   
   Where $\lambda(t)$ is a transition function.

3. **Recurring concepts**: $P_t$ cycles between known distributions
   
   $$P_t = P_{t \mod T}$$

**Drift Detection Test:**

**Page-Hinkley Test** for change in mean:

$$
\begin{aligned}
PH_t &= \sum_{i=1}^{t} (x_i - \bar{x}_t) - \delta \\
&= PH_{t-1} + (x_t - \bar{x}_t) - \delta
\end{aligned}
$$

Alarm when $PH_t > \lambda$ (threshold).

**Kolmogorov-Smirnov Test** for distribution change:

$$
D_{n,m} = \sup_x |F_{1,n}(x) - F_{2,m}(x)|
$$

Reject null (no drift) if $D_{n,m} > c(\alpha) \sqrt{\frac{n+m}{nm}}$.

### Part B: Practical Application

**Design a monitoring and adaptation system for production deployment.**

1. **Detection**: What metrics would you monitor?

2. **Adaptation**: How would you update the model?

3. **Validation**: How do you ensure the updated model is better?

### Expected Answer

**1. Monitoring Metrics:**

```python
# Performance-based metrics
prediction_error = y_true - y_pred
rolling_mape = np.abs(prediction_error / y_true).rolling(30).mean()

# Distribution-based metrics (input drift)
reference_distribution = X_train  # Baseline
current_window = X_recent

# KS test for each feature
for feature in features:
    ks_stat, p_value = ks_2samp(reference_distribution[feature], 
                                 current_window[feature])
    if p_value < 0.01:
        alert(f"Drift detected in {feature}")

# Population Stability Index (PSI)
def calculate_psi(expected, actual, buckets=10):
    def scale_range(input, min_val, max_val):
        return (input - min_val) / (max_val - min_val)
    
    breakpoints = np.linspace(0, 1, buckets + 1)
    breakpoints = np.percentile(expected, breakpoints * 100)
    
    expected_percents = np.histogram(expected, breakpoints)[0] / len(expected)
    actual_percents = np.histogram(actual, breakpoints)[0] / len(actual)
    
    psi = np.sum((expected_percents - actual_percents) * 
                 np.log(expected_percents / actual_percents + 1e-10))
    return psi

# Alert if PSI > 0.25 (significant change)
```

**2. Adaptation Strategies:**

| Strategy | When to Use | Implementation |
|---|---|---|
| **Periodic retraining** | Gradual drift | Weekly/monthly full retrain |
| **Online learning** | Continuous adaptation | Incremental updates (SGD) |
| **Ensemble approach** | Recurring concepts | Weight models by recent performance |
| **Drift-aware algorithms** | Known drift patterns | ADWIN, Hoeffding trees |

**Online Learning Update:**

```python
# Incremental update with new data
model.partial_fit(X_new, y_new)

# Or with exponential weighting
for (x, y) in stream:
    prediction = model.predict(x)
    error = y - prediction
    
    # Weight recent samples more heavily
    weight = lambda ** (t_current - t_sample)
    model.update(x, y, weight=weight)
```

**3. Validation Framework:**

**Backtesting with rolling windows:**

```
Window 1: Train [2018-2020], Test [2021]
Window 2: Train [2019-2021], Test [2022]
Window 3: Train [2020-2022], Test [2023]
```

Compare:
- Static model (trained once)
- Periodically retrained model
- Online adapted model

Use **statistical significance testing**:

```python
from scipy.stats import ttest_rel

# Compare two models
errors_model_a = [...]  # MAPE for each test period
errors_model_b = [...]

t_stat, p_value = ttest_rel(errors_model_a, errors_model_b)
if p_value < 0.05 and np.mean(errors_model_b) < np.mean(errors_model_a):
    print("Model B significantly better")
```

**Production Deployment:**

```python
class DriftAwarePredictor:
    def __init__(self, base_model, drift_detector, retrain_threshold):
        self.model = base_model
        self.drift_detector = drift_detector
        self.retrain_threshold = retrain_threshold
        self.buffer = []
        
    def predict(self, X):
        return self.model.predict(X)
    
    def update(self, X, y_true):
        # Add to buffer
        self.buffer.append((X, y_true))
        
        # Check for drift
        if self.drift_detector.detect(X):
            # Trigger retraining
            if len(self.buffer) >= self.retrain_threshold:
                self.model.fit([x for x, _ in self.buffer],
                              [y for _, y in self.buffer])
                self.buffer = []  # Clear buffer after retrain
```

### Why This Matters

Models in production face a constantly changing world. A model that worked yesterday may fail tomorrow. The mathematics of drift detection and adaptation ensures that models remain accurate and reliable over time—critical for operational decisions in utilities where model failures can have significant consequences.

---

## QUESTION 13: End-to-End Prescriptive Analytics System for Leakage Reduction

### The Problem

Business Stream wants to build a comprehensive system that:
1. **Predicts** where leaks are likely to occur (predictive)
2. **Prioritizes** which leaks to repair first (prescriptive)
3. **Optimizes** crew dispatch and resource allocation (optimization)
4. **Learns** from repair outcomes to improve future predictions

This is the ultimate test of a senior data scientist—integrating multiple techniques into a production system.

### Part A: System Architecture

**Design the end-to-end architecture. What components are needed, and how do they interact?**

### Expected Answer

**System Architecture:**

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA INGESTION LAYER                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │ Flow     │  │ Pressure │  │ Weather  │  │ Work     │        │
│  │ Sensors  │  │ Sensors  │  │ API      │  │ Orders   │        │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘        │
└───────┼─────────────┼─────────────┼─────────────┼──────────────┘
        │             │             │             │
        └─────────────┴─────────────┴─────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    FEATURE ENGINEERING PIPELINE                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  • Lag features    • Rolling statistics    • Anomalies  │   │
│  │  • Weather effects • Network topology      • History    │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
┌─────────────────┐ ┌──────────────┐ ┌─────────────────┐
│  PREDICTIVE     │ │  PRESCRIPTIVE │ │  OPTIMIZATION   │
│  LAYER          │ │  LAYER        │ │  LAYER          │
│                 │ │               │ │                 │
│ ┌─────────────┐ │ │ ┌──────────┐  │ │ ┌─────────────┐ │
│ │ Leakage Risk│ │ │ │ Business│  │ │ │ Crew        │ │
│ │ Model       │ │ │ │ Value   │  │ │ │ Dispatch    │ │
│ │ (XGBoost)   │ │ │ │ Scoring │  │ │ │ (MILP)      │ │
│ └─────────────┘ │ │ └──────────┘  │ │ └─────────────┘ │
│                 │ │               │ │                 │
│ ┌─────────────┐ │ │ ┌──────────┐  │ │ ┌─────────────┐ │
│ │ Failure     │ │ │ │ Priority│  │ │ │ Resource    │ │
│ │ Prediction  │ │ │ │ Queue   │  │ │ │ Allocation  │ │
│ │ (Survival)  │ │ │ │         │  │ │ │             │ │
│ └─────────────┘ │ │ └──────────┘  │ │ └─────────────┘ │
└─────────────────┘ └──────────────┘ └─────────────────┘
          │               │               │
          └───────────────┼───────────────┘
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    FEEDBACK & LEARNING LOOP                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  • Repair outcomes    • False positive tracking         │   │
│  │  • Model performance  • Continuous retraining           │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Part B: Mathematical Integration

**Derive the mathematical formulation that connects all three layers.**

### Expected Answer

**Layer 1: Predictive (Leakage Probability)**

For each DMA $i$ at time $t$:

$$
P(\text{leak}_{i,t} | X_{i,t}) = f_{\theta}(X_{i,t})
$$

Where $f_\theta$ is a gradient boosting model outputting leak probability.

**Layer 2: Prescriptive (Business Value)**

Expected value of repairing DMA $i$:

$$
EV_i = P(\text{leak}_i) \cdot V_i - C_{\text{dispatch}} - C_{\text{repair}} \cdot P(\text{leak}_i)
$$

Where:
- $V_i$ = Value of water saved if leak exists (leak rate × water cost × time)
- $C_{\text{dispatch}}$ = Fixed crew dispatch cost
- $C_{\text{repair}}$ = Expected repair cost

**Layer 3: Optimization (Crew Dispatch)**

Given $N$ potential leaks and $K$ crews, maximize total expected value:

$$
\begin{aligned}
\max_{x_{i,k}} \quad & \sum_{i=1}^{N} \sum_{k=1}^{K} EV_i \cdot x_{i,k} \\
\text{s.t.} \quad & \sum_{i=1}^{N} x_{i,k} \leq 1 \quad \forall k \quad \text{(one assignment per crew)} \\
& \sum_{k=1}^{K} x_{i,k} \leq 1 \quad \forall i \quad \text{(one crew per leak)} \\
& \sum_{i=1}^{N} T_i \cdot x_{i,k} \leq H_k \quad \forall k \quad \text{(time constraint)} \\
& x_{i,k} \in \{0, 1\} \quad \text{(binary assignment)}
\end{aligned}
$$

Where $T_i$ is estimated repair time, $H_k$ is crew $k$'s available hours.

**Integration:**

The output of Layer 1 feeds into Layer 2:

$$
EV_i = g(P(\text{leak}_i), \text{leak\_rate}_i, \text{location}_i, \text{crew\_availability})
$$

The prioritized list from Layer 2 feeds into Layer 3:

$$
x^* = \arg\max_{x} \sum_{i \in \text{PriorityQueue}} EV_i \cdot x_i
$$

### Part C: Practical Implementation

**Address the following production challenges:**

1. **Latency**: Leakage alerts need near real-time response. How do you achieve this?

2. **Uncertainty**: Model predictions have uncertainty. How do you incorporate this?

3. **Fairness**: How do you ensure all regions receive adequate attention?

### Expected Answer

**1. Latency Optimization:**

**Architecture:**

```
Sensor Data → Kafka → Stream Processor (Flink/Spark) → 
Feature Store → Pre-computed Model → Alert API → Dispatch System
```

**Optimizations:**
- Pre-compute features in batch (rolling averages, baselines)
- Use online inference with cached models
- Implement approximate algorithms where exactness isn't critical

```python
# Feature store pattern
class FeatureStore:
    def __init__(self):
        self.cache = Redis()
        
    def get_features(self, dma_id, timestamp):
        # Try cache first
        features = self.cache.get(f"{dma_id}:{timestamp}")
        if features:
            return features
        
        # Compute on-demand (fallback)
        features = self.compute_features(dma_id, timestamp)
        self.cache.setex(f"{dma_id}:{timestamp}", 3600, features)
        return features
```

**2. Uncertainty Quantification:**

Use ensemble uncertainty:

```python
# Monte Carlo dropout for uncertainty
predictions = []
for _ in range(100):
    pred = model.predict(X, training=True)  # Enable dropout
    predictions.append(pred)

mean_pred = np.mean(predictions)
uncertainty = np.std(predictions)

# Decision threshold with uncertainty
if mean_pred > 0.7 and uncertainty < 0.1:
    priority = "HIGH"
elif mean_pred > 0.5:
    priority = "MEDIUM"
else:
    priority = "LOW"
```

**3. Fairness Constraints:**

Add equity constraints to optimization:

$$
\sum_{i \in R_j} \sum_{k} x_{i,k} \geq Q_j \quad \forall j \in \text{Regions}
$$

Where $Q_j$ is a minimum service quota for region $j$.

Alternatively, use weighted objective:

$$
\max \sum_i EV_i \cdot x_i + \lambda \cdot \min_j \left( \sum_{i \in R_j} x_i \right)
$$

This maximizes both total value and minimum regional coverage.

### Part D: Feedback Loop

**How does the system learn and improve over time?**

### Expected Answer

**Feedback Loop Design:**

```
┌─────────────────────────────────────────────────────────┐
│                    FEEDBACK LOOP                        │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  1. PREDICTION → Action taken (repair scheduled)        │
│         ↓                                               │
│  2. OUTCOME → Leak confirmed? Repair successful?        │
│         ↓                                               │
│  3. LABEL → True positive / False positive /            │
│             True negative / False negative              │
│         ↓                                               │
│  4. UPDATE → Retrain model with new labels              │
│         ↓                                               │
│  5. EVALUATE → Track precision, recall, cost savings    │
│         ↓                                               │
│  6. ADJUST → Update thresholds, features, model         │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

**Continuous Learning Pipeline:**

```python
class ContinuousLearningPipeline:
    def __init__(self):
        self.model = load_model()
        self.feedback_buffer = []
        self.retrain_threshold = 1000
        
    def process_feedback(self, dma_id, prediction, outcome):
        """
        outcome: {'leak_confirmed': True/False, 
                  'repair_cost': X, 
                  'water_saved': Y}
        """
        self.feedback_buffer.append({
            'dma_id': dma_id,
            'features': self.get_historical_features(dma_id),
            'prediction': prediction,
            'outcome': outcome
        })
        
        # Trigger retraining if enough new data
        if len(self.feedback_buffer) >= self.retrain_threshold:
            self.retrain()
    
    def retrain(self):
        # Prepare training data
        X = [f['features'] for f in self.feedback_buffer]
        y = [f['outcome']['leak_confirmed'] for f in self.feedback_buffer]
        
        # Sample weights based on recency
        weights = np.exp(-0.01 * np.arange(len(y))[::-1])
        
        # Retrain with new data
        new_model = XGBClassifier()
        new_model.fit(X, y, sample_weight=weights)
        
        # A/B test before deployment
        if self.validate(new_model):
            self.deploy(new_model)
            self.feedback_buffer = []
```

**Key Metrics to Track:**

| Metric | Target | Business Impact |
|---|---|---|
| Precision | > 80% | Minimize wasted crew dispatches |
| Recall | > 70% | Catch majority of leaks |
| Cost per leak found | < £2,000 | Operational efficiency |
| Water saved per £ spent | > 5:1 | ROI |
| Time to detection | < 7 days | Minimize water loss |

### Why This Matters

This question tests the candidate's ability to integrate multiple ML techniques into a production system that delivers measurable business value. The mathematics of predictive, prescriptive, and optimization layers must work together seamlessly. In utilities, such systems can reduce leakage by 10-20%, saving millions of pounds and millions of litres of water annually.

---

## APPENDIX: Evaluation Rubric

| Question | Key Concepts Tested | Expected Depth |
|---|---|---|
| 1. Time Series | SARIMAX, seasonality, state-space | Can derive equations |
| 2. Anomaly Detection | Isolation Forest, cost-sensitive learning | Understands operational trade-offs |
| 3. Churn Prediction | Survival analysis, censoring | Can explain why survival > classification |
| 4. Interpretability | SHAP, axiomatic foundations | Understands regulatory implications |
| 5. A/B Testing | MDE, interference, stratification | Can design valid experiments |
| 6. Bayesian Methods | Posterior derivation, prior elicitation | Knows when to apply each approach |
| 7. Bias-Variance | Decomposition, overfitting diagnosis | Can diagnose and remediate |
| 8. Feature Engineering | Lags, rolling stats, Fourier | Creates domain-relevant features |
| 9. Cross-Validation | Temporal validation, leakage | Understands why standard CV fails |
| 10. When Not to Use ML | Decision framework, alternatives | Shows business judgment |
| 11. Optimization | KKT conditions, LP formulation | Can formulate real problems |
| 12. Concept Drift | Drift types, detection tests | Designs monitoring systems |
| 13. End-to-End System | Integration, feedback loops | Systems thinking |

---

*"The mathematics do not lie. In the regulated utilities sector, our models must be both accurate and accountable. These questions separate those who can implement algorithms from those who understand the underlying principles and can apply them to real-world challenges."*

---

**Document prepared by Professor Algorithma, Sage of Machine Learning and Statistical Inference**
