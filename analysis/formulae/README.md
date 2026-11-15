# Silver and Gold formulae

Mathematical forms of the calculations implemented by the Silver and Gold layers.
Times are UTC instants; elapsed-time quantities are in seconds unless noted.

## Silver

### Visit time and delay

For visit \(i\), choose the actual event and its corresponding aimed event:

\[
t_i^{\mathrm{visit}} =
\begin{cases}
t_i^{\mathrm{actual\ departure}}, & \text{if available},\\
t_i^{\mathrm{actual\ arrival}}, & \text{otherwise},
\end{cases}
\qquad
d_i = t_i^{\mathrm{visit}} - t_i^{\mathrm{aimed}}.
\]

\[
\mathrm{outlier}_i =
(d_i < -1800) \lor (d_i > 10800),
\qquad
\mathrm{early}: d_i < -60,\quad
\mathrm{on\ time}: -60 \le d_i < 300,\quad
\mathrm{late}: d_i \ge 300.
\]

A Gold-valid visit has an actual record, a non-outlier delay, and a defined \(d_i\).

### Headways

For consecutive trips \(i-1,i\) at the same line, direction, and stop:

\[
h_i = t_i^{\mathrm{visit}} - t_{i-1}^{\mathrm{visit}},
\qquad
h_i^{S} = t_i^{\mathrm{aimed}} - t_{i-1}^{\mathrm{aimed}},
\qquad
r_i = \frac{h_i}{h_i^{S}}.
\]

\[
\mathrm{bunched}_i = (r_i < 0.25),\qquad
\mathrm{gapped}_i = (r_i > 2).
\]

### Stop-to-stop distance and speed

For coordinates \((\phi_1,\lambda_1),(\phi_2,\lambda_2)\), with angles in radians,
\(R=6{,}371{,}000\ \mathrm{m}\):

\[
a = \sin^2\!\left(\frac{\phi_2-\phi_1}{2}\right)
+ \cos(\phi_1)\cos(\phi_2)\sin^2\!\left(\frac{\lambda_2-\lambda_1}{2}\right),
\qquad
D = 2R\arcsin(\sqrt{a}).
\]

For departure at the first stop and arrival at the next:

\[
\Delta t = t_{\mathrm{next\ arrival}} - t_{\mathrm{departure}},
\qquad
v_{\mathrm{segment}} = 3.6\frac{D}{\Delta t}\quad \mathrm{km/h}.
\]

Silver marks a segment valid when \(0 < \Delta t \le 3600\) s and neither endpoint
is a delay outlier.

### GPS-derived speed

For a local flat coordinate grid around \((\phi_0,\lambda_0)\), with coordinates in degrees:

\[
x = 111{,}320\cos\!\left(\frac{\pi\phi_0}{180}\right)(\lambda-\lambda_0),\qquad
y = 111{,}320(\phi-\phi_0),\qquad
\Delta s_j = \sqrt{(x_j-x_{j-1})^2+(y_j-y_{j-1})^2}.
\]

For consecutive pings \(j-1,j\), \(5 \le \Delta t_j \le 90\) s:

\[
v_j = \frac{\Delta s_j}{\Delta t_j}\quad \mathrm{m/s},\qquad
v_i^{\min} = \min(v_i^{\mathrm{in}},v_i^{\mathrm{out}}).
\]

The local cruise speed \(c_i\) is the centered rolling 75th percentile of up to
11 neighboring incoming step speeds (at least 5 samples):

\[
c_i = Q_{0.75}(v_j),\qquad
\rho_i = \frac{v_i^{\min}}{c_i}\quad\text{when }c_i\ge3\ \mathrm{m/s}.
\]

### GPS evidence that a stop was visited

For each observed call at \((x_s,y_s,t_s)\), use pings within \(60\) s of \(t_s\).
A ping is near the stop when:

\[
|t_j-t_s|\le60\ \mathrm{s},\qquad
\delta_j = \sqrt{(x_j-x_s)^2+(y_j-y_s)^2}\le30\ \mathrm{m}.
\]

Thus \(n_{\mathrm{near}}=\sum_j\mathbf{1}[\delta_j\le30]\) over pings in the time window.
For near pings, define the slow-ping count:

\[
n_{\mathrm{slow}} =
\sum_j \mathbf{1}\!\left[
\delta_j\le30 \ \land\
\left(v_j^{\min}\le2\ \mathrm{m/s}\ \lor\ \rho_j\le0.75\right)
\right].
\]

The production classification is applied in this precedence order:

\[
\begin{array}{ll}
\texttt{no\_vehicle} & \text{no trip vehicle assignment},\\
\texttt{no\_gps} & \text{no usable GPS interpolation at }t_s,\\
\texttt{stopped\_confirmed} & n_{\mathrm{slow}}\ge2,\\
\texttt{likely\_stopped} & n_{\mathrm{slow}}=1,\\
\texttt{likely\_passed} & n_{\mathrm{near}}>0\ \land\ \min(\rho_j\mid\delta_j\le30)\ge0.90,\\
\texttt{near\_unclear} & n_{\mathrm{near}}>0\text{ and none of the above},\\
\texttt{no\_ping\_at\_stop} & \text{otherwise}.
\end{array}
\]

The trip vehicle is the candidate with the smallest median interpolated distance to
the trip's observed stops, among candidates with at least 3 matched stops; assignment
is accepted only when that median is at most 100 m.

These are GPS evidence classes, not proof of door opening or passenger boarding.
In particular, \(\texttt{no\_ping\_at\_stop}\) is unknown evidence, not evidence that
the bus passed.

### Route-based stop time-loss evidence

Let \(s_j\) be the bus's projected, non-decreasing position along the route. For
consecutive pings:

\[
\Delta s_j = s_j-s_{j-1},\qquad
c_j = Q_{0.75}\!\left(\frac{\Delta s}{\Delta t}\right)
\text{ over the centered 11-step window},
\qquad
L_j = \Delta t_j-\frac{\Delta s_j}{c_j}.
\]

For a stop at route position \(s_i\), the stop zone is
\([s_i-25,s_i+25]\) m. Its estimated time loss is the largest finite \(L_j\) for
an interval intersecting that zone:

\[
L_i^{\mathrm{stop}} = \max_{j:\,[s_{j-1},s_j]\cap[s_i-25,s_i+25]\ne\varnothing} L_j.
\]

Let \(L_k^0\) be the finite time losses for stop-free intervals on the trip and
\(N=|\{k\}|\). The empirical excess-loss probability is:

\[
p_{\mathrm{excess},i}
= \frac{\#\{k:L_k^0\ge L_i^{\mathrm{stop}}\}+1}{N+1}.
\]

### Older v2 reference heuristic

The v2 reference notebook uses dwell time rather than GPS speed evidence:

\[
\mathrm{dwell}_i =
t_i^{\mathrm{actual\ departure}}-t_i^{\mathrm{actual\ arrival}},
\qquad
\begin{cases}
\texttt{unknown}, & \mathrm{dwell}_i\text{ missing},\\
\texttt{likely\_passed}, & \mathrm{dwell}_i\le2\ \mathrm{s},\\
\texttt{likely\_stopped}, & \mathrm{dwell}_i>2\ \mathrm{s}.
\end{cases}
\]

This is a research/reference rule, not the production v6 GPS classifier above.

## Gold

### Segment and road speeds

For a valid run with segment distance \(D\) metres and elapsed time \(T\) seconds:

\[
v = 3.6\frac{D}{T}\quad\mathrm{km/h},\qquad
\bar v = 3.6\frac{\sum_i D_i}{\sum_i T_i}.
\]

Road runs used for Gold busy-road metrics satisfy
\(0<T\le1800\) s, \(D\ge50\) m, and \(0<v\le90\) km/h. For each stop pair,
free-flow speed is:

\[
v_{\mathrm{ff}}=Q_{0.85}(v_i),\qquad
T_{\mathrm{excess},i}
=\max\!\left(T_i-\frac{D_i}{v_{\mathrm{ff}}/3.6},0\right).
\]

\[
\mathrm{TTI}=\frac{v_{\mathrm{ff}}}{v_{\mathrm{peak}}},
\qquad
\mathrm{peak\ speed\ loss}=1-\frac{v_{\mathrm{peak}}}{v_{\mathrm{ff}}},
\qquad
\mathrm{vehicle\ hours\ lost}=\frac{\sum_i T_{\mathrm{excess},i}}{3600}.
\]

### Headway aggregates

Gold evaluates positive actual headways \(h_i\) where scheduled headway
\(120\le h_i^S\le3600\) s:

\[
\bar h=\frac{1}{n}\sum_i h_i,\qquad
\mathrm{CV}_h =
\frac{\sqrt{\max\!\left(\frac{1}{n}\sum_i h_i^2-\bar h^2,0\right)}}{\bar h},
\qquad
\mathrm{pct}_{\mathrm{bunched}} =
\frac{\#\{i:h_i<0.25h_i^S\}}{n}.
\]

\[
\mathrm{excess\ wait} =
\frac{\sum_i h_i^2}{2\sum_i h_i}
-\frac{\sum_i (h_i^S)^2}{2\sum_i h_i^S}.
\]

### Route reliability score

Across routes in the comparison period:

\[
N_d =
\begin{cases}
1-\dfrac{\bar d-\min(\bar d)}{\max(\bar d)-\min(\bar d)},&
\max(\bar d)>\min(\bar d),\\
1,&\text{otherwise},
\end{cases}
\qquad
N_v =
\begin{cases}
\dfrac{\bar v-\min(\bar v)}{\max(\bar v)-\min(\bar v)},&
\max(\bar v)>\min(\bar v),\\
1,&\text{otherwise},
\end{cases}
\]

\[
N_b=1-\operatorname{clip}(\mathrm{pct}_{\mathrm{bunched}},0,1).
\]

With \(I_b,I_v\in\{0,1\}\) indicating that the bunching and speed metrics exist:

\[
\mathrm{score}
=100\,
\frac{0.40\,\mathrm{pct}_{\mathrm{on\ time}}
+0.25N_d+0.20I_bN_b+0.15I_vN_v}
{0.40+0.25+0.20I_b+0.15I_v}.
\]

### Daily anomaly score

For a daily metric \(x_t\), let \(\mu_t,\sigma_t\) be the mean and sample standard
deviation of its preceding 7-day window:

\[
z_t=\frac{x_t-\mu_t}{\sigma_t},\qquad
\Delta_t=x_t-\mu_t,\qquad
\Delta_t^{\%}=\frac{x_t-\mu_t}{|\mu_t|}.
\]

An anomaly score is emitted only with at least 3 historical values and
\(\sigma_t>10^{-6}\).

## Source notebooks and implementations

- [Silver cleaning, headways, and segments](../silver-layer/spark_jobs/silver.py)
- [Silver GPS geometry and stop classes](../silver-layer/spark_jobs/stop_geometry.py)
- [Silver route time-loss comparison](../silver-layer/spark_jobs/stop_analysis.py)
- [Silver v6 research notebook](../notebooks/silver/research/analyzing_parquet_files_v6.ipynb)
- [Silver v2 reference notebook](../notebooks/silver/reference/building_silver_layer_v2.ipynb)
- [Gold formulas and thresholds](../gold/build_gold.py)
