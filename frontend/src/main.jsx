import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import "./style.css";

const dataFiles = ["analytics", "customer_segments", "products", "recommendations"];
const number = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
const decimal = new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 });
const percent = new Intl.NumberFormat("en-US", { style: "percent", maximumFractionDigits: 1 });
const money = (value) => value == null || !Number.isFinite(Number(value)) ? "—" : decimal.format(Number(value));
const count = (value) => value == null || !Number.isFinite(Number(value)) ? "—" : number.format(Number(value));

function App() {
  const [data, setData] = useState(null);
  const [error, setError] = useState("");
  const [customer, setCustomer] = useState("");
  const [recommendationLimit, setRecommendationLimit] = useState(10);
  const [year, setYear] = useState("all");
  const [segment, setSegment] = useState("all");
  const [customerSearch, setCustomerSearch] = useState("");

  useEffect(() => {
    Promise.all(dataFiles.map((name) => fetch(`/data/${name}.json`).then((response) => {
      if (!response.ok) throw new Error(`${name}.json could not be loaded`);
      return response.json();
    })))
      .then((values) => setData(Object.fromEntries(dataFiles.map((name, index) => [name, values[index]]))))
      .catch((loadError) => setError(loadError.message));
  }, []);

  const recommendationCustomers = useMemo(() => {
    const grouped = new Map();
    (data?.recommendations || []).forEach((row) => {
      const customerId = String(row.CustomerID);
      const score = Number(row.score);
      if (!Number.isFinite(score)) return;
      const current = grouped.get(customerId) || { id: customerId, totalScore: 0, count: 0 };
      current.totalScore += score;
      current.count += 1;
      grouped.set(customerId, current);
    });
    return [...grouped.values()]
      .map((item) => ({ ...item, averageScore: item.totalScore / item.count }))
      .sort((a, b) =>
        b.averageScore - a.averageScore ||
        a.id.localeCompare(b.id, undefined, { numeric: true }));
  }, [data?.recommendations]);

  if (error) return <main><Notice title="Unable to load dashboard data" detail={error} /></main>;
  if (!data) return <main><Notice title="Loading analytics…" /></main>;

  const analytics = data.analytics || {};
  const report = (name) => analytics[name] || [];
  const totalRevenue = report("total_revenue")[0]?.TotalRevenue;
  const totalOrders = report("total_orders")[0]?.TotalOrders;
  const totalCustomers = report("total_customers")[0]?.TotalCustomers;
  const totalProducts = report("total_products")[0]?.TotalProducts;
  const averageOrderValue = report("average_order_value")[0]?.AverageOrderValue;
  const years = [...new Set(report("monthly_revenue").map((row) => String(row.Year)))].sort();
  const monthly = report("monthly_revenue").filter((row) => year === "all" || String(row.Year) === year);
  const monthlyOrders = report("monthly_orders").filter((row) => year === "all" || String(row.Year) === year);
  const behavior = report("customer_behavior_summary")[0] || {};
  const segments = report("segment_summary");
  const visibleSegments = (data.customer_segments || []).filter((row) =>
    (segment === "all" || row.Segment === segment) &&
    String(row.CustomerID).toLowerCase().includes(customerSearch.toLowerCase()));
  const selectedRecommendations = (data.recommendations || [])
    .filter((row) => row.CustomerID === customer)
    .sort((a, b) => Number(a.rank) - Number(b.rank))
    .slice(0, recommendationLimit);
  const highestMonth = monthly.reduce((best, row) => !best || row.TotalRevenue > best.TotalRevenue ? row : best, null);
  const lowestMonth = monthly.reduce((best, row) => !best || row.TotalRevenue < best.TotalRevenue ? row : best, null);
  const topProduct = report("top_products_by_revenue")[0];
  const frequentProduct = report("top_products_by_quantity")[0];
  const largestSegment = segments[0];
  const highestSegmentRevenue = [...segments].sort((a, b) => Number(b.SegmentRevenue || 0) - Number(a.SegmentRevenue || 0))[0];

  return <main>
    <header className="hero">
      <div><p className="eyebrow">Spark-generated analytics</p><h1>E-Commerce Analytics</h1><p>Sales, product, customer, and recommendation intelligence from cleaned Online Retail II transactions.</p></div>
    </header>

    <Section title="Overview" subtitle="All figures use valid, non-cancelled cleaned transactions.">
      <div className="cards">
        <Card title="Total revenue" value={money(totalRevenue)} />
        <Card title="Total orders" value={count(totalOrders)} />
        <Card title="Unique customers" value={count(totalCustomers)} />
        <Card title="Unique products" value={count(totalProducts)} />
        <Card title="Average order value" value={money(averageOrderValue)} />
      </div>
    </Section>

    <Section title="Sales analytics" actions={<label>Year <select value={year} onChange={(event) => setYear(event.target.value)}><option value="all">All years</option>{years.map((item) => <option key={item}>{item}</option>)}</select></label>}>
      <div className="grid two"><Panel title="Monthly revenue"><LineChart rows={monthly} x={(row) => `${row.Year}-${String(row.Month).padStart(2, "0")}`} y="TotalRevenue" /></Panel><Panel title="Monthly order volume"><LineChart rows={monthlyOrders} x={(row) => `${row.Year}-${String(row.Month).padStart(2, "0")}`} y="TotalOrders" /></Panel></div>
      <div className="insight-row"><Insight label="Highest-revenue month" value={highestMonth ? `${highestMonth.Year}-${highestMonth.Month}: ${money(highestMonth.TotalRevenue)}` : "Unavailable"} /><Insight label="Lowest-revenue month" value={lowestMonth ? `${lowestMonth.Year}-${lowestMonth.Month}: ${money(lowestMonth.TotalRevenue)}` : "Unavailable"} /></div>
    </Section>

    <Section title="Product analytics">
      <div className="grid two"><Panel title="Top 10 products by revenue"><BarChart rows={report("top_products_by_revenue")} label="Description" value="TotalRevenue" /></Panel><Panel title="Top 10 products by quantity sold"><BarChart rows={report("top_products_by_quantity")} label="Description" value="TotalQuantitySold" /></Panel></div>
      <Panel title="Product details"><DataTable rows={report("top_products_by_revenue")} columns={[["StockCode", "Product ID"], ["Description", "Product"], ["TotalRevenue", "Revenue"]]} format={(key, value) => key === "TotalRevenue" ? money(value) : value} /></Panel>
    </Section>

    <Section title="Customer segmentation" subtitle="RFM rules: High-Value (recency ≤90 days, frequency ≥5, monetary ≥500), Regular (≤180, ≥2, ≥100), Occasional (≤365), otherwise Inactive.">
      <div className="grid two"><Panel title="Segment distribution"><BarChart rows={segments} label="Segment" value="CustomerCount" /></Panel><Panel title="Segment summary"><DataTable rows={segments} columns={[["Segment", "Segment"], ["CustomerCount", "Customers"], ["CustomerPercentage", "Share"], ["SegmentRevenue", "Revenue"]]} format={(key, value) => key === "Segment" ? value : key === "CustomerPercentage" ? (Number.isFinite(Number(value)) ? percent.format(Number(value)) : "—") : key === "SegmentRevenue" ? money(value) : count(value)} /></Panel></div>
      <Panel title="Customer RFM details" actions={<div className="filters"><input aria-label="Search customer ID" placeholder="Search customer ID" value={customerSearch} onChange={(event) => setCustomerSearch(event.target.value)} /><select value={segment} onChange={(event) => setSegment(event.target.value)}><option value="all">All segments</option>{segments.map((item) => <option key={item.Segment}>{item.Segment}</option>)}</select></div>}><DataTable rows={visibleSegments.slice(0, 10)} columns={[["CustomerID", "Customer ID"], ["Segment", "Segment"], ["Recency", "Recency"], ["Frequency", "Frequency"], ["Monetary", "Monetary"]]} format={(key, value) => key === "Monetary" ? money(value) : value} /></Panel>
    </Section>

    <Section title="Customer purchasing behavior">
      <div className="cards compact"><Card title="Repeat customers" value={count(behavior.RepeatCustomers)} /><Card title="One-time customers" value={count(behavior.OneTimeCustomers)} /><Card title="Repeat purchase rate" value={behavior.RepeatPurchaseRate == null ? "—" : percent.format(Number(behavior.RepeatPurchaseRate))} /><Card title="Average orders/customer" value={money(behavior.AverageOrdersPerCustomer)} /><Card title="Average customer spending" value={money(behavior.AverageCustomerSpending)} /></div>
      <Panel title="Purchase-frequency distribution"><BarChart rows={report("purchase_frequency_distribution")} label="TotalOrders" value="CustomerCount" xPrefix="orders: " /></Panel>
      <p className="definition">Repeat customers placed more than one distinct order. One-time customers placed exactly one distinct order. Purchase frequency counts distinct invoices, not transaction lines.</p>
    </Section>

    <Section title="Personalized ALS recommendations" subtitle="New products are filtered against each customer's complete purchase history. The score is a model score, not a probability." actions={<label>Show <select value={recommendationLimit} onChange={(event) => setRecommendationLimit(Number(event.target.value))}><option value="5">5</option><option value="10">10</option><option value="20">20</option></select></label>}>
      <select value={customer} onChange={(event) => setCustomer(event.target.value)}><option value="">Select a customer</option>{recommendationCustomers.map((item) => <option key={item.id} value={item.id}>{item.id}</option>)}</select>
      {customer && selectedRecommendations.length ? <ul className="recommendations">{selectedRecommendations.map((item) => <li key={`${item.CustomerID}-${item.StockCode}`}><span className="badge">{item.RecommendationType || "New product"}</span><span><strong>{item.StockCode}</strong> — {item.Description || "Unnamed product"}</span><span title="ALS predicted preference score">Score: {decimal.format(Number(item.score))}</span></li>)}</ul> : <Empty text={customer ? "No unseen recommendations are available for this customer." : "Select a customer to view precomputed recommendations."} />}
    </Section>

    <Section title="Business insights"><div className="insight-grid"><Insight label="Top product by revenue" value={topProduct ? `${topProduct.Description || topProduct.StockCode} (${money(topProduct.TotalRevenue)})` : "Unavailable"} /><Insight label="Most frequently purchased product" value={frequentProduct ? `${frequentProduct.Description || frequentProduct.StockCode} (${count(frequentProduct.TotalQuantitySold)} units)` : "Unavailable"} /><Insight label="Largest customer segment" value={largestSegment ? `${largestSegment.Segment} (${percent.format(Number(largestSegment.CustomerPercentage))})` : "Unavailable"} /><Insight label="Segment with most revenue" value={highestSegmentRevenue ? `${highestSegmentRevenue.Segment} (${money(highestSegmentRevenue.SegmentRevenue)})` : "Unavailable"} /><Insight label="Repeat-customer rate" value={behavior.RepeatPurchaseRate == null ? "Unavailable" : percent.format(Number(behavior.RepeatPurchaseRate))} /></div></Section>
  </main>;
}

function Section({ title, subtitle, actions, children }) { return <section><div className="section-heading"><div><h2>{title}</h2>{subtitle && <p>{subtitle}</p>}</div>{actions}</div>{children}</section>; }
function Panel({ title, actions, children }) { return <article className="panel"><div className="panel-heading"><h3>{title}</h3>{actions}</div>{children}</article>; }
function Card({ title, value }) { return <article className="card"><span>{title}</span><strong>{value}</strong></article>; }
function Insight({ label, value }) { return <article className="insight"><span>{label}</span><strong>{value}</strong></article>; }
function Notice({ title, detail }) { return <div className="notice"><h2>{title}</h2>{detail && <p>{detail}</p>}</div>; }
function Empty({ text }) { return <div className="empty">{text}</div>; }
function DataTable({ rows, columns, format = (_, value) => value ?? "—" }) { if (!rows.length) return <Empty text="No data available." />; return <div className="table-wrap"><table><thead><tr>{columns.map(([, label]) => <th key={label}>{label}</th>)}</tr></thead><tbody>{rows.map((row, index) => <tr key={`${row.CustomerID || row.StockCode || row.metric || index}`}>{columns.map(([key]) => <td key={key}>{format(key, row[key])}</td>)}</tr>)}</tbody></table></div>; }
function LineChart({ rows, x, y }) { if (!rows.length) return <Empty text="No data available for this filter." />; const max = Math.max(...rows.map((row) => Number(row[y]) || 0), 1); const points = rows.map((row, index) => `${(index / Math.max(rows.length - 1, 1)) * 100},${96 - ((Number(row[y]) || 0) / max) * 82}`).join(" "); return <div className="chart"><svg viewBox="0 0 100 100" role="img" aria-label="Line chart"><polyline points={points} fill="none" stroke="#2b6de0" strokeWidth="1.8" vectorEffect="non-scaling-stroke" />{rows.map((row, index) => <circle key={index} cx={(index / Math.max(rows.length - 1, 1)) * 100} cy={96 - ((Number(row[y]) || 0) / max) * 82} r="1.4" fill="#2b6de0"><title>{x(row)}: {row[y]}</title></circle>)}</svg><div className="chart-labels"><span>{x(rows[0])}</span><span>{x(rows[rows.length - 1])}</span></div></div>; }
function BarChart({ rows, label, value, xPrefix = "" }) { if (!rows.length) return <Empty text="No data available." />; const max = Math.max(...rows.map((row) => Number(row[value]) || 0), 1); return <div className="bars">{rows.slice(0, 10).map((row, index) => <div className="bar-row" key={index}><span title={row[label]}>{xPrefix}{row[label] || "Unknown"}</span><div><i style={{ width: `${((Number(row[value]) || 0) / max) * 100}%` }} /><b>{count(row[value])}</b></div></div>)}</div>; }

createRoot(document.getElementById("root")).render(<App />);
