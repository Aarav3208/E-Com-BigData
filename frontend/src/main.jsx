import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import "./style.css";

const dataFiles = ["analytics", "customer_segments", "products", "recommendations", "evaluation"];

function App() {
  const [data, setData] = useState({});
  const [customer, setCustomer] = useState("");

  useEffect(() => {
    Promise.all(dataFiles.map((name) => fetch(`/data/${name}.json`).then((response) => response.json())))
      .then((values) => setData(Object.fromEntries(dataFiles.map((name, index) => [name, values[index]]))))
      .catch((error) => console.error("Unable to load exported analytics", error));
  }, []);

  const recommendations = useMemo(
    () => (data.recommendations || []).filter((item) => item.CustomerID === customer),
    [data.recommendations, customer],
  );
  const summary = data.analytics || {};
  const metric = (name, key) => summary[name]?.[0]?.[key] ?? "—";

  return <main>
    <header><h1>E-Commerce Analytics</h1><p>Static Spark exports and personalized ALS recommendations</p></header>
    <section className="cards">
      <Card title="Revenue" value={metric("total_revenue", "TotalRevenue")} />
      <Card title="Orders" value={metric("total_orders", "TotalOrders")} />
      <Card title="Customers" value={metric("total_customers", "TotalCustomers")} />
      <Card title="Products" value={metric("total_products", "TotalProducts")} />
    </section>
    <section className="panel"><h2>New products recommended from prior purchases</h2>
      <select value={customer} onChange={(event) => setCustomer(event.target.value)}>
        <option value="">Select a customer</option>
        {[...new Set((data.recommendations || []).map((item) => item.CustomerID))].sort().map((id) => <option key={id}>{id}</option>)}
      </select>
      {customer && <p className="hint">These products were not previously purchased by this customer. Recommendations are based on similar purchase patterns learned by ALS.</p>}
      <ul>{recommendations.map((item) => <li key={`${item.CustomerID}-${item.StockCode}`}><span className="badge">{item.RecommendationType || "New product"}</span> {item.StockCode} — {item.Description || "Product"} <strong>Score: {Number(item.score).toFixed(2)}</strong></li>)}</ul>
    </section>
    <section className="columns">
      <Table title="Monthly revenue" rows={summary.monthly_revenue || []} columns={["Year", "Month", "TotalRevenue"]} />
      <Table title="Top products" rows={summary.top_products_by_revenue || []} columns={["StockCode", "Description", "TotalRevenue"]} />
      <Table title="Customer segments" rows={data.customer_segments || []} columns={["CustomerID", "Segment", "Monetary"]} />
    </section>
  </main>;
}

function Card({ title, value }) { return <article className="card"><span>{title}</span><strong>{value}</strong></article>; }
function Table({ title, rows, columns }) {
  return <article className="panel"><h2>{title}</h2><table><thead><tr>{columns.map((column) => <th key={column}>{column}</th>)}</tr></thead><tbody>{rows.slice(0, 10).map((row, index) => <tr key={index}>{columns.map((column) => <td key={column}>{row[column] ?? "—"}</td>)}</tr>)}</tbody></table></article>;
}

createRoot(document.getElementById("root")).render(<App />);
