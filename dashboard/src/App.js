import React, { useState, useEffect } from 'react';
import Navbar from './components/Navbar';
import { CarrierChart, CategoryChart, HourlyChart } from './components/Charts';
import api from './services/api';
import './App.css';

function App() {
  const [stats, setStats] = useState(null);
  const [carriers, setCarriers] = useState([]);
  const [categories, setCategories] = useState([]);
  const [cities, setCities] = useState([]);
  const [hourly, setHourly] = useState([]);
  const [recentShipments, setRecentShipments] = useState([]);
  const [delayedShipments, setDelayedShipments] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 3000);
    return () => clearInterval(interval);
  }, []);

  const fetchData = async () => {
  try {
    const [
      statsRes,
      carriersRes,
      categoriesRes,
      citiesRes,
      hourlyRes,
      recentRes,
      delayedRes
    ] = await Promise.all([
      api.getStats(),
      api.getCarriers(),
      api.getCategories(),
      api.getCities(),
      api.getHourlyStats(),
      api.getRecentShipments(10),
      api.getDelayedShipments(10)
    ]);

    setStats(statsRes.data.data);
    setCarriers(carriersRes.data.data || []);
    setCategories(categoriesRes.data.data || []);
    setCities(citiesRes.data.data || []);
    setHourly(hourlyRes.data.data || []);
    setRecentShipments(recentRes.data.data || []);
    setDelayedShipments(delayedRes.data.data || []);
    setLoading(false);
  } catch (err) {
    console.error('Error:', err);
    setLoading(false);
  }
};

  return (
    <div className="App">
      <Navbar />

      <div className="container-fluid mt-4 px-4">
        {loading && (
          <div className="alert alert-info">
            <div className="spinner-border spinner-border-sm me-2"></div>
            Loading dashboard...
          </div>
        )}

        {/* KPI Cards */}
        <div className="row mb-4">
          <div className="col-md-3 mb-3">
            <div className="card text-white bg-primary">
              <div className="card-body">
                <h6 className="card-title">Total Shipments</h6>
                <h2>{stats?.total_shipments || 0}</h2>
                <small>Live updating</small>
              </div>
            </div>
          </div>
          <div className="col-md-3 mb-3">
            <div className="card text-white bg-success">
              <div className="card-body">
                <h6 className="card-title">Delivered</h6>
                <h2>{stats?.delivered || 0}</h2>
                <small>{((stats?.delivered / stats?.total_shipments) * 100 || 0).toFixed(1)}% of total</small>
              </div>
            </div>
          </div>
          <div className="col-md-3 mb-3">
            <div className="card text-white bg-warning text-dark">
              <div className="card-body">
                <h6 className="card-title">In Transit</h6>
                <h2>{stats?.in_transit || 0}</h2>
                <small>Currently shipping</small>
              </div>
            </div>
          </div>
          <div className="col-md-3 mb-3">
            <div className="card text-white bg-danger">
              <div className="card-body">
                <h6 className="card-title">Delayed</h6>
                <h2>{stats?.delayed || 0}</h2>
                <small>{((stats?.delayed / stats?.total_shipments) * 100 || 0).toFixed(1)}% delayed</small>
              </div>
            </div>
          </div>
        </div>

        {/* Secondary Metrics */}
        <div className="row mb-4">
          <div className="col-md-4 mb-3">
            <div className="card">
              <div className="card-body text-center">
                <h6 className="text-muted">Avg Delivery Days</h6>
                <h3 className="text-primary">{stats?.avg_delivery_days?.toFixed(1) || 0}</h3>
              </div>
            </div>
          </div>
          <div className="col-md-4 mb-3">
            <div className="card">
              <div className="card-body text-center">
                <h6 className="text-muted">On-Time Rate</h6>
                <h3 className="text-success">{stats?.on_time_delivery_rate?.toFixed(1) || 0}%</h3>
              </div>
            </div>
          </div>
          <div className="col-md-4 mb-3">
            <div className="card">
              <div className="card-body text-center">
                <h6 className="text-muted">Active Carriers</h6>
                <h3 className="text-info">{carriers.length}</h3>
              </div>
            </div>
          </div>
        </div>

        {/* Charts Row */}
        <div className="row mb-4">
          <div className="col-md-4 mb-3">
            <div className="card">
              <div className="card-body" style={{ height: '350px' }}>
                {carriers.length > 0 && <CarrierChart data={carriers} />}
              </div>
            </div>
          </div>
          <div className="col-md-4 mb-3">
            <div className="card">
              <div className="card-body" style={{ height: '350px' }}>
                {categories.length > 0 && <CategoryChart data={categories} />}
              </div>
            </div>
          </div>
          <div className="col-md-4 mb-3">
            <div className="card">
              <div className="card-body" style={{ height: '350px' }}>
                {hourly.length > 0 && <HourlyChart data={hourly} />}
              </div>
            </div>
          </div>
        </div>

        {/* Tables Row */}
        <div className="row mb-4">
          {/* Top Cities */}
          <div className="col-md-6 mb-3">
            <div className="card">
              <div className="card-header bg-info text-white">
                <h5 className="mb-0">Top 10 Cities by Shipments</h5>
              </div>
              <div className="card-body p-0" style={{ maxHeight: '400px', overflowY: 'auto' }}>
                <table className="table table-hover mb-0">
                  <thead className="table-light sticky-top">
                    <tr>
                      <th>#</th>
                      <th>City</th>
                      <th>State</th>
                      <th>Shipments</th>
                      <th>Sales</th>
                    </tr>
                  </thead>
                  <tbody>
                    {cities.slice(0, 10).map((city, idx) => (
                      <tr key={idx}>
                        <td>{idx + 1}</td>
                        <td>{city.city_name}</td>
                        <td>{city.state}</td>
                        <td>{city.total_shipments}</td>
                        <td>${parseFloat(city.total_sales).toFixed(0)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          {/* Carrier Performance */}
          <div className="col-md-6 mb-3">
            <div className="card">
              <div className="card-header bg-primary text-white">
                <h5 className="mb-0">Carrier Performance</h5>
              </div>
              <div className="card-body p-0">
                <table className="table table-hover mb-0">
                  <thead className="table-light">
                    <tr>
                      <th>Carrier</th>
                      <th>Shipments</th>
                      <th>Avg Days</th>
                      <th>On-Time %</th>
                    </tr>
                  </thead>
                  <tbody>
                    {carriers.map((carrier, idx) => (
                      <tr key={idx}>
                        <td><strong>{carrier.carrier_name}</strong></td>
                        <td>{carrier.total_shipments}</td>
                        <td>{carrier.avg_delivery_days?.toFixed(1)}</td>
                        <td>
                          <span className={`badge ${carrier.on_time_rate > 80 ? 'bg-success' : 'bg-warning'}`}>
                            {carrier.on_time_rate?.toFixed(1)}%
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>

        {/* Recent & Delayed Shipments */}
        <div className="row mb-4">
          {/* Recent Shipments */}
          <div className="col-md-6 mb-3">
            <div className="card">
              <div className="card-header bg-success text-white">
                <h5 className="mb-0">📦 Recent Shipments</h5>
              </div>
              <div className="card-body p-0" style={{ maxHeight: '400px', overflowY: 'auto' }}>
                <table className="table table-sm mb-0">
                  <thead className="table-light sticky-top">
                    <tr>
                      <th>ID</th>
                      <th>Customer</th>
                      <th>City</th>
                      <th>Mode</th>
                      <th>Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {recentShipments.slice(0, 10).map((shipment, idx) => (
                      <tr key={idx}>
                        <td><small>{shipment.shipment_id}</small></td>
                        <td><small>{shipment.customer_fname} {shipment.customer_lname}</small></td>
                        <td><small>{shipment.customer_city}</small></td>
                        <td><small>{shipment.shipping_mode}</small></td>
                        <td>
                          <span className={`badge ${shipment.late_delivery_risk ? 'bg-danger' : 'bg-success'}`}>
                            <small>{shipment.delivery_status}</small>
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          {/* Delayed Shipments Alert */}
          <div className="col-md-6 mb-3">
            <div className="card border-danger">
              <div className="card-header bg-danger text-white">
                <h5 className="mb-0">⚠️ Delayed Shipments</h5>
              </div>
              <div className="card-body p-0" style={{ maxHeight: '400px', overflowY: 'auto' }}>
                {delayedShipments.length > 0 ? (
                  <table className="table table-sm mb-0">
                    <thead className="table-light sticky-top">
                      <tr>
                        <th>ID</th>
                        <th>Customer</th>
                        <th>City</th>
                        <th>Delay</th>
                      </tr>
                    </thead>
                    <tbody>
                      {delayedShipments.map((shipment, idx) => (
                        <tr key={idx}>
                          <td><small>{shipment.shipment_id}</small></td>
                          <td><small>{shipment.customer_name}</small></td>
                          <td><small>{shipment.customer_city}</small></td>
                          <td><span className="badge bg-danger">{shipment.delay_days} days</span></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  <div className="p-4 text-center text-muted">
                    <p className="mb-0">✅ No delayed shipments - Great job!</p>
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="row mb-4">
          <div className="col-12">
            <div className="card bg-light">
              <div className="card-body text-center">
                <small className="text-muted">
                  🔄 Dashboard auto-refreshes every 3 seconds | 
                  Last updated: {new Date().toLocaleTimeString()}
                </small>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;