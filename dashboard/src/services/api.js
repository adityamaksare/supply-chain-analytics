import axios from 'axios';

const API_BASE_URL = 'http://localhost:5001/api';

const api = {
  health: () => axios.get('http://localhost:5001/health'),
  getStats: () => axios.get(`${API_BASE_URL}/stats`),
  getCarriers: () => axios.get(`${API_BASE_URL}/carriers`),
  getCategories: () => axios.get(`${API_BASE_URL}/categories`),
  getCities: () => axios.get(`${API_BASE_URL}/cities`),
  getHourlyStats: () => axios.get(`${API_BASE_URL}/hourly`),
  getRecentShipments: (limit = 20) => axios.get(`${API_BASE_URL}/recent-shipments?limit=${limit}`),
  getDelayedShipments: (limit = 50) => axios.get(`${API_BASE_URL}/delayed-shipments?limit=${limit}`),
  getShipmentDetail: (shipmentId) => axios.get(`${API_BASE_URL}/shipments/${shipmentId}`),
  getRoutes: () => axios.get(`${API_BASE_URL}/routes`), // NEW
};

export default api;