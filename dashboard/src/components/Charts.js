import React from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  ArcElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';
import { Line, Bar, Pie } from 'react-chartjs-2';

// Register Chart.js components
ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  ArcElement,
  Title,
  Tooltip,
  Legend
);

export function CarrierChart({ data }) {
  const chartData = {
    labels: data.map(d => d.carrier_name),
    datasets: [
      {
        label: 'Total Shipments',
        data: data.map(d => d.total_shipments),
        backgroundColor: 'rgba(54, 162, 235, 0.6)',
      },
    ],
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { display: false },
      title: { display: true, text: 'Shipments by Carrier' },
    },
  };

  return <Bar data={chartData} options={options} height={300} />;
}

export function CategoryChart({ data }) {
  const chartData = {
    labels: data.slice(0, 6).map(d => d.category_name),
    datasets: [
      {
        label: 'Sales',
        data: data.slice(0, 6).map(d => parseFloat(d.total_sales)),
        backgroundColor: [
          'rgba(255, 99, 132, 0.6)',
          'rgba(54, 162, 235, 0.6)',
          'rgba(255, 206, 86, 0.6)',
          'rgba(75, 192, 192, 0.6)',
          'rgba(153, 102, 255, 0.6)',
          'rgba(255, 159, 64, 0.6)',
        ],
      },
    ],
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { position: 'right' },
      title: { display: true, text: 'Sales by Category' },
    },
  };

  return <Pie data={chartData} options={options} height={300} />;
}

export function HourlyChart({ data }) {
  const chartData = {
    labels: data.map(d => new Date(d.hour_timestamp).toLocaleTimeString()),
    datasets: [
      {
        label: 'Shipments',
        data: data.map(d => d.shipment_count),
        borderColor: 'rgb(75, 192, 192)',
        backgroundColor: 'rgba(75, 192, 192, 0.2)',
        tension: 0.1,
      },
    ],
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { display: false },
      title: { display: true, text: 'Shipments Over Time' },
    },
    scales: {
      y: { beginAtZero: true },
    },
  };

  return <Line data={chartData} options={options} height={250} />;
}