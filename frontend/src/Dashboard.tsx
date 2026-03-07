import React, { useEffect, useState } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
)

const STORAGE_KEY = 'api_key'

// Strict TypeScript interfaces for API responses
interface ScoreBucket {
  bucket: string
  count: number
}

interface TimelinePoint {
  date: string // ISO date 'YYYY-MM-DD'
  submissions: number
}

interface PassRateItem {
  task: string
  avg_score: number
  attempts: number
}

async function fetchJson<T>(url: string, token: string | null): Promise<T> {
  const headers: Record<string, string> = {}
  if (token) headers.Authorization = `Bearer ${token}`
  const res = await fetch(url, { headers })
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  const data = (await res.json()) as T
  return data
}

export default function Dashboard(): JSX.Element {
  const [scores, setScores] = useState<ScoreBucket[] | null>(null)
  const [timeline, setTimeline] = useState<TimelinePoint[] | null>(null)
  const [passRates, setPassRates] = useState<PassRateItem[] | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const token = localStorage.getItem(STORAGE_KEY)
    const base = '/analytics'

    Promise.all([
      fetchJson<ScoreBucket[]>(`${base}/scores?lab=lab-04`, token),
      fetchJson<TimelinePoint[]>(`${base}/timeline?lab=lab-04`, token),
      fetchJson<PassRateItem[]>(`${base}/pass-rates?lab=lab-04`, token),
    ])
      .then(([s, t, p]) => {
        setScores(s)
        // ensure timeline sorted by date ascending
        setTimeline(t.slice().sort((a, b) => (a.date < b.date ? -1 : 1)))
        setPassRates(p)
      })
      .catch((err: unknown) => {
        const msg = err instanceof Error ? err.message : String(err)
        setError(msg)
      })
  }, [])

  if (error) {
    return (
      <div>
        <h2>Dashboard</h2>
        <p style={{ color: 'red' }}>Error: {error}</p>
      </div>
    )
  }

  const scoreBucketsOrder = ['0-25', '26-50', '51-75', '76-100']
  const scoreCounts = scoreBucketsOrder.map((b) =>
    scores ? scores.find((s) => s.bucket === b)?.count ?? 0 : 0,
  )

  const barData = {
    labels: scoreBucketsOrder,
    datasets: [
      {
        label: 'Submissions',
        data: scoreCounts,
        backgroundColor: ['#c92a2a', '#f08a24', '#ffd43b', '#37b24d'],
      },
    ],
  }

  const lineData = {
    labels: timeline ? timeline.map((p) => p.date) : [],
    datasets: [
      {
        label: 'Submissions',
        data: timeline ? timeline.map((p) => p.submissions) : [],
        borderColor: '#2563eb',
        backgroundColor: 'rgba(37,99,235,0.2)',
        fill: true,
        tension: 0.2,
      },
    ],
  }

  return (
    <div>
      <h2>Dashboard</h2>

      <section style={{ maxWidth: 700 }}>
        <h3>Score Distribution</h3>
        <Bar data={barData} />
      </section>

      <section style={{ maxWidth: 800, marginTop: 24 }}>
        <h3>Submissions Over Time</h3>
        <Line data={lineData} />
      </section>

      <section style={{ marginTop: 24 }}>
        <h3>Per-task Averages</h3>
        <table>
          <thead>
            <tr>
              <th>Task</th>
              <th>Avg score</th>
              <th>Attempts</th>
            </tr>
          </thead>
          <tbody>
            {passRates && passRates.length > 0 ? (
              passRates.map((row) => (
                <tr key={row.task}>
                  <td>{row.task}</td>
                  <td>{row.avg_score.toFixed(1)}</td>
                  <td>{row.attempts}</td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={3}>No data</td>
              </tr>
            )}
          </tbody>
        </table>
      </section>
    </div>
  )
}
