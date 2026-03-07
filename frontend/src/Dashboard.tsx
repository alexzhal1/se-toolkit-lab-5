import { useEffect, useState } from 'react'
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

interface ScoreBucket {
  bucket: string
  count: number
}

interface TimelineEntry {
  date: string
  submissions: number
}

interface PassRate {
  task: string
  avg_score: number
  attempts: number
}

interface LabItem {
  id: number
  title: string
  slug: string
}

function slugFromTitle(title: string): string {
  // titles look like "Lab 04 \u2014 Testing" or "Lab 5..."; grab first number sequence
  const m = title.match(/Lab\s*(\d+)/i)
  const num = m ? m[1].padStart(2, '0') : ''
  return num ? `lab-${num}` : ''
}

export default function Dashboard() {
  const [labs, setLabs] = useState<LabItem[]>([])
  const [selectedLab, setSelectedLab] = useState<string>('')
  const [scores, setScores] = useState<ScoreBucket[]>([])
  const [timeline, setTimeline] = useState<TimelineEntry[]>([])
  const [passRates, setPassRates] = useState<PassRate[]>([])
  const [loading, setLoading] = useState<boolean>(false)
  const [error, setError] = useState<string | null>(null)

  const token = localStorage.getItem(STORAGE_KEY) || ''

  useEffect(() => {
    if (!token) return
    // fetch labs
    fetch('/items', { headers: { Authorization: `Bearer ${token}` } })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        return res.json() as Promise<{
          id: number
          type: string
          title: string
        }[]>
      })
      .then((data) => {
        const labsOnly: LabItem[] = data
          .filter((it) => it.type === 'lab')
          .map((it) => ({
            id: it.id,
            title: it.title,
            slug: slugFromTitle(it.title),
          }))
        setLabs(labsOnly)
        if (labsOnly.length && !selectedLab) {
          setSelectedLab(labsOnly[0].slug)
        }
      })
      .catch((err) => setError(err.message))
  }, [token])

  useEffect(() => {
    if (!token || !selectedLab) return
    setLoading(true)
    setError(null)

    const headers = { Authorization: `Bearer ${token}` }
    const qs = `?lab=${encodeURIComponent(selectedLab)}`

    Promise.all([
      fetch(`/analytics/scores${qs}`, { headers }).then((r) => {
        if (!r.ok) throw new Error(`scores HTTP ${r.status}`)
        return r.json() as Promise<ScoreBucket[]>
      }),
      fetch(`/analytics/timeline${qs}`, { headers }).then((r) => {
        if (!r.ok) throw new Error(`timeline HTTP ${r.status}`)
        return r.json() as Promise<TimelineEntry[]>
      }),
      fetch(`/analytics/pass-rates${qs}`, { headers }).then((r) => {
        if (!r.ok) throw new Error(`pass-rates HTTP ${r.status}`)
        return r.json() as Promise<PassRate[]>
      }),
    ])
      .then(([s, t, p]) => {
        setScores(s)
        setTimeline(t)
        setPassRates(p)
      })
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false))
  }, [token, selectedLab])

  if (!token) {
    return <p>Please connect with an API key to view the dashboard.</p>
  }

  return (
    <div>
      <h1>Analytics Dashboard</h1>
      {error && <p style={{ color: 'red' }}>Error: {error}</p>}
      {loading && <p>Loading...</p>}

      <label>
        Select lab:{' '}
        <select
          value={selectedLab}
          onChange={(e) => setSelectedLab(e.target.value)}
        >
          <option value="" disabled>
            -- choose --
          </option>
          {labs.map((lab) => (
            <option key={lab.id} value={lab.slug}>
              {lab.title}
            </option>
          ))}
        </select>
      </label>

      {scores.length > 0 && (
        <div style={{ maxWidth: 600 }}>
          <h2>Score distribution</h2>
          <Bar
            data={{
              labels: scores.map((b) => b.bucket),
              datasets: [
                {
                  label: 'Count',
                  data: scores.map((b) => b.count),
                  backgroundColor: 'rgba(75, 192, 192, 0.5)',
                },
              ],
            }}
          />
        </div>
      )}

      {timeline.length > 0 && (
        <div style={{ maxWidth: 600 }}>
          <h2>Submissions per day</h2>
          <Line
            data={{
              labels: timeline.map((t) => t.date),
              datasets: [
                {
                  label: 'Submissions',
                  data: timeline.map((t) => t.submissions),
                  borderColor: 'rgba(53, 162, 235, 0.5)',
                  fill: false,
                },
              ],
            }}
          />
        </div>
      )}

      {passRates.length > 0 && (
        <div>
          <h2>Pass rates</h2>
          <table>
            <thead>
              <tr>
                <th>Task</th>
                <th>Avg score</th>
                <th>Attempts</th>
              </tr>
            </thead>
            <tbody>
              {passRates.map((p) => (
                <tr key={p.task}>
                  <td>{p.task}</td>
                  <td>{p.avg_score}</td>
                  <td>{p.attempts}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
