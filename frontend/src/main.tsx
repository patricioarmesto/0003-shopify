import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
import './index.css'

// StrictMode ejecuta los efectos dos veces en desarrollo para ayudar a
// detectar efectos no limpios. Acá es inofensivo porque los timers se limpian
// en el return de useEffect.
ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
