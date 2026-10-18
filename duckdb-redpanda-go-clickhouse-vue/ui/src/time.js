// Event times are shown on the exchange clock, whatever the browser's zone.
export const ZONE = 'Europe/Berlin'

const fmt = (opts) => new Intl.DateTimeFormat('en-GB', { timeZone: ZONE, hourCycle: 'h23', ...opts })
const hm = fmt({ hour: '2-digit', minute: '2-digit' })
const dayHm = fmt({ weekday: 'short', day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit' })
const day = fmt({ weekday: 'short', day: '2-digit', month: 'short' })
const full = fmt({ year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' })

export const formatHM = (ms) => hm.format(ms)
export const formatDayHM = (ms) => dayHm.format(ms)
export const formatDay = (ms) => day.format(ms)
export const formatFull = (ms) => full.format(ms).replace(',', '')
