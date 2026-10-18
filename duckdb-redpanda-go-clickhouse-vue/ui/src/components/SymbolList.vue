<script setup>
import { computed, ref } from 'vue'

const props = defineProps({ symbols: Array, selected: String })
const emit = defineEmits(['select'])

const query = ref('')
// Only the first matches are rendered; there are thousands of symbols
const LIMIT = 300
const matches = computed(() => {
  const q = query.value.trim().toUpperCase()
  const list = q ? props.symbols.filter((s) => s.symbol.includes(q)) : props.symbols
  return list.slice(0, LIMIT)
})
const total = computed(() => {
  const q = query.value.trim().toUpperCase()
  return q ? props.symbols.filter((s) => s.symbol.includes(q)).length : props.symbols.length
})
</script>

<template>
  <section class="panel symbols">
    <header>Symbols <span class="sub">{{ total.toLocaleString() }}</span></header>
    <input v-model="query" class="search" placeholder="Search, e.g. MC.FR" spellcheck="false" />
    <ol>
      <li v-for="s in matches" :key="s.symbol" :class="{ selected: s.symbol === selected }"
          @click="emit('select', s.symbol)">
        <span class="sym">{{ s.symbol }}</span>
        <span class="meta muted">{{ s.sectype === 'I' ? 'Index' : 'Equity' }} · {{ s.trades.toLocaleString() }} trades</span>
        <span class="num">{{ s.last_close.toLocaleString(undefined, { maximumFractionDigits: 4 }) }}</span>
        <span class="num muted" title="Signals">{{ s.signals }} sig</span>
      </li>
    </ol>
    <p v-if="total > LIMIT" class="more muted">Showing {{ LIMIT }} of {{ total.toLocaleString() }}; refine the search</p>
  </section>
</template>

<style scoped>
.search {
  margin: 8px;
  padding: 6px 8px;
  border: 1px solid var(--border);
  border-radius: 6px;
  background: var(--bg);
  color: var(--text);
  font: inherit;
}
ol { list-style: none; margin: 0; padding: 0 4px 4px; overflow-y: auto; flex: 1; }
li {
  display: grid;
  grid-template-columns: 1fr auto;
  gap: 0 8px;
  padding: 6px 8px;
  border-radius: 6px;
  cursor: pointer;
}
li:hover { background: var(--hover); }
li.selected { background: var(--selected); }
.sym { font-weight: 600; }
.meta, .num { font-size: 12px; }
.num { text-align: right; }
.more { margin: 0; padding: 6px 12px; font-size: 12px; border-top: 1px solid var(--border); }
</style>
