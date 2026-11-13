<script setup>
import { onMounted, ref } from 'vue'
import { getJSON, postJSON, putJSON } from '../api'

const cfg = ref(null)
const dirInput = ref('')
const batches = ref([])
const rec = ref(null)
const scanResult = ref(null)
const lastOrch = ref(null)
const events = ref([])
const eventsFor = ref(null)
const busy = ref(false)
const err = ref('')
const msg = ref('')

const STATUS_LABEL = {
  pending: '待确认',
  triggered: '已触发',
  ignored: '已忽略',
  invalid: '无效',
  failed: '触发失败',
}
const STATUS_CLASS = { pending: 'warn', triggered: 'ok', ignored: '', invalid: 'danger', failed: 'danger' }

async function refresh() {
  cfg.value = await getJSON('/api/dropzone/config')
  dirInput.value = cfg.value.dir_setting
  batches.value = await getJSON('/api/dropzone/batches')
  rec.value = await getJSON('/api/dropzone/reconcile')
}
async function saveDir() {
  err.value = ''; msg.value = ''
  try {
    cfg.value = await putJSON('/api/dropzone/config', { dir: dirInput.value })
    msg.value = '目录已保存'
    await refresh()
  } catch (e) { err.value = String(e.message || e) }
}
async function scan() {
  err.value = ''; busy.value = true
  try {
    scanResult.value = await postJSON('/api/dropzone/scan')
    await refresh()
  } catch (e) { err.value = String(e.message || e) }
  finally { busy.value = false }
}
async function act(fn, id) {
  err.value = ''; msg.value = ''; busy.value = true
  try {
    const r = await postJSON(`/api/dropzone/batches/${id}/${fn}`)
    if (r.orchestration) lastOrch.value = r.orchestration
    await refresh()
  } catch (e) { err.value = String(e.message || e) }
  finally { busy.value = false }
}
async function showEvents(b) {
  err.value = ''
  try {
    events.value = await getJSON(`/api/dropzone/batches/${b.id}/events`)
    eventsFor.value = b
  } catch (e) { err.value = String(e.message || e) }
}
onMounted(async () => {
  try { await refresh() } catch (e) { err.value = String(e.message || e) }
})
</script>
<template>
  <div class="page">
    <h1>落盘触发</h1>
    <p class="muted">本机落盘目录扫描 → 登记触发批次 → 确认后开一趟编排。文件只是触发信号，不读内容、不做业务键归并。</p>
    <p v-if="err" class="tag danger">{{ err }}</p>
    <p v-if="msg" class="tag ok">{{ msg }}</p>

    <div class="panel" v-if="cfg">
      <h3>落盘目录</h3>
      <p>
        <input v-model="dirInput" style="width:22rem" placeholder="相对 DATA_DIR 或绝对路径" />
        <button class="secondary" style="margin-left:0.5rem" @click="saveDir">保存目录</button>
        <button style="margin-left:0.5rem" :disabled="busy" @click="scan">{{ busy ? '扫描中…' : '立即扫描' }}</button>
      </p>
      <p class="mono muted">
        解析到 {{ cfg.resolved_dir }} · {{ cfg.exists ? `磁盘上 ${cfg.files_on_disk} 个文件` : '目录尚不存在（扫描时自动创建）' }}
        · 允许扩展名 {{ cfg.extensions.join(' ') }}
      </p>
      <p v-if="scanResult" class="mono">
        本次扫描：新登记待确认 {{ scanResult.new_pending }} · 新登记无效 {{ scanResult.new_invalid }} ·
        已登记跳过 {{ scanResult.already_known }} · 磁盘共 {{ scanResult.total_files }} 个文件
        <template v-if="scanResult.missing_from_disk"> · 已登记但磁盘缺失 {{ scanResult.missing_from_disk }}</template>
      </p>
    </div>

    <div class="panel" v-if="rec">
      <h3>触发对账 <span class="tag" :class="rec.ok ? 'ok' : 'danger'">{{ rec.ok ? '对得上' : '对不上' }}</span></h3>
      <p>
        发现 <b>{{ rec.discovered }}</b> · 待确认 {{ rec.pending }} · 有效触发 {{ rec.triggered }} ·
        已忽略 {{ rec.ignored }} · 无效 {{ rec.invalid }} · 触发失败 {{ rec.failed }}
      </p>
      <p class="mono muted">
        触发次数合计 {{ rec.trigger_slots }} = dropzone 来源编排 {{ rec.dropzone_orchestrations }}；
        <span v-for="(v, k) in rec.checks" :key="k" class="tag" :class="v ? 'ok' : 'danger'" style="margin-right:0.35rem">{{ k }}</span>
      </p>
    </div>

    <div class="panel">
      <h3>触发批次（{{ batches.length }}）</h3>
      <table>
        <thead>
          <tr><th>ID</th><th>文件名</th><th>大小</th><th>发现时间</th><th>状态</th><th>编排</th><th>次数</th><th>操作</th></tr>
        </thead>
        <tbody>
          <tr v-for="b in batches" :key="b.id">
            <td>{{ b.id }}</td>
            <td class="mono">{{ b.filename }}<div v-if="b.status_note" class="muted">{{ b.status_note }}</div></td>
            <td>{{ b.size_bytes }} B</td>
            <td class="mono">{{ b.discovered_at }}</td>
            <td><span class="tag" :class="STATUS_CLASS[b.status]">{{ STATUS_LABEL[b.status] || b.status }}</span></td>
            <td>
              <template v-if="b.orchestration_id">
                <router-link :to="{ path: '/attempts', query: { orchestration_id: b.orchestration_id } }">#{{ b.orchestration_id }}</router-link>
                <span class="tag" :class="b.orchestration_status === 'ok' ? 'ok' : 'warn'">{{ b.orchestration_status }}</span>
              </template>
              <span v-else class="muted">—</span>
            </td>
            <td>{{ b.trigger_count }}</td>
            <td>
              <button v-if="b.status === 'pending'" :disabled="busy" @click="act('trigger', b.id)">确认触发</button>
              <button v-if="b.status === 'triggered'" class="secondary" :disabled="busy" @click="act('retrigger', b.id)">重触发</button>
              <button v-if="b.status === 'pending' || b.status === 'invalid'" class="secondary" :disabled="busy" @click="act('ignore', b.id)">忽略</button>
              <button class="secondary" @click="showEvents(b)">留痕</button>
            </td>
          </tr>
          <tr v-if="!batches.length"><td colspan="8" class="muted">暂无批次：先配目录并扫描。</td></tr>
        </tbody>
      </table>
    </div>

    <div class="panel" v-if="lastOrch">
      <h3>刚拉开的编排</h3>
      <p>
        #{{ lastOrch.id }} · <span class="tag ok">{{ lastOrch.status }}</span> · 来源 {{ lastOrch.source }}（批次 #{{ lastOrch.trigger_batch_id }}）·
        succeeded {{ lastOrch.report?.succeeded }} / failed {{ lastOrch.report?.failed }} / skipped {{ lastOrch.report?.skipped }}
        · 对账 <span class="tag" :class="lastOrch.report?.reconcile_ok ? 'ok' : 'danger'">{{ lastOrch.report?.reconcile_ok ? 'OK' : '不平' }}</span>
      </p>
      <p>
        <router-link :to="{ path: '/attempts', query: { orchestration_id: lastOrch.id } }">查看本次尝试 →</router-link>
        · <router-link to="/orchestrate">编排台 →</router-link>
      </p>
    </div>

    <div class="panel" v-if="eventsFor">
      <h3>批次 #{{ eventsFor.id }}（{{ eventsFor.filename }}）留痕</h3>
      <table>
        <thead><tr><th>时间</th><th>事件</th><th>详情</th></tr></thead>
        <tbody>
          <tr v-for="e in events" :key="e.id">
            <td class="mono">{{ e.created_at }}</td>
            <td><span class="tag">{{ e.event }}</span></td>
            <td class="mono">{{ JSON.stringify(e.detail) }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
